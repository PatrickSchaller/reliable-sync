using System.Globalization;
using System.Net;
using System.Text.Json;

namespace ReliableSync;

public sealed class SourceClient(HttpClient http, Action<object> log,
    Func<TimeSpan, CancellationToken, Task>? delay = null)
{
    public const int MaximumAttempts = 3;
    private const int MaximumResponseBytes = 64 * 1024;
    private readonly Func<TimeSpan, CancellationToken, Task> wait = delay ?? Task.Delay;

    public async Task<EventPage> Read(long after, Scenario scenario, CancellationToken cancellationToken = default)
    {
        if (after < 0 || !Enum.IsDefined(scenario)) throw new SyncException("Invalid source request.");
        for (var attempt = 1; attempt <= MaximumAttempts; attempt++)
        {
            TimeSpan retryDelay;
            string reason;
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                var path = $"events/{scenario.ToString().ToLowerInvariant()}?after={after.ToString(CultureInfo.InvariantCulture)}";
                using var response = await http.GetAsync(path, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    await using var stream = await response.Content.ReadAsStreamAsync(timeout.Token);
                    var buffer = new byte[MaximumResponseBytes + 1];
                    var length = await stream.ReadAtLeastAsync(buffer, buffer.Length, throwOnEndOfStream: false, timeout.Token);
                    if (length > MaximumResponseBytes) throw new SyncException("Source response exceeds 64 KiB. No page was applied.");
                    try
                    {
                        return JsonSerializer.Deserialize<EventPage>(buffer.AsSpan(0, length), SyncJson.Options)
                            ?? throw new SyncException("Source returned an empty JSON page.");
                    }
                    catch (JsonException) { throw new SyncException("Source returned invalid or incomplete JSON. No page was applied."); }
                }
                var status = (int)response.StatusCode;
                if (response.StatusCode is not (HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests) && status is not (>= 500 and <= 599))
                    throw new SyncException($"Source returned HTTP {status}; this response is not retried.");
                if (attempt == MaximumAttempts) throw new SyncException($"Source returned HTTP {status} after {MaximumAttempts} attempts. Run again later to resume.");
                var retryAfter = response.Headers.RetryAfter;
                retryDelay = retryAfter?.Delta ?? (retryAfter?.Date is { } date
                    ? date - DateTimeOffset.UtcNow : Backoff(attempt));
                if (retryDelay < TimeSpan.Zero) retryDelay = TimeSpan.Zero;
                if (retryDelay > TimeSpan.FromSeconds(30))
                    throw new SyncException("Source Retry-After exceeds the 30-second wait budget. Run again later; no early retry was sent.");
                reason = $"http_{status}";
            }
            catch (Exception error) when (error is HttpRequestException or IOException ||
                error is OperationCanceledException && !cancellationToken.IsCancellationRequested)
            {
                if (attempt == MaximumAttempts)
                    throw new SyncException($"Source could not be reached after {MaximumAttempts} attempts. Run again later to resume.");
                reason = error is OperationCanceledException ? "timeout" : "network";
                retryDelay = Backoff(attempt);
            }
            log(new { @event = "retry", after, attempt, reason, delayMilliseconds = retryDelay.TotalMilliseconds });
            await wait(retryDelay, cancellationToken);
        }
        throw new InvalidOperationException("Retry loop did not return a result.");
    }

    private static TimeSpan Backoff(int attempt) => TimeSpan.FromMilliseconds(250 * (1 << (attempt - 1)));
}
