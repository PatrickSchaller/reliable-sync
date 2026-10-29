using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using Microsoft.Data.Sqlite;
using ReliableSync;
using Xunit;

namespace ReliableSync.Tests;

public sealed class SyncTests : IDisposable
{
    private readonly string directory = Path.Combine(Path.GetTempPath(), "reliable-sync-tests", Guid.NewGuid().ToString("N"));
    private readonly string database;
    private readonly SyncStore store;

    public SyncTests()
    {
        database = Path.Combine(directory, "catalog.db");
        store = new(database);
        store.Initialize();
    }

    private static EventPage Page(long after, ItemEvent[] events, bool more = false, long? next = null) =>
        new(DemoSource.StreamId, after, next ?? (events.Length == 0 ? after : events[^1].Cursor), more, [.. events]);
    private static ItemEvent Event(long cursor, string sku = "HW-104", int quantity = 10) =>
        new(cursor, $"evt-{cursor}", sku, "Invented item", quantity);
    private static HttpResponseMessage Ok(EventPage page) => new(HttpStatusCode.OK)
        { Content = JsonContent.Create(page, options: SyncJson.Options) };
    private static HttpClient Http(StubHandler handler) => new(handler)
        { BaseAddress = new Uri("http://127.0.0.1/"), Timeout = Timeout.InfiniteTimeSpan };
    private static Task NoDelay(TimeSpan delay, CancellationToken token) { token.ThrowIfCancellationRequested(); return Task.CompletedTask; }

    [Fact]
    public void Page_and_checkpoint_survive_reopening()
    {
        Assert.Equal(2, store.Apply(0, Page(0, [Event(1), Event(2, "HW-208", 24)])));
        var status = new SyncStore(database).Status();
        Assert.Equal(2, status.Cursor);
        Assert.Equal(2, status.AppliedEvents);
        Assert.Equal(2, status.Items.Count);
        Assert.Equal(DemoSource.StreamId, status.StreamId);
    }

    [Fact]
    public void Unrelated_database_is_refused_without_modification()
    {
        var unrelated = Path.Combine(directory, "unrelated.db");
        using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = unrelated, Pooling = false }.ToString()))
        {
            connection.Open();
            using var create = connection.CreateCommand();
            create.CommandText = "CREATE TABLE ExistingData(Value TEXT); INSERT INTO ExistingData VALUES('leave this alone');";
            create.ExecuteNonQuery();
        }
        var before = File.ReadAllBytes(unrelated);
        Assert.Throws<SyncException>(() => new SyncStore(unrelated).Initialize());
        Assert.Equal(before, File.ReadAllBytes(unrelated));
    }

    [Fact]
    public void Unsupported_database_version_is_refused()
    {
        using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = database, Pooling = false }.ToString()))
        {
            connection.Open();
            using var change = connection.CreateCommand();
            change.CommandText = "PRAGMA user_version=99";
            change.ExecuteNonQuery();
        }
        Assert.Throws<SyncException>(() => store.Initialize());
        Assert.Throws<SyncException>(() => store.Status());
    }

    [Fact]
    public void Replayed_old_events_cannot_overwrite_newer_stock()
    {
        var first = Event(1, quantity: 10);
        store.Apply(0, Page(0, [first]));
        store.Apply(1, Page(1, [first, Event(2, quantity: 15)]));
        Assert.Equal(0, store.Apply(2, Page(2, [first], next: 2)));
        var status = store.Status();
        Assert.Equal(2, status.Cursor);
        Assert.Equal(2, status.AppliedEvents);
        Assert.Equal(15, Assert.Single(status.Items).Quantity);
    }

    [Fact]
    public void Reused_identity_with_changed_payload_rolls_back_the_page()
    {
        store.Apply(0, Page(0, [Event(1)]));
        Assert.Throws<SyncException>(() => store.Apply(1, Page(1, [Event(1, quantity: 999), Event(2)])));
        Assert.Equal(1, store.Status().Cursor);
        Assert.Equal(10, Assert.Single(store.Status().Items).Quantity);
    }

    [Fact]
    public void Reusing_an_event_id_at_a_new_cursor_is_rejected()
    {
        store.Apply(0, Page(0, [Event(1)]));
        Assert.Throws<SyncException>(() => store.Apply(1, Page(1, [Event(2) with { EventId = "evt-1" }])));
        Assert.Equal(1, store.Status().AppliedEvents);
    }

    [Fact]
    public void Destination_cannot_silently_switch_streams()
    {
        store.Apply(0, Page(0, [Event(1)]));
        Assert.Throws<SyncException>(() => store.Apply(1, Page(1, [Event(2)]) with { StreamId = "different-stream" }));
        Assert.Equal(DemoSource.StreamId, store.Status().StreamId);
        Assert.Equal(1, store.Status().Cursor);
    }

    [Theory]
    [InlineData("gap")]
    [InlineData("out-of-order")]
    [InlineData("invalid-later-event")]
    [InlineData("skip-next-cursor")]
    [InlineData("wrong-request-cursor")]
    [InlineData("no-progress")]
    [InlineData("invalid-id")]
    [InlineData("null-events")]
    [InlineData("too-many-events")]
    public void Invalid_pages_leave_no_partial_item_event_or_checkpoint(string kind)
    {
        var page = kind switch
        {
            "gap" => Page(0, [Event(2)]),
            "out-of-order" => Page(0, [Event(2), Event(1)], next: 2),
            "invalid-later-event" => Page(0, [Event(1), Event(2, quantity: -1)]),
            "skip-next-cursor" => Page(0, [Event(1)], next: 2),
            "wrong-request-cursor" => Page(1, [Event(2)]),
            "no-progress" => Page(0, [], more: true),
            "invalid-id" => Page(0, [Event(1) with { EventId = "evt-1\n" }]),
            "null-events" => Page(0, []) with { Events = null! },
            _ => Page(0, Enumerable.Range(1, 21).Select(i => Event(i)).ToArray())
        };
        Assert.Throws<SyncException>(() => store.Apply(0, page));
        var status = new SyncStore(database).Status();
        Assert.Equal(0, status.Cursor);
        Assert.Equal(0, status.AppliedEvents);
        Assert.Empty(status.Items);
        Assert.Null(status.StreamId);
    }

    [Fact]
    public void Checkpoint_write_failure_rolls_back_already_written_items_and_events()
    {
        using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = database, Pooling = false }.ToString()))
        {
            connection.Open();
            using var trigger = connection.CreateCommand();
            trigger.CommandText = "CREATE TRIGGER FailCheckpoint BEFORE UPDATE ON SyncState BEGIN SELECT RAISE(ABORT,'simulated checkpoint failure'); END;";
            trigger.ExecuteNonQuery();
        }
        Assert.Throws<SqliteException>(() => store.Apply(0, Page(0, [Event(1), Event(2)])));
        var reopened = new SyncStore(database).Status();
        Assert.Equal(0, reopened.Cursor);
        Assert.Equal(0, reopened.AppliedEvents);
        Assert.Empty(reopened.Items);
    }

    [Fact]
    public async Task Competing_writers_cannot_commit_against_the_same_old_checkpoint()
    {
        var results = await Task.WhenAll(Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            try { store.Apply(0, Page(0, [Event(1), Event(2)])); return true; }
            catch (SyncException) { return false; }
        })));
        Assert.Single(results, result => result);
        Assert.Equal(2, store.Status().Cursor);
        Assert.Equal(2, store.Status().AppliedEvents);
    }

    [Fact]
    public void Cancelled_page_does_not_write()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.Throws<OperationCanceledException>(() => store.Apply(0, Page(0, [Event(1)]), cancellation.Token));
        Assert.Equal(0, store.Status().AppliedEvents);
    }

    [Fact]
    public async Task Rate_limit_and_server_error_retry_with_the_prescribed_delays()
    {
        var delays = new List<TimeSpan>();
        var limited = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        limited.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(2));
        using var handler = new StubHandler((_, call) => call switch { 1 => limited, 2 => new(HttpStatusCode.ServiceUnavailable), _ => Ok(Page(0, [Event(1)])) });
        using var http = Http(handler);
        var client = new SourceClient(http, _ => { }, (delay, _) => { delays.Add(delay); return Task.CompletedTask; });
        var page = await client.Read(0, Scenario.Healthy);
        Assert.Single(page.Events);
        Assert.Equal(3, handler.Calls);
        Assert.Equal([TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(500)], delays);
    }

    [Theory]
    [InlineData("network")]
    [InlineData("body-io")]
    [InlineData("timeout")]
    public async Task Transient_transport_failures_can_recover(string kind)
    {
        using var handler = new StubHandler((_, call) => call == 1
            ? throw kind switch { "network" => new HttpRequestException("simulated"), "body-io" => new IOException("simulated"), _ => new TaskCanceledException("simulated") }
            : Ok(Page(0, [Event(1)])));
        using var http = Http(handler);
        await new SourceClient(http, _ => { }, NoDelay).Read(0, Scenario.Healthy);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task Exhausted_retries_do_not_advance_the_checkpoint()
    {
        using var handler = new StubHandler((_, _) => new(HttpStatusCode.ServiceUnavailable));
        using var http = Http(handler);
        var runner = new SyncRunner(store, new SourceClient(http, _ => { }, NoDelay), _ => { });
        await Assert.ThrowsAsync<SyncException>(() => runner.Run(Scenario.Healthy));
        Assert.Equal(3, handler.Calls);
        Assert.Equal(0, store.Status().Cursor);
        Assert.Empty(store.Status().Items);
    }

    [Theory]
    [InlineData(400)]
    [InlineData(401)]
    [InlineData(404)]
    [InlineData(302)]
    [InlineData(201)]
    [InlineData(204)]
    [InlineData(600)]
    public async Task Nontransient_statuses_are_not_retried(int status)
    {
        using var handler = new StubHandler((_, _) => new((HttpStatusCode)status));
        using var http = Http(handler);
        await Assert.ThrowsAsync<SyncException>(() => new SourceClient(http, _ => { }, NoDelay).Read(0, Scenario.Healthy));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Long_retry_after_stops_instead_of_retrying_earlier_than_requested()
    {
        var waits = 0;
        using var handler = new StubHandler((_, _) =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            response.Headers.RetryAfter = new RetryConditionHeaderValue(DateTimeOffset.UtcNow.AddMinutes(2));
            return response;
        });
        using var http = Http(handler);
        await Assert.ThrowsAsync<SyncException>(() => new SourceClient(http, _ => { }, (_, _) => { waits++; return Task.CompletedTask; }).Read(0, Scenario.Healthy));
        Assert.Equal(1, handler.Calls);
        Assert.Equal(0, waits);
    }

    [Fact]
    public async Task Past_retry_date_allows_an_immediate_retry()
    {
        var delays = new List<TimeSpan>();
        using var handler = new StubHandler((_, call) =>
        {
            if (call > 1) return Ok(Page(0, []));
            var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            response.Headers.RetryAfter = new RetryConditionHeaderValue(DateTimeOffset.UtcNow.AddMinutes(-1));
            return response;
        });
        using var http = Http(handler);
        await new SourceClient(http, _ => { }, (delay, _) => { delays.Add(delay); return Task.CompletedTask; }).Read(0, Scenario.Healthy);
        Assert.Equal(TimeSpan.Zero, Assert.Single(delays));
    }

    [Theory]
    [InlineData("malformed")]
    [InlineData("missing-field")]
    [InlineData("oversized")]
    public async Task Invalid_responses_are_not_retried_or_committed(string kind)
    {
        var body = kind switch
        {
            "malformed" => "{broken",
            "missing-field" => "{\"streamId\":\"demo-catalog-v1\",\"requestedAfter\":0,\"nextCursor\":0,\"hasMore\":false}",
            _ => new string('x', 65537)
        };
        using var handler = new StubHandler((_, _) => new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        using var http = Http(handler);
        await Assert.ThrowsAsync<SyncException>(() => new SyncRunner(store, new SourceClient(http, _ => { }, NoDelay), _ => { }).Run(Scenario.Healthy));
        Assert.Equal(1, handler.Calls);
        Assert.Equal(0, store.Status().Cursor);
    }

    [Fact]
    public async Task Cancelling_during_retry_wait_does_not_send_another_request()
    {
        using var cancellation = new CancellationTokenSource();
        using var handler = new StubHandler((_, _) => new(HttpStatusCode.TooManyRequests));
        using var http = Http(handler);
        var client = new SourceClient(http, _ => { }, (_, token) => { cancellation.Cancel(); token.ThrowIfCancellationRequested(); return Task.CompletedTask; });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.Read(0, Scenario.Healthy, cancellation.Token));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Endless_valid_source_is_bounded_and_leaves_a_resumable_checkpoint()
    {
        using var handler = new StubHandler((_, call) => Ok(Page(call - 1, [Event(call)], more: true)));
        using var http = Http(handler);
        await Assert.ThrowsAsync<SyncException>(() => new SyncRunner(store, new SourceClient(http, _ => { }, NoDelay), _ => { }).Run(Scenario.Healthy));
        Assert.Equal(100, handler.Calls);
        Assert.Equal(100, store.Status().Cursor);
        Assert.Equal(100, store.Status().AppliedEvents);
    }

    [Fact]
    public async Task Real_http_source_can_fail_then_resume_and_replay()
    {
        await using var app = DemoSource.Build("http://127.0.0.1:0");
        await app.StartAsync();
        try
        {
            using var http = new HttpClient { BaseAddress = new Uri(Assert.Single(app.Urls)), Timeout = Timeout.InfiniteTimeSpan };
            var runner = new SyncRunner(store, new SourceClient(http, _ => { }, NoDelay), _ => { });
            await Assert.ThrowsAsync<SyncException>(() => runner.Run(Scenario.Fail));
            Assert.Equal(2, store.Status().Cursor);
            Assert.Equal(2, store.Status().AppliedEvents);
            Assert.Equal(10, store.Status().Items.Single(item => item.Sku == "HW-104").Quantity);

            var reopened = new SyncStore(database);
            var recovery = new SyncRunner(reopened, new SourceClient(http, _ => { }, NoDelay), _ => { });
            await recovery.Run(Scenario.Healthy);
            await recovery.Run(Scenario.Replay);
            var final = reopened.Status();
            Assert.Equal(6, final.Cursor);
            Assert.Equal(6, final.AppliedEvents);
            Assert.Equal(4, final.Items.Count);
            Assert.Equal(15, final.Items.Single(item => item.Sku == "HW-104").Quantity);
            Assert.Equal(22, final.Items.Single(item => item.Sku == "HW-208").Quantity);
        }
        finally { await app.StopAsync(); }
    }

    [Theory]
    [InlineData(Scenario.Retry)]
    [InlineData(Scenario.Replay)]
    public async Task Real_http_sample_retry_and_overlap_scenarios_finish(Scenario scenario)
    {
        await using var app = DemoSource.Build("http://127.0.0.1:0");
        await app.StartAsync();
        try
        {
            var delays = new List<TimeSpan>();
            using var http = new HttpClient { BaseAddress = new Uri(Assert.Single(app.Urls)), Timeout = Timeout.InfiniteTimeSpan };
            var source = new SourceClient(http, _ => { }, (delay, _) => { delays.Add(delay); return Task.CompletedTask; });
            await new SyncRunner(store, source, _ => { }).Run(scenario);
            Assert.Equal(6, store.Status().Cursor);
            Assert.Equal(6, store.Status().AppliedEvents);
            if (scenario == Scenario.Retry)
            {
                Assert.Equal(3, delays.Count(delay => delay == TimeSpan.FromSeconds(1)));
                Assert.Equal(3, delays.Count(delay => delay == TimeSpan.FromMilliseconds(500)));
            }
        }
        finally { await app.StopAsync(); }
    }

    public void Dispose()
    {
        foreach (var file in Directory.EnumerateFiles(directory)) File.Delete(file);
        Directory.Delete(directory);
    }

    private sealed class StubHandler(Func<HttpRequestMessage, int, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public int Calls { get; private set; }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(respond(request, ++Calls));
        }
    }
}
