using System.Text.Json;
using Microsoft.Data.Sqlite;
using ReliableSync;

const string usage = "Usage: source | run [database-path] [healthy|retry|fail|invalid|replay] | status [database-path]";
if (args is ["--help"])
{
    Console.WriteLine(usage);
    return 0;
}
if (args is ["source"])
{
    Console.WriteLine($"Invented catalog source listening at {DemoSource.BaseUrl}");
    await using var app = DemoSource.Build();
    await app.RunAsync();
    return 0;
}
if (args.Length is < 1 or > 3 || args[0] is not ("run" or "status") || args[0] == "status" && args.Length > 2)
{
    Console.Error.WriteLine(usage);
    return 2;
}
var scenario = Scenario.Healthy;
if (args.Length == 3 && (!Enum.TryParse(args[2], true, out scenario) || !Enum.IsDefined(scenario)))
{
    Console.Error.WriteLine(usage);
    return 2;
}
using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; cancellation.Cancel(); };
try
{
    var database = args.Length >= 2 ? args[1] : Path.Combine("data", "catalog.db");
    if (args[0] == "status" && !File.Exists(database))
    {
        Console.Error.WriteLine("Database does not exist. Run the sync first or supply the correct path.");
        return 2;
    }
    var store = new SyncStore(database);
    if (args[0] == "status")
    {
        Console.WriteLine(JsonSerializer.Serialize(store.Status(), new JsonSerializerOptions(SyncJson.Options) { WriteIndented = true }));
        return 0;
    }
    store.Initialize();
    using var handler = new SocketsHttpHandler { AllowAutoRedirect = false, UseCookies = false, ConnectTimeout = TimeSpan.FromSeconds(3) };
    using var http = new HttpClient(handler) { BaseAddress = new Uri(DemoSource.BaseUrl), Timeout = Timeout.InfiniteTimeSpan };
    Action<object> log = entry => Console.WriteLine(JsonSerializer.Serialize(entry, SyncJson.Options));
    await new SyncRunner(store, new SourceClient(http, log), log).Run(scenario, cancellation.Token);
    return 0;
}
catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
{
    Console.Error.WriteLine("Sync cancelled. Run again to resume from the last committed page.");
    return 130;
}
catch (Exception error) when (error is SyncException or SqliteException or IOException or UnauthorizedAccessException)
{
    Console.Error.WriteLine(JsonSerializer.Serialize(new { @event = "stopped", message = error.Message,
        recovery = "Use status to inspect committed data, then run again when the source is ready." }));
    return 1;
}
