using System.Collections.Concurrent;
using System.Net;

namespace ReliableSync;

public static class DemoSource
{
    public const string StreamId = "demo-catalog-v1";
    public const string BaseUrl = "http://127.0.0.1:5097/";
    private static readonly ItemEvent[] Events = [
        new(1, "evt-001", "HW-104", "Brass cabinet handle", 10),
        new(2, "evt-002", "HW-208", "Steel corner bracket", 24),
        new(3, "evt-003", "HW-104", "Brass cabinet handle", 15),
        new(4, "evt-004", "WD-310", "Birch plywood panel", 8),
        new(5, "evt-005", "HW-208", "Steel corner bracket", 22),
        new(6, "evt-006", "WD-415", "Oak dowel, 12 mm", 36)
    ];

    public static WebApplication Build(string url = BaseUrl)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls(url);
        builder.Configuration["AllowedHosts"] = "localhost;127.0.0.1;[::1]";
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        var app = builder.Build();
        var attempts = new ConcurrentDictionary<(Scenario, long), int>();
        app.Use(async (context, next) =>
        {
            if (context.Connection.RemoteIpAddress is { } address && !IPAddress.IsLoopback(address))
            {
                context.Response.StatusCode = 403;
                return;
            }
            await next(context);
        });
        app.MapGet("/events/{scenario}", (string scenario, long after, HttpContext context) =>
        {
            if (!Enum.TryParse<Scenario>(scenario, true, out var selected) || !Enum.IsDefined(selected) || after is < 0 or > 6)
                return Results.Problem(statusCode: 400, detail: "Use a documented scenario and a cursor from 0 to 6.");
            var attempt = attempts.AddOrUpdate((selected, after), 1, (_, count) => count + 1);
            if (selected == Scenario.Retry && attempt == 1)
            {
                context.Response.Headers.RetryAfter = "1";
                return Results.StatusCode(429);
            }
            if ((selected == Scenario.Retry && attempt == 2) || (selected == Scenario.Fail && after >= 2))
                return Results.StatusCode(503);
            var events = Events.Where(item => item.Cursor > after).Take(2).ToList();
            if (selected == Scenario.Replay && after > 0)
                events.Insert(0, Events.Single(item => item.Cursor == after));
            if (selected == Scenario.Invalid && after == 2)
                events[1] = events[1] with { Quantity = -1 };
            var nextCursor = events.Count == 0 ? after : events[^1].Cursor;
            return Results.Json(new EventPage(StreamId, after, nextCursor, nextCursor < Events.Length, events));
        });
        return app;
    }
}
