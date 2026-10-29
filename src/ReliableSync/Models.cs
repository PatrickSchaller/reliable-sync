using System.Text.Json;

namespace ReliableSync;

public record ItemEvent(long Cursor, string EventId, string Sku, string Name, int Quantity);
public record EventPage(string StreamId, long RequestedAfter, long NextCursor, bool HasMore, List<ItemEvent> Events);
public record SyncedItem(string Sku, string Name, int Quantity, long LastCursor);
public record SyncStatus(string? StreamId, long Cursor, long AppliedEvents, List<SyncedItem> Items);
public enum Scenario { Healthy, Retry, Fail, Invalid, Replay }
public sealed class SyncException(string message) : Exception(message);

public static class SyncJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        RespectRequiredConstructorParameters = true
    };
}
