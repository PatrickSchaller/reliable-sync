using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;

namespace ReliableSync;

public sealed class SyncStore(string databasePath)
{
    private const int ApplicationId = 1381194062;
    private readonly string connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = Path.GetFullPath(databasePath), ForeignKeys = true,
        Pooling = false, DefaultTimeout = 3
    }.ToString();
    private static readonly Regex Identifier = new("\\A[A-Za-z0-9][A-Za-z0-9._-]{0,79}\\z", RegexOptions.CultureInvariant);

    private SqliteConnection Open(bool readOnly = false)
    {
        var settings = new SqliteConnectionStringBuilder(connectionString);
        if (readOnly) settings.Mode = SqliteOpenMode.ReadOnly;
        var connection = new SqliteConnection(settings.ToString());
        connection.Open();
        return connection;
    }

    private static SqliteCommand Command(SqliteConnection connection, SqliteTransaction? transaction,
        string sql, params (string Name, object Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        return command;
    }

    private static void CheckDatabase(SqliteConnection connection, bool allowEmpty = false)
    {
        using var identity = Command(connection, null, "PRAGMA application_id");
        using var version = Command(connection, null, "PRAGMA user_version");
        var id = (long)identity.ExecuteScalar()!;
        var schemaVersion = (long)version.ExecuteScalar()!;
        if (id == ApplicationId && schemaVersion == 1) return;
        if (allowEmpty && id == 0 && schemaVersion == 0)
        {
            using var tables = Command(connection, null,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
            if ((long)tables.ExecuteScalar()! == 0) return;
        }
        throw new SyncException("Destination is not a recognized Reliable Sync database. Choose a new file or the correct existing database.");
    }

    public void Initialize()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(databasePath))!);
        using var connection = Open();
        CheckDatabase(connection, allowEmpty: true);
        using (var journal = Command(connection, null, "PRAGMA journal_mode=WAL")) journal.ExecuteScalar();
        using var transaction = connection.BeginTransaction();
        using var command = Command(connection, transaction, """
            PRAGMA application_id=1381194062;
            PRAGMA user_version=1;
            CREATE TABLE IF NOT EXISTS SyncState (
                Id INTEGER PRIMARY KEY CHECK(Id=1), StreamId TEXT,
                Cursor INTEGER NOT NULL CHECK(Cursor>=0));
            INSERT OR IGNORE INTO SyncState(Id,StreamId,Cursor) VALUES(1,NULL,0);
            CREATE TABLE IF NOT EXISTS Items (
                Sku TEXT PRIMARY KEY, Name TEXT NOT NULL,
                Quantity INTEGER NOT NULL CHECK(Quantity BETWEEN 0 AND 1000000),
                LastCursor INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS AppliedEvents (
                EventId TEXT PRIMARY KEY, Cursor INTEGER NOT NULL UNIQUE, Payload TEXT NOT NULL);
            """);
        command.ExecuteNonQuery();
        transaction.Commit();
    }

    public SyncStatus Status()
    {
        using var connection = Open(readOnly: true);
        CheckDatabase(connection);
        using var transaction = connection.BeginTransaction(deferred: true);
        string? streamId;
        long cursor;
        using (var state = Command(connection, transaction, "SELECT StreamId,Cursor FROM SyncState WHERE Id=1"))
        using (var reader = state.ExecuteReader())
        {
            if (!reader.Read()) throw new SyncException("Destination checkpoint is missing.");
            streamId = reader.IsDBNull(0) ? null : reader.GetString(0);
            cursor = reader.GetInt64(1);
        }
        var items = new List<SyncedItem>();
        using (var query = Command(connection, transaction, "SELECT Sku,Name,Quantity,LastCursor FROM Items ORDER BY Sku"))
        using (var reader = query.ExecuteReader())
            while (reader.Read()) items.Add(new(reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetInt64(3)));
        using var count = Command(connection, transaction, "SELECT COUNT(*) FROM AppliedEvents");
        var appliedEvents = (long)count.ExecuteScalar()!;
        transaction.Commit();
        return new(streamId, cursor, appliedEvents, items);
    }

    public int Apply(long expectedAfter, EventPage page, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!ValidId(page.StreamId) || page.RequestedAfter != expectedAfter || page.NextCursor < expectedAfter ||
            page.Events is null || page.Events.Count > 20)
            throw new SyncException("Source page has an invalid stream, cursor, or event count.");

        using var connection = Open();
        CheckDatabase(connection);
        // Serialize the checkpoint check and all writes, including competing processes.
        using var transaction = connection.BeginTransaction(deferred: false);
        using (var checkpoint = Command(connection, transaction, "SELECT StreamId,Cursor FROM SyncState WHERE Id=1"))
        using (var reader = checkpoint.ExecuteReader())
        {
            if (!reader.Read() || reader.GetInt64(1) != expectedAfter)
                throw new SyncException("Another run advanced the checkpoint. Inspect status, then run again.");
            if (!reader.IsDBNull(0) && reader.GetString(0) != page.StreamId)
                throw new SyncException("Source stream identity changed. Use the correct source or a separate database.");
        }

        long lastSeen = 0;
        var nextCursor = expectedAfter;
        var applied = 0;
        foreach (var item in page.Events)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (item is null || item.Cursor <= lastSeen || item.Cursor > page.NextCursor ||
                !ValidId(item.EventId) || !ValidId(item.Sku) || item.Sku.Length > 64 ||
                string.IsNullOrWhiteSpace(item.Name) || item.Name.Length > 200 ||
                item.Name != item.Name.Trim() || item.Name.Any(char.IsControl) || item.Quantity is < 0 or > 1_000_000)
                throw new SyncException("Source page contains an invalid or out-of-order event. No part of this page was committed.");
            lastSeen = item.Cursor;
            var payload = JsonSerializer.Serialize(item, SyncJson.Options);
            using (var existing = Command(connection, transaction,
                "SELECT Cursor,Payload FROM AppliedEvents WHERE EventId=$id", ("$id", item.EventId)))
            using (var reader = existing.ExecuteReader())
            {
                if (reader.Read())
                {
                    if (reader.GetInt64(0) != item.Cursor || reader.GetString(1) != payload || item.Cursor > expectedAfter)
                        throw new SyncException("An event identity was reused with different content or inconsistent destination state.");
                    continue;
                }
            }
            if (nextCursor == long.MaxValue || item.Cursor != nextCursor + 1)
                throw new SyncException("Source cursor contains a gap or an untracked old event. The checkpoint was not advanced.");

            using (var eventInsert = Command(connection, transaction,
                "INSERT INTO AppliedEvents(EventId,Cursor,Payload) VALUES($id,$cursor,$payload)",
                ("$id", item.EventId), ("$cursor", item.Cursor), ("$payload", payload)))
                eventInsert.ExecuteNonQuery();
            using (var upsert = Command(connection, transaction, """
                INSERT INTO Items(Sku,Name,Quantity,LastCursor) VALUES($sku,$name,$qty,$cursor)
                ON CONFLICT(Sku) DO UPDATE SET Name=excluded.Name,Quantity=excluded.Quantity,LastCursor=excluded.LastCursor
                """, ("$sku", item.Sku), ("$name", item.Name), ("$qty", item.Quantity), ("$cursor", item.Cursor)))
                upsert.ExecuteNonQuery();
            nextCursor = item.Cursor;
            applied++;
        }
        if (page.NextCursor != nextCursor || (page.HasMore && nextCursor == expectedAfter))
            throw new SyncException("Source page skips a cursor or promises more data without making progress.");
        cancellationToken.ThrowIfCancellationRequested();
        using (var update = Command(connection, transaction,
            "UPDATE SyncState SET StreamId=$stream,Cursor=$cursor WHERE Id=1",
            ("$stream", page.StreamId), ("$cursor", nextCursor)))
            update.ExecuteNonQuery();
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
        return applied;
    }

    private static bool ValidId(string? value) => value is not null && Identifier.IsMatch(value);
}
