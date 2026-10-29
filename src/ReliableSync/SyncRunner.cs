namespace ReliableSync;

public sealed class SyncRunner(SyncStore store, SourceClient source, Action<object> log)
{
    public async Task Run(Scenario scenario, CancellationToken cancellationToken = default)
    {
        // shortcut: cap one demonstration run at 100 pages; use scheduled batches for larger streams.
        for (var pageNumber = 0; pageNumber < 100; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var after = store.Status().Cursor;
            log(new { @event = "page_requested", after });
            var page = await source.Read(after, scenario, cancellationToken);
            var applied = store.Apply(after, page, cancellationToken);
            log(new { @event = "page_committed", cursor = page.NextCursor, received = page.Events.Count, applied });
            if (!page.HasMore)
            {
                log(new { @event = "caught_up", cursor = page.NextCursor });
                return;
            }
        }
        throw new SyncException("The 100-page run limit was reached. Run again to resume from the last committed cursor.");
    }
}
