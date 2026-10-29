# Follow a page through the code

1. [Program.cs](../src/ReliableSync/Program.cs) selects the console command, opens the dedicated database, and creates one HTTP client for the run. The source command starts a separate local simulator.
2. [SyncRunner.cs](../src/ReliableSync/SyncRunner.cs) reads committed state, requests the next page, applies it, and stops when the source says it is caught up. Every run has a 100-page bound.
3. [SourceClient.cs](../src/ReliableSync/SourceClient.cs) handles transport failures. HTTP reads have a deadline and size bound. JSON and constructor requirements are checked before the page reaches the store. Retry timing is separate from database state.
4. [SyncStore.cs](../src/ReliableSync/SyncStore.cs) checks the file's application identity, obtains the SQLite writer lock, verifies the expected checkpoint, and validates event order and identity as it applies the page. Known events are compared and skipped. New events, item snapshots, and the checkpoint share one commit.
5. [DemoSource.cs](../src/ReliableSync/DemoSource.cs) creates repeatable source-side failures with six invented events. It is a controlled test partner, not a model of every legacy API.

Questions to work through while reviewing:

- What would happen if the checkpoint were saved before the destination writes, or in a second transaction?
- Why can a replayed old snapshot overwrite newer stock unless identity is checked before the upsert?
- Why does the destination bind to a stream ID as well as storing a cursor?
- When should a retry stop instead of shortening a server's requested wait?
- What guarantees in this sample would fail for an API whose timestamp cursor can skip ties or whose event IDs change between requests?

Use [SyncTests.cs](../tests/ReliableSync.Tests/SyncTests.cs) to find executable evidence. Start with the checkpoint-trigger failure test, the replay test, and the real HTTP failure/recovery test.
