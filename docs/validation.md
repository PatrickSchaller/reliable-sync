# Validation record

Checked on October 28, 2026.

| Check | Result |
| --- | --- |
| Local API/client/database suite | 41 passed, 0 failed, 0 skipped |
| Fresh checkout and locked restore | Passed |
| Fresh checkout, Release build and tests | 41 passed |
| Separate command-line worker processes | Failure at cursor 2, resume to cursor 6, replay unchanged |
| Real retry scenario | Six prescribed retry waits across three pages; final cursor 6 |
| Invalid second page | Stopped with cursor 2 and no partial second-page changes |
| Hosted Linux and Windows CI | [Passed](https://github.com/PatrickSchaller/reliable-sync/actions/runs/40812619142) |

The captured walkthrough contains only invented catalog data. Retry unit tests inject the delay function for speed; the command-line walkthrough used actual waits against the local HTTP simulator.

The local runner's .NET HTTPS transport could not reach NuGet, so locked local restores used the temporary feed of the same public NuGet packages downloaded over certificate-validated HTTPS. Hosted CI restored normally from the public NuGet source. No private feed or credentials are required.

Rollback after a checkpoint-write exception and recovery across ordinary process exits were verified. Forced process termination during a transaction, physical power loss, load, and a real ERP's ordering/identity guarantees were not tested.
