# Trusted File Tool Worker

This executable processes one bounded file operation on stdin. The host passes
four literal arguments: absolute workspace, input byte limit, read byte limit
and write byte limit. Requests use `kolyan-tools::ExactFileWorkerRequest` only;
successful stdout contains one `FileOperationResult`. Failure returns a nonzero
exit. Raw file operations are not accepted as transport requests.

The configured workspace must exactly equal the request's physical workspace.
The exact executor validates pinned directory and target identities before
effects; the model path is only a result label, not a path to resolve again.
Replacement requests require a host-owned absent staging leaf outside the
workspace on the same filesystem. Reads prohibit staging. Identity or rename
failures never fall back to ambient paths, truncation or copying. Rename is the
commit point; cancellation does not imply rollback or concurrent-writer CAS.

The worker does not issue permissions or sandbox itself. The governed executor
must resolve its trusted executable, verify the exact grant, clear environment
and launch it through `kolyan-sandbox`. Running it directly does not prove
isolation. It performs no shell execution or Agent orchestration.

See [requirement 0030](../../docs/requirements/0030-governed-agent-execution.md).
