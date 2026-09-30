# Trusted File Tool Worker

This executable processes one bounded file operation on stdin. The host passes
four literal arguments: absolute workspace, input byte limit, read byte limit
and write byte limit. Requests use `kolyan-tools::FileOperation`; successful
stdout contains one `FileOperationResult`. Failure returns a nonzero exit.

The worker does not issue permissions or sandbox itself. The governed executor
must resolve its trusted executable, verify the exact grant, clear environment
and launch it through `kolyan-sandbox`. Running it directly does not prove
isolation. It performs no shell execution or Agent orchestration.

See [requirement 0030](../../docs/requirements/0030-governed-agent-execution.md).
