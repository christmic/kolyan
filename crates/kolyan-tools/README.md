# Workspace 工具边界

## Isolated Agent environment tools

`IsolatedToolSet` assembles exactly `file.read`, `file.write`, `file.edit` and
`shell` under one physical workspace. It is the inventory for Agent host assembly,
not an Agent Runner. Named or inline Agent permissions must filter both advertised
schemas and actual invocation routing. `agent.invoke` is orchestration and is
not part of this environment inventory.

Both adapters implement Core's mandatory asynchronous preparation and scoped
`ToolInvocation` port. Preparation derives claims and binds implementation and
physical execution plans; execution validates the exact grant against independently
admitted scope and current policy. There is no optional grant or ambient fallback.
The host supplies the trusted worker binary, external staging root, protected
control paths and byte/time limits. Assembly unions both adapters' protected paths
and denies the staging root to sibling Shell execution.

`IsolatedFileTools::adapter_revision()` reads the worker binary and hashes the
normalized file configuration using the same calculation as `prepare`.
`IsolatedToolSet::file_adapter_revision()` forwards to the actual file instance
after protected-root merging. Both return explicit errors and perform synchronous
read-only file I/O: call them during blocking host assembly. They do not execute
the worker, prepare a dummy call, inspect target files or grant authority.
Recalculation can change after worker/configuration changes; historical goal
verification uses its saved trusted revision set, never current-worker inspection.
Independent revision fixtures export complete JSONL rows, close the file and
physically reread it before comparing clone/preparation, mutation, merged roots,
read errors and unchanged target/staging observations.

File workers consume only `ExactFileWorkerRequest`. They verify saved directory
and leaf identities through nofollow handles instead of resolving model paths
again. Write/edit create a 0700 private staging directory outside the workspace,
on the same filesystem, and replace the target through pinned parent handles.
Only the selected staging leaf is admitted. Rename is the effect commit point;
this does not promise a transaction against an uncooperative external writer,
cross-filesystem replacement or power-loss durability.

Zero-exit worker receipts are strictly decoded before a successful ToolResult:
read content is checked against its UTF-8 byte length and SHA-256; write bytes
and SHA-256 must match the prepared content, with no returned replacement content.
Edit checks shape, limits and canonical digest, not a digest inferred from its
replacement segment. Malformed or inconsistent write/edit receipts are Uncertain
because rename may already have committed; they are not ordinary retryable tool
failures. No file is reopened or effect replayed for this validation. See
[goal verification prerequisites](../../docs/requirements/0035-goal-verification-and-agent-host.md).

Input and output bounds are independent. Shell stdin is empty; the trusted file
protocol has a bounded input envelope. Complete serialized `ToolResult` size is
checked separately from process output. Shell results preserve readable UTF-8 or
explicit lossless hex for binary bytes, along with exit status and error state.
Cancellation reaches the real process group and waits for cleanup; it does not
roll back an effect already committed.

Module tests remain under their source modules. Cross-crate real worker and
sibling-shell mutation matrices live in `kolyan-integration-tests/tests/tools/`,
with separate datasets and test-owned JSONL export before comparisons. They are
real local subprocess tests, not actual-model or multi-Agent acceptance.

## Trusted in-process primitives

`RestrictedFileTool` 和 `RestrictedShellTool` 在构造时打开受信任的根目录，
后续访问均通过 `cap_std::fs::Dir` 相对句柄完成，不再将模型路径拼成绝对路径
交给 `std::fs`。打开根目录失败会保留错误，后续文件操作失败关闭。

- 拒绝绝对路径和显式父目录组件；软链接目标也必须处于目录权限边界内。
- 内部相对软链接可用；外部链接的读取、写入、目录查询均拒绝。
- 根路径被替换后仍使用原目录句柄，不跟随新的根路径。
- 这不是进程级沙箱：不隔离宿主其他权限，不隔离预先创建的硬链接、挂载点，
  也不保证已授权目录内容不会被其他进程修改。宿主负责选择可信根目录。
- 不提供写入事务、覆盖冲突检测或磁盘配额；这些属于后续文件工具治理。

对应需求：[0025 / R1](../../docs/requirements/0025-architecture-hardening.md)。

## Trusted file operations (0030)

The isolated file, shell and combined four-tool adapters optionally accept
`with_process_observer(sender)` from a host-owned bounded Sandbox observation
channel. After normal exact preparation/grant validation, each launch binds a
strict `ToolProcessObservationContext` containing tool name, independent host
scope and prepared digest. It never takes scope from a grant or changes input,
policy, executable, deadline, output ceiling or tool result. Receiver loss is
explicit; the host must not interpret missing events as process completion.

Data-driven module tests demonstrate actual shell arithmetic entry followed by
existing explicit cancellation or dropped-future cleanup, and unchanged raw
stdout/stderr/nonzero exit. They record successful wait separately from group
cleanup and capture. This is local shell-native evidence, not Agent/Provider
network acceptance or ordinary file-worker inflight evidence. That file-native
gap remains open: the ordinary worker protocol has no progress handshake.

`FileOperations` (exported at the crate root) is a separate synchronous primitive for trusted
worker assembly; it does not change `RestrictedFileTool` or `RestrictedShellTool`.
It supplies `file.read`, `file.write`, and `file.edit` through a strictly decoded
`FileOperation` (`name` plus `arguments`). Unknown fields/tools are rejected.
Arguments are `path`; `path, content`; and `path, old_text, new_text,
expected_sha256?`, respectively. SHA-256 is hexadecimal over exact UTF-8 bytes.

- Host-owned `FileOperationLimits` bound read and write bytes (default 1 MiB).
  Oversized reads fail, never return misleading truncated success.
- Operations use the existing pinned directory capability. Absolute, parent,
  empty and NUL paths are invalid. Parent directories must already exist.
- Write/edit stage a new file in the pinned parent directory, sync it, then
  rename over the destination. A successful rename is the commit point; this
  is not a directory-fsync durability or hostile-writer compare-and-swap claim.
- Edit requires exactly one match, including overlapping occurrences. Empty,
  absent, ambiguous or stale matches fail without changing the destination.
  The optional digest is checked before staging, not an external-writer lock.
- Read permits in-root symlinks. Replacement rejects final symlinks (including
  in-root links) and nonregular targets; escaping parent symlinks also fail.
  Successful replacements create a new inode and do not preserve old metadata.
- Ordinary staging errors remove temporary files; process termination may leave
  an uncommitted staging file, but does not partially overwrite the target.
- This module neither authorizes calls nor provides a process sandbox. Worker
  assembly must apply grants and OS isolation, and blocking I/O must not run on
  an asynchronous executor thread. Existing trusted-root hardlink/mount caveats
  still apply. Source tests are in `src/file_operations/tests.rs`.
