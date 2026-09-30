# Workspace 工具边界

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
