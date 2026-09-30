# kolyan-sandbox

Real macOS Seatbelt execution for requirement 0030. This crate accepts trusted
host admission, not model instructions about permissions. It does not authorize,
write ledger facts, retry effects, or interpret tool-worker messages.

## Contract

- `SandboxExecutor`: object-safe async execution port; `MacOsSandbox` is its first
  concrete implementation. `SandboxFuture` permits host-selected trait objects.
- `MACOS_SEATBELT_POLICY_REVISION` / `policy_revision()`: trusted stable policy
  identity to bind alongside worker revision/hash and exact prepared resources.
  Policy semantics changes require a revision change.
- `SandboxConfig`: existing canonical read/write directory roots; write implies
  read. `protected_roots` denies existing control paths even under a writable
  root. `.git` and `.kolyan` paths are denied automatically, including creation.
- `SandboxRequest`: admitted canonical cwd, `SandboxCommand`, bounded stdin,
  positive timeout and combined stdout/stderr byte limit.
- `SandboxCommand::Executable`: trusted absolute executable and literal argv.
  Only that executable file is additionally readable/mappable; its parent tree
  is not implicitly granted. Suitable for a separately built trusted file worker.
- `SandboxCommand::Shell`: fixed `/bin/sh -c`, never login or interactive mode.
- `SandboxOutput`: raw stdout/stderr bytes and optional exit code. Nonzero exit
  remains an actual process outcome; `None` means signal termination.
- `SandboxCancellation`: explicit cancellation token. Dropping the future also
  signals cancellation. Cleanup runs independently of the async runtime.
- `SandboxError`: unsupported platform, missing backend, invalid admission,
  I/O/worker failure, timeout, cancellation or output overflow. No unsandboxed
  fallback, retries, arbitrary inherited environment, or arbitrary policy text.

The fixed backend is `/usr/bin/sandbox-exec`. Profiles start with deny-default;
roots use `-D` parameters, never interpolated SBPL strings. No network permission
is granted, including loopback. Environment is cleared, then PATH=/usr/bin:/bin,
LANG=C and LC_ALL=C are set. Shell-generated PWD/SHLVL are not inherited secrets.

The platform read/map allowance is restricted to `/bin`, `/usr/bin`, `/usr/lib`,
the selected executable, literal `/dev/null`, `/dev/urandom`, literal `/`, and
the host-owned `/private/var/select/sh` selector documented by macOS `sh(1)`.
The selector allowance does not grant access to the surrounding `/private/var`
tree. `/bin/sh` reexecutes the selected platform shell; it is not a user startup
file and does not enable login-shell configuration.
The literal root directory is needed by the current dyld cache boot sequence;
it is not a recursive root read. A small named sysctl allowance supports dyld and
Rust runtime page-size initialization. Framework/PrivateFramework trees, `/etc`,
user homes and preferences are not generally readable. Programs needing extra
libraries/resources fail unless the host explicitly admits appropriate roots.

## Process ownership and cleanup

A dedicated blocking worker owns the process and pipe readers. Polling uses safe
rustix `waitid(NOWAIT)` to retain the leader identity until group termination,
then safe nix `killpg(SIGKILL)` and `Child::wait`. Output is bounded across both
pipes, not independently doubled. Input writing cannot block the async runtime.
Timeout, cancellation, overflow, future drop and normal leader exit all terminate
the invocation group, including leftover background commands. Grandchildren are
reaped by their parent or the OS reaper; this host can directly wait only its child.

Process-group killing alone does **not** prevent detached-session escape. The
profile separately denies `setsid`, `setpgid`, and `posix_spawn` Unix syscalls.
The last deny closes the kernel spawn-flags path to new process groups/sessions.
Fork/exec within the existing group remains allowed. Consequently sandboxed Rust
`Command`/other posix-spawn-based child launching may fail even for otherwise
admitted programs. The trusted file worker should perform file operations itself;
shell execution is a separate host-started invocation, not worker-spawned shell.

Actual tests exercise a non-group-leader child attempting setsid, setpgid and
posix_spawn with new-group flags, and ordinary descendants after cancellation,
future drop, timeout and successful leader exit. Kernel syscall-filter support is
version-sensitive; these gates must run on every supported macOS version. Do not
advertise group-containment support on an untested OS just because a profile parses.

## Security limits

- Seatbelt is OS enforcement, not Apple's signed entitlement-based App Sandbox.
  The installed `sandbox-exec(1)` manual explicitly marks it deprecated. There is
  no assurance that a future macOS release retains this backend or policy syntax.
- Trusted roots must not contain secrets, uncontrolled hard links/mounts or
  hostile replacements. The host must enumerate additional/nested control paths
  with `protected_roots`; automatic names apply at each admitted root, not every
  possible nested directory. Root-directory name listing is visible.
- Host code, executable selection and grants remain trusted. Privileged callers,
  malicious host configuration, inherited non-CLOEXEC descriptors, kernel defects
  and effects outside the admitted filesystem contract are not contained here.
- Killing a process does not roll back writes or prove exactly-once shell effects.
  Runtime receipts and reconciliation remain required. Drop cleanup is asynchronous
  and assumes the host process stays alive; host death is not a completed cleanup.
- Timeout begins after spawn. Root/executable metadata checks are on the worker;
  this is not a host filesystem-latency bound or a CPU/memory/process-count quota.

Mechanism references: the sibling Codex Seatbelt adapter/profile was inspected for
fixed executable, parameterization and group lifecycle; policies are not copied.
Apple's XNU `bsd/kern/kern_prot.c` documents group/session operations. The local
Apple system profiles expose `syscall-unix` filtering. Both implementation and OS
tests must be reevaluated when the backend changes.

## Verification

`cargo test -p kolyan-sandbox` executes actual macOS enforcement tests: positive
admitted I/O, readonly denial, outside-root and symlink read/write denial,
protected metadata, exact argv/stdin, clean environment, parameter injection,
network EPERM (not merely service refusal), output bounds, timeout and cleanup.
Two test-harness subprocess entrypoints are probes, not separate acceptance gates.
Backend absence is tested without replacing the fixed OS executable. Non-macOS
construction returns `Unsupported` and never starts an unsandboxed process.

`cargo fmt -p kolyan-sandbox --check` and
`cargo clippy -p kolyan-sandbox --all-targets -- -D warnings` are the local checks.
File-helper integration and real Agent/Provider acceptance belong to the main
integration work, not this standalone crate's acceptance claim.
