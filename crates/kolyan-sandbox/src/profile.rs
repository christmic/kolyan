//! Profile syntax is host-generated; paths are passed exclusively through -D.

use std::path::{Path, PathBuf};

use crate::{SandboxCommand, SandboxConfig, SandboxError, SandboxRequest};

// Non-macOS construction fails before these process-only fields are consumed.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(crate) struct Admitted {
    pub request: SandboxRequest,
    pub profile: String,
    pub parameters: Vec<String>,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
}

pub(crate) fn canonical_config(mut config: SandboxConfig) -> Result<SandboxConfig, SandboxError> {
    for root in config
        .read_roots
        .iter_mut()
        .chain(config.write_roots.iter_mut())
    {
        *root = root.canonicalize()?;
        if !root.is_dir() || root.parent().is_none() {
            return Err(SandboxError::Invalid(
                "roots must be existing non-root directories".into(),
            ));
        }
        path_text(root)?;
    }
    if config.read_roots.is_empty() && config.write_roots.is_empty() {
        return Err(SandboxError::Invalid(
            "at least one admitted root is required".into(),
        ));
    }
    for root in &mut config.protected_roots {
        *root = root.canonicalize()?;
        path_text(root)?;
    }
    for root in config.read_roots.iter().chain(config.write_roots.iter()) {
        for name in [".git", ".kolyan"] {
            let control = root.join(name);
            if control.exists() {
                config.protected_roots.push(control.canonicalize()?);
            }
            config.protected_roots.push(control);
        }
    }
    Ok(config)
}

pub(crate) fn admit(
    config: &SandboxConfig,
    mut request: SandboxRequest,
) -> Result<Admitted, SandboxError> {
    if request.timeout.is_zero()
        || request.max_output_bytes == 0
        || request.stdin.len() > request.max_output_bytes
    {
        return Err(SandboxError::Invalid(
            "positive timeout/output cap and bounded stdin required".into(),
        ));
    }
    request.cwd = request.cwd.canonicalize()?;
    if !request.cwd.is_dir()
        || !config
            .read_roots
            .iter()
            .chain(config.write_roots.iter())
            .any(|r| request.cwd.starts_with(r))
    {
        return Err(SandboxError::Invalid("cwd outside admitted roots".into()));
    }
    let (executable, arguments) = match &request.command {
        SandboxCommand::Shell(script) => {
            (PathBuf::from("/bin/sh"), vec!["-c".into(), script.clone()])
        }
        SandboxCommand::Executable { path, arguments } => {
            if !path.is_absolute() {
                return Err(SandboxError::Invalid("executable must be absolute".into()));
            }
            (path.canonicalize()?, arguments.clone())
        }
    };
    let mut parameters = vec![format!("EXEC={}", path_text(&executable)?)];
    // No user configuration directories, network permissions, or blanket reads.
    let mut profile = String::from(
        "(version 1)(deny default)\n(allow process-exec process-fork)\n(allow signal (target same-sandbox))\n(allow file-read* file-map-executable (literal (param \"EXEC\")))\n(allow file-read* file-map-executable (subpath \"/bin\") (subpath \"/usr/bin\") (subpath \"/usr/lib\"))\n(allow sysctl-read (sysctl-name \"security.mac.lockdown_mode_state\") (sysctl-name \"kern.bootargs\"))\n(allow file-read* file-write-data (literal \"/dev/null\"))\n(allow file-read* (literal \"/dev/urandom\") (literal \"/\"))\n(allow file-read-metadata (literal \"/dev\"))\n",
    );
    // macOS sh(1) selects bash/dash/zsh through this host-owned symlink.
    // Denying its read makes the launcher emit an error before command stderr.
    profile.push_str("(allow file-read* (literal \"/private/var/select/sh\"))\n");
    for (index, root) in config
        .read_roots
        .iter()
        .chain(config.write_roots.iter())
        .enumerate()
    {
        parameters.push(format!("ROOT{index}={}", path_text(root)?));
        profile.push_str(&format!("(allow file-read* (subpath (param \"ROOT{index}\")))\n(allow file-read-metadata (path-ancestors (param \"ROOT{index}\")))\n"));
        if index >= config.read_roots.len() {
            profile.push_str(&format!(
                "(allow file-write* (subpath (param \"ROOT{index}\")))\n"
            ));
        }
    }
    // posix_spawn can create a group/session without a setsid/setpgid syscall.
    profile.push_str("(allow sysctl-read (sysctl-name \"hw.pagesize\") (sysctl-name \"hw.pagesize_compat\") (sysctl-name \"hw.ncpu\") (sysctl-name \"kern.osproductversion\"))\n(allow syscall-unix)\n(deny syscall-unix (syscall-number SYS_setsid) (syscall-number SYS_setpgid) (syscall-number SYS_posix_spawn))\n");
    for (index, root) in config.protected_roots.iter().enumerate() {
        parameters.push(format!("PROTECT{index}={}", path_text(root)?));
        profile.push_str(&format!(
            "(deny file-read* file-write* (subpath (param \"PROTECT{index}\")))\n"
        ));
    }
    Ok(Admitted {
        request,
        profile,
        parameters,
        executable,
        arguments,
    })
}

fn path_text(path: &Path) -> Result<&str, SandboxError> {
    path.to_str()
        .filter(|p| !p.contains(['\0', '\n', '\r']))
        .ok_or_else(|| SandboxError::Invalid("path must be valid single-line UTF-8".into()))
}
