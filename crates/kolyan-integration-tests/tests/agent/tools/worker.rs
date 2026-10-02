//! Explicit run installation; each case saves an exact Ready artifact reference.
//! Reconstruction verifies durable evidence and never runs another bootstrap.

mod bootstrap;
mod bootstrap_tests;
mod installation;
mod readiness_tests;
mod startup;
mod tests;

use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::Evidence;
use installation::{InstalledWorker, ReadyEvidence, checked_file, digest, write_private};

pub struct WorkerRun {
    root: PathBuf,
    ready: Result<ReferencePin, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferencePin {
    schema_version: u32,
    installation: InstalledWorker,
    ready_path: PathBuf,
    ready_sha256: String,
}

impl WorkerRun {
    pub async fn prepare() -> Self {
        Self::prepare_from(Path::new(env!("CARGO_BIN_EXE_kolyan-test-tool-worker"))).await
    }

    pub(crate) async fn prepare_from(source: &Path) -> Self {
        let source = source.to_path_buf();
        tokio::task::spawn_blocking(move || Self::install(&source))
            .await
            .expect("installation host task")
    }

    fn install(source: &Path) -> Self {
        let root = tempfile::Builder::new()
            .prefix("kolyan-worker-installation-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap()
            .keep()
            .canonicalize()
            .unwrap();
        let evidence = Evidence::new(&root.join("actual.jsonl"));
        println!(
            "WORKER_INSTALLATION_TRACE={}",
            root.join("actual.jsonl").display()
        );
        let ready = (|| {
            let installed = installation::install(&root, source, &evidence)?;
            let config = bootstrap::dataset()?;
            let observed = bootstrap::launch(&installed, &config, &evidence)?;
            installation::verify(&installed)?;
            let ready_path = root.join("ready.json");
            let bytes = serde_json::to_vec(&observed).map_err(|error| error.to_string())?;
            evidence.append(json!({"event":"installation_ready_intent","evidence":observed}))?;
            write_private(&ready_path, &bytes, 0o400)?;
            let pin = ReferencePin {
                schema_version: 2,
                installation: installed,
                ready_path,
                ready_sha256: digest(&bytes),
            };
            evidence.append(json!({"event":"installation_ready","reference":pin}))?;
            validate(&pin)?;
            Ok(pin)
        })();
        if let Err(error) = &ready {
            evidence.append(json!({"event":"installation_failed","error":error,"model_admission":"blocked"})).expect("installation failure evidence");
        }
        Self { root, ready }
    }

    pub fn bind_case(&self, root: &Path, evidence: &Evidence) -> Result<(), String> {
        evidence.append(json!({"event":"worker_case_admission","run_root":self.root,"reference":self.ready.as_ref().ok(),"error":self.ready.as_ref().err(),"model_admission":if self.ready.is_ok(){"pending_verification"}else{"blocked"}}))?;
        let pin = self.ready.as_ref().map_err(Clone::clone)?;
        validate(pin)?;
        let directory = root.join("trusted-worker");
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .map_err(|error| error.to_string())?;
        write_private(
            &directory.join("pin.json"),
            &serde_json::to_vec(pin).map_err(|error| error.to_string())?,
            0o400,
        )?;
        fs::File::open(&directory)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())?;
        verified_worker(root, evidence)?;
        Ok(())
    }
}

pub fn initialize_worker(root: &Path, evidence: &Evidence, run: &WorkerRun) -> Result<(), String> {
    run.bind_case(root, evidence)
}

pub(crate) fn verified_worker(root: &Path, evidence: &Evidence) -> Result<PathBuf, String> {
    let result: Result<ReferencePin, String> = (|| {
        let physical = root.canonicalize().map_err(|error| error.to_string())?;
        let path = physical.join("trusted-worker/pin.json");
        checked_file(&path, true)?;
        let pin: ReferencePin =
            serde_json::from_slice(&fs::read(path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        validate(&pin)?;
        Ok(pin)
    })();
    evidence.append(json!({"event":"trusted_worker_verification","reference":result.as_ref().ok(),"error":result.as_ref().err()}))?;
    let pin = result?;
    Ok(pin.installation.path)
}

fn validate(pin: &ReferencePin) -> Result<(), String> {
    if pin.schema_version != 2 {
        return Err("unsupported case worker reference schema".into());
    }
    installation::verify(&pin.installation)?;
    if pin.ready_path != pin.installation.root.join("ready.json") {
        return Err("Ready path does not match installation".into());
    }
    checked_file(&pin.ready_path, true)?;
    let bytes = fs::read(&pin.ready_path).map_err(|error| error.to_string())?;
    if digest(&bytes) != pin.ready_sha256 {
        return Err("worker Ready evidence digest mismatch".into());
    }
    let ready: ReadyEvidence = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    ready.validate(&pin.installation)
}
