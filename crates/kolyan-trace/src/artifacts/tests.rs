use super::*;

#[test]
fn digest_reopen_bounds_and_retention() {
    let directory = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(directory.path(), 10).unwrap();
    let reference = store.put(b"abc", Retention::Optional).unwrap();
    assert_eq!(
        reference.digest,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(store.put(b"abc", Retention::Optional).unwrap(), reference);
    assert!(store.read(&reference, 2).is_err());
    assert!(store.put(&[0; 11], Retention::Optional).is_err());
    let required = store.put(b"abc", Retention::Required).unwrap();
    assert!(matches!(
        store.remove(&required),
        Err(ArtifactError::Required)
    ));
    let reopened = ArtifactStore::new(directory.path(), 10).unwrap();
    assert_eq!(reopened.read(&reference, 3).unwrap(), b"abc");
    assert!(matches!(
        reopened.remove(&reference),
        Err(ArtifactError::Required)
    ));
    let optional = store.put(b"delete", Retention::Optional).unwrap();
    store.remove(&optional).unwrap();
    assert!(matches!(
        store.read(&optional, 10),
        Err(ArtifactError::Io(_))
    ));
}

#[test]
fn corrupt_missing_length_and_path_fail_explicitly() {
    let directory = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(directory.path(), 10).unwrap();
    let mut reference = store.put(b"abc", Retention::Optional).unwrap();
    reference.byte_length = 2;
    assert!(matches!(
        store.read(&reference, 10),
        Err(ArtifactError::Integrity)
    ));
    reference.byte_length = 3;
    fs::write(directory.path().join(&reference.digest), b"bad").unwrap();
    assert!(matches!(
        store.read(&reference, 10),
        Err(ArtifactError::Integrity)
    ));
    assert!(store.put(b"abc", Retention::Optional).is_err());
    fs::remove_file(directory.path().join(&reference.digest)).unwrap();
    assert!(matches!(
        store.read(&reference, 10),
        Err(ArtifactError::Io(_))
    ));
    for digest in ["../escape".into(), "A".repeat(64), "z".repeat(64)] {
        reference.digest = digest;
        assert!(matches!(
            store.read(&reference, 10),
            Err(ArtifactError::Invalid(_))
        ));
    }
    assert!(ArtifactStore::new(directory.path(), 0).is_err());
}

#[cfg(unix)]
#[test]
fn symlink_content_is_not_followed() {
    let directory = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(directory.path(), 10).unwrap();
    let reference = store.put(b"abc", Retention::Optional).unwrap();
    let path = directory.path().join(&reference.digest);
    fs::remove_file(&path).unwrap();
    let outside = directory.path().join("outside");
    fs::write(&outside, b"abc").unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    assert!(store.read(&reference, 10).is_err());
    assert!(store.remove(&reference).is_err());
    assert_eq!(fs::read(outside).unwrap(), b"abc");
}

#[test]
fn concurrent_identical_writes_and_required_pin_survive() {
    let directory = tempfile::tempdir().unwrap();
    std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    ArtifactStore::new(directory.path(), 10)
                        .unwrap()
                        .put(b"abc", Retention::Required)
                        .unwrap()
                })
            })
            .collect();
        for job in jobs {
            assert_eq!(job.join().unwrap().byte_length, 3);
        }
    });
    let store = ArtifactStore::new(directory.path(), 10).unwrap();
    let reference = store.put(b"abc", Retention::Optional).unwrap();
    assert!(matches!(
        store.remove(&reference),
        Err(ArtifactError::Required)
    ));
}
