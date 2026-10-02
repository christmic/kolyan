//! Listener queue readiness and accepted-stream read mode are separate contracts.

use std::io::{BufRead, BufReader, ErrorKind};

use super::*;

#[test]
fn empty_accept_queue_and_explicit_stream_modes_have_distinct_outcomes() {
    let root = directory();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    // No client exists yet: this proves WouldBlock without a scheduling race.
    let empty = listener.accept().unwrap_err();
    super::super::super::append(
        &root.join("probe.jsonl"),
        &json!({"phase":"empty_accept","kind":format!("{:?}",empty.kind()),"errno":empty.raw_os_error()}),
    );

    listener.set_nonblocking(false).unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (stream, _) = listener.accept().unwrap();
    stream.set_nonblocking(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let started = Instant::now();
    let unread = reader.read_line(&mut line).unwrap_err();
    let elapsed = started.elapsed();
    super::super::super::append(
        &root.join("probe.jsonl"),
        &json!({"phase":"nonblocking_read","kind":format!("{:?}",unread.kind()),"elapsed_us":elapsed.as_micros(),"partial_line":line,"read_timeout_seconds":30}),
    );

    reader.get_ref().set_nonblocking(false).unwrap();
    let expected = "POST /v1/responses HTTP/1.1\r\n";
    client.write_all(expected.as_bytes()).unwrap();
    let read = reader.read_line(&mut line).unwrap();
    super::super::super::append(
        &root.join("probe.jsonl"),
        &json!({"phase":"blocking_read","read":read,"line":line}),
    );
    eprintln!("Listener/socket mode evidence: {}", root.display());
    assert_eq!(empty.kind(), ErrorKind::WouldBlock);
    assert_eq!(unread.kind(), ErrorKind::WouldBlock);
    assert!(elapsed < Duration::from_secs(1), "not a 30-second timeout");
    assert_eq!(read, expected.len());
    assert_eq!(line, expected);
}
