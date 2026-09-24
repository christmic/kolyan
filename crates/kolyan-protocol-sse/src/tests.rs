use super::*;

#[test]
fn all_sdk_line_endings_work_across_every_byte_boundary() {
    for ending in ["\n", "\r\n", "\r"] {
        let input = format!("event: text{ending}data: 中文🦀{ending}{ending}");
        for split in 0..=input.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&input.as_bytes()[..split]).unwrap();
            events.extend(decoder.push(&input.as_bytes()[split..]).unwrap());
            assert_eq!(
                events,
                vec![Event {
                    kind: "text".into(),
                    data: "中文🦀".into()
                }]
            );
            assert!(decoder.finish().is_ok());
        }
    }
}

#[test]
fn preserves_unicode_and_multiline_data_at_every_split() {
    for ending in ["\n", "\r\n"] {
        let input = format!(
            ": heartbeat{ending}event: delta{ending}data: 中文🦀{ending}data:  trailing {ending}{ending}"
        );
        for split in 0..=input.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&input.as_bytes()[..split]).unwrap();
            events.extend(decoder.push(&input.as_bytes()[split..]).unwrap());
            decoder.finish().unwrap();
            assert_eq!(
                events,
                vec![Event {
                    kind: "delta".into(),
                    data: "中文🦀\n trailing ".into()
                }]
            );
        }
    }
}

#[test]
fn handles_bytewise_chunks_and_multiple_events() {
    let mut decoder = Decoder::default();
    let mut events = Vec::new();
    for byte in "data: 中文\r\n\r\ndata:\n\n".as_bytes() {
        events.extend(decoder.push(&[*byte]).unwrap());
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].data, "中文");
    assert_eq!(events[1].data, "");
    decoder.finish().unwrap();
}

#[test]
fn rejects_truncated_invalid_and_oversized_frames() {
    for input in [
        b"data: partial".as_slice(),
        b"data: partial\n",
        b"event: delta\n",
    ] {
        let mut decoder = Decoder::default();
        assert!(decoder.push(input).unwrap().is_empty());
        assert!(matches!(decoder.finish(), Err(DecodeError::Truncated)));
    }
    assert!(matches!(
        Decoder::default().push(b"data: \xff\n\n"),
        Err(DecodeError::Utf8(_))
    ));
    assert!(matches!(
        Decoder::default().push(&vec![b'x'; MAX_EVENT_BYTES + 1]),
        Err(DecodeError::TooLarge)
    ));
}
