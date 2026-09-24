use super::*;
use futures_util::{FutureExt, StreamExt, stream};

fn fixture(chunks: Vec<Vec<u8>>) -> MessageStream {
    MessageStream {
        body: Box::pin(stream::iter(
            chunks.into_iter().map(|bytes| Ok(bytes.into())),
        )),
        decoder: Decoder::default(),
        pending: VecDeque::new(),
        done: false,
        diagnostics: ResponseDiagnostics {
            status: 200,
            content_type: Some("text/event-stream".into()),
            content_encoding: None,
            received_bytes: 0,
            tail_preview: None,
        },
        capture_tail: false,
        tail: Vec::new(),
    }
}

#[test]
fn fatal_json_error_discards_following_events() {
    let events = fixture(vec![
        b"event: message_start\ndata: invalid\n\ndata: {\"type\":\"unused\"}\n\n".to_vec(),
    ])
    .collect::<Vec<_>>()
    .now_or_never()
    .unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], Err(AnthropicError::Decode(_))));
}

#[test]
fn unfinished_event_is_not_completed_at_eof() {
    let events = fixture(vec![b"data: {\"type\":\"unused\"}\n".to_vec()])
        .collect::<Vec<_>>()
        .now_or_never()
        .unwrap();
    assert!(events.is_empty());
}

#[test]
fn utf8_survives_each_transport_byte_boundary() {
    let bytes =
        "event: content_block_delta\r\ndata: {\"type\":\"test\",\"delta\":\"中文🦀\"}\r\n\r\n"
            .as_bytes();
    let events = fixture(bytes.iter().map(|b| vec![*b]).collect())
        .collect::<Vec<_>>()
        .now_or_never()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_ref().unwrap().fields["delta"], "中文🦀");
}
