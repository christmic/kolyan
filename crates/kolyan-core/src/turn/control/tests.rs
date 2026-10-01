use super::*;

use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use futures_util::{FutureExt, future::poll_fn};

#[tokio::test]
async fn cancellation_wakes_every_registered_parallel_consumer() {
    let control = TurnControl::default();
    let ready = Arc::new(tokio::sync::Barrier::new(65));
    let mut subscribers = Vec::new();
    for _ in 0..64 {
        let control = control.clone();
        let ready = ready.clone();
        subscribers.push(tokio::spawn(async move {
            let mut cancelled = Box::pin(control.cancelled());
            poll_fn(|cx| {
                assert!(cancelled.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            ready.wait().await;
            cancelled.await;
        }));
    }
    ready.wait().await;
    control.cancel();
    tokio::time::timeout(Duration::from_secs(2), async {
        for subscriber in subscribers {
            subscriber.await.unwrap();
        }
    })
    .await
    .expect("all registered cancellation consumers must wake");
}

#[tokio::test]
async fn early_cancel_is_retained_and_approval_does_not_cancel() {
    let control = TurnControl::default();
    let mut cancelled = Box::pin(control.cancelled());
    assert!(cancelled.as_mut().now_or_never().is_none());
    control.approve_tool("file.write");
    assert!(cancelled.as_mut().now_or_never().is_none());
    control.cancel();
    tokio::time::timeout(Duration::from_secs(1), cancelled)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), control.cancelled())
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_approval_waiters_clears_only_their_own_markers() {
    let control = TurnControl::default();
    let mut first = Box::pin(control.wait_for_tool_approval("file.write"));
    let mut second = Box::pin(control.wait_for_tool_approval("file.write"));
    assert!(first.as_mut().now_or_never().is_none());
    assert!(second.as_mut().now_or_never().is_none());
    assert!(control.is_waiting_for_approval("file.write"));
    drop(first);
    assert!(control.is_waiting_for_approval("file.write"));
    control.approve_tool("file.write");
    tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .unwrap();
    assert!(!control.is_waiting_for_approval("file.write"));
    let mut another = Box::pin(control.wait_for_tool_approval("file.write"));
    assert!(
        another.as_mut().now_or_never().is_none(),
        "one approval must be consumed once"
    );
    drop(another);
    assert!(!control.is_waiting_for_approval("file.write"));
}
