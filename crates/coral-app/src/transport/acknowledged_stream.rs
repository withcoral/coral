//! Acknowledged progress-event streams for long-running gRPC operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::{mpsc, oneshot};
use tokio_stream::Stream;
use tonic::Status;

use crate::bootstrap::AppError;

const EVENT_CHANNEL_CAPACITY: usize = 8;

pub(crate) struct AcknowledgedEventSender<E> {
    tx: mpsc::Sender<PendingAcknowledgedEvent<E>>,
    closed_message: &'static str,
}

impl<E> Clone for AcknowledgedEventSender<E> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            closed_message: self.closed_message,
        }
    }
}

impl<E: Send> AcknowledgedEventSender<E> {
    pub(crate) async fn send(&self, event: E) -> Result<(), AppError> {
        let (delivered, delivered_rx) = oneshot::channel();
        self.tx
            .send(PendingAcknowledgedEvent { event, delivered })
            .await
            .map_err(|_closed| AppError::FailedPrecondition(self.closed_message.to_string()))?;
        delivered_rx
            .await
            .map_err(|_closed| AppError::FailedPrecondition(self.closed_message.to_string()))
    }
}

struct PendingAcknowledgedEvent<E> {
    event: E,
    delivered: oneshot::Sender<()>,
}

impl<E> PendingAcknowledgedEvent<E> {
    fn into_event(self) -> E {
        // Acknowledges server dequeue, not network or application-level consumption.
        let _delivery = self.delivered.send(());
        self.event
    }
}

pub(crate) fn acknowledged_operation_response_stream<E, T, R, F, Fut>(
    closed_message: &'static str,
    operation: F,
    event_to_response: fn(E) -> R,
    result_to_response: impl FnOnce(T) -> R + Send + 'static,
) -> Pin<Box<dyn Stream<Item = Result<R, Status>> + Send>>
where
    E: Send + 'static,
    T: 'static,
    R: Send + Unpin + 'static,
    F: FnOnce(AcknowledgedEventSender<E>) -> Fut,
    Fut: Future<Output = Result<T, Status>> + Send + 'static,
{
    let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let event_sender = AcknowledgedEventSender {
        tx: event_tx,
        closed_message,
    };
    Box::pin(AcknowledgedOperationResponseStream {
        events: event_rx,
        operation: Some((
            Box::pin(operation(event_sender)),
            Box::new(result_to_response),
        )),
        completion: None,
        event_to_response,
    })
}

struct AcknowledgedOperationResponseStream<E, T, R> {
    events: mpsc::Receiver<PendingAcknowledgedEvent<E>>,
    #[expect(clippy::type_complexity, reason = "private one-shot operation slot")]
    operation: Option<(
        Pin<Box<dyn Future<Output = Result<T, Status>> + Send>>,
        Box<dyn FnOnce(T) -> R + Send>,
    )>,
    completion: Option<Result<R, Status>>,
    event_to_response: fn(E) -> R,
}

impl<E, T, R> AcknowledgedOperationResponseStream<E, T, R> {
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<R>> {
        Pin::new(&mut self.events)
            .poll_recv(cx)
            .map(|event| event.map(|event| (self.event_to_response)(event.into_event())))
    }
}

impl<E, T, R: Unpin> Stream for AcknowledgedOperationResponseStream<E, T, R> {
    type Item = Result<R, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Poll::Ready(Some(event)) = this.poll_event(cx) {
                return Poll::Ready(Some(Ok(event)));
            }
            if let Some(completion) = this.completion.take() {
                return Poll::Ready(Some(completion));
            }
            let Some((operation, _)) = this.operation.as_mut() else {
                return Poll::Ready(None);
            };
            match operation.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    if let Some((_, result_to_response)) = this.operation.take() {
                        this.completion = Some(result.map(result_to_response));
                    }
                }
                Poll::Pending => {
                    return match this.poll_event(cx) {
                        Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(event))),
                        Poll::Ready(None) | Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}
