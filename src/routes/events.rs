use super::AppState;
use axum::{
    extract::State,
    response::{Sse, sse::Event},
};
use std::convert::Infallible;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

pub async fn sse_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();

    let stream = BroadcastStream::new(rx).map(|msg| {
        let data = msg.unwrap_or_else(|_| "error".to_string());
        Ok(Event::default().data(data))
    });

    Sse::new(stream)
}
