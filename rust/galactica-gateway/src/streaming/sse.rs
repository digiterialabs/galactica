use axum::response::sse::{Event, Sse};
use futures::stream::Stream;
use std::convert::Infallible;

/// Create an SSE stream for streaming chat completions.
///
/// Each chunk follows the OpenAI streaming format: `data: {...}\n\n`
pub fn create_completion_stream() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        // TODO: receive chunks from runtime backend and yield SSE events
        yield Ok(Event::default().data("[DONE]"));
    };

    Sse::new(stream)
}
