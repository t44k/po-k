//! `PokJson<T>`: a JSON body extractor that fails with a JSON 400 naming the
//! problem. axum's own `Json` rejects with text/plain 415/422, which agents
//! and curl users routinely misread as "the endpoint returned nothing".
//! Content-Type is not required; an empty body is treated as `{}`.

use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::Json;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

pub struct PokJson<T>(pub T);

impl<S, T> FromRequest<S> for PokJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = (StatusCode, Json<Value>);

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": format!("bad body: {e}") }))))?;
        let slice: &[u8] = if bytes.is_empty() { b"{}" } else { &bytes };
        serde_json::from_slice::<T>(slice)
            .map(PokJson)
            .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": format!("bad body: {e}") }))))
    }
}
