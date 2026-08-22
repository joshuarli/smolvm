//! Serde-backed JSON boundary for the h12tiny web API.
//!
//! h12tiny's published web crate intentionally uses miniserde. The smolvm API
//! already has a serde/serde_json contract (including its field naming and
//! custom serializers), so keep that contract local to the application edge.

use h12tiny::web::{Bytes, FromRequest, IntoResponse, Request, RequestMeta, Rejection, Response};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;

/// Serde-backed JSON request and response body.
#[derive(Clone, Debug, PartialEq)]
pub struct Json<T>(pub T);

impl<T> Json<T> {
    /// Construct a JSON body.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Unwrap the JSON body.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.0) {
            Ok(body) => {
                let mut response = Bytes::from(body).into_response();
                response.headers_mut().insert(
                    "content-type",
                    h12tiny::web::HeaderValue::from_static("application/json"),
                );
                response
            }
            Err(error) => Rejection::new(
                h12tiny::web::StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to serialize JSON response: {error}"),
            )
            .into_response(),
        }
    }
}

/// Optional serde-backed JSON request body.
#[derive(Clone, Debug, PartialEq)]
pub struct OptionalJson<T>(pub Option<T>);

impl<S, T> FromRequest<S> for OptionalJson<T>
where
    S: Send + Sync + 'static,
    T: DeserializeOwned + Send + 'static,
{
    type Rejection = Rejection;

    fn from_request(
        request: Request,
        state: &Option<S>,
        meta: &RequestMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(Self, Request), Self::Rejection>> + Send>> {
        let future = <Bytes as FromRequest<S>>::from_request(request, state, meta);
        Box::pin(async move {
            let (body, request) = future.await?;
            if body.is_empty() {
                return Ok((Self(None), request));
            }
            let value = serde_json::from_slice(&body).map_err(|error| {
                Rejection::new(
                    h12tiny::web::StatusCode::BAD_REQUEST,
                    format!("invalid JSON: {error}"),
                )
            })?;
            Ok((Self(Some(value)), request))
        })
    }
}

/// Serde-backed URL query extractor.
#[derive(Clone, Debug, PartialEq)]
pub struct Query<T>(pub T);

impl<S, T> FromRequest<S> for Query<T>
where
    S: Send + Sync + 'static,
    T: DeserializeOwned + Send + 'static,
{
    type Rejection = Rejection;

    fn from_request(
        request: Request,
        _state: &Option<S>,
        _meta: &RequestMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(Self, Request), Self::Rejection>> + Send>> {
        Box::pin(async move {
            let query = request.uri().query().unwrap_or_default();
            let value = serde_urlencoded::from_str(query).map_err(|error| {
                Rejection::new(
                    h12tiny::web::StatusCode::BAD_REQUEST,
                    format!("invalid query: {error}"),
                )
            })?;
            Ok((Self(value), request))
        })
    }
}

impl<S, T> FromRequest<S> for Json<T>
where
    S: Send + Sync + 'static,
    T: DeserializeOwned + Send + 'static,
{
    type Rejection = Rejection;

    fn from_request(
        request: Request,
        state: &Option<S>,
        meta: &RequestMeta,
    ) -> Pin<Box<dyn Future<Output = Result<(Self, Request), Self::Rejection>> + Send>> {
        let future = <Bytes as FromRequest<S>>::from_request(request, state, meta);
        Box::pin(async move {
            let (body, request) = future.await?;
            let value = serde_json::from_slice(&body).map_err(|error| {
                Rejection::new(
                    h12tiny::web::StatusCode::BAD_REQUEST,
                    format!("invalid JSON: {error}"),
                )
            })?;
            Ok((Self(value), request))
        })
    }
}
