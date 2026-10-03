//! HTTPS-facing API that translates HTTP requests into cache-server TCP commands.

pub mod client;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use cache_proto::{validate_key, validate_value, Command, ProtoError, Response};
use client::{CacheClient, ClientError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

/// Cache client shared by all request handlers.
type Shared = Arc<CacheClient>;

/// OpenAPI document for the API, served at `/openapi.json`.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "RustCache Web API",
        description = "HTTPS API in front of the RustCache in-memory cache server."
    ),
    paths(health, get_key, put_key, delete_key, expire_key, incr_key),
    components(schemas(KeyBody, PutBody, ExpireBody, IncrBody, ErrorBody, HealthBody))
)]
pub struct ApiDoc;

#[derive(Serialize, ToSchema)]
/// JSON body returned for every error.
struct ErrorBody {
    /// Human-readable description of the failure.
    error: String,
}

#[derive(Serialize, ToSchema)]
/// JSON body of the health check.
struct HealthBody {
    /// `ok` when the cache server answers, otherwise `cache unavailable`.
    status: String,
}

#[derive(Serialize, ToSchema)]
/// JSON body returned by the increment endpoint.
struct IncrBody {
    /// The value after incrementing.
    value: i64,
}

/// Builds the HTTP router with all API routes, Swagger UI and the OpenAPI spec.
///
/// # Arguments
/// * `client` - shared cache client used by every handler.
pub fn app(client: Shared) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .route("/healthz", get(health))
        .route(
            "/v1/keys/:key",
            get(get_key).put(put_key).delete(delete_key),
        )
        .route("/v1/keys/:key/expire", post(expire_key))
        .route("/v1/keys/:key/incr", post(incr_key))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(client)
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Invalid key, value or request body (HTTP 400).
    #[error("{0}")]
    BadRequest(String),
    /// The key does not exist (HTTP 404).
    #[error("key not found")]
    NotFound,
    /// The cache server rejected the command with an `ERR` reply (HTTP 422).
    #[error("{0}")]
    Cache(String),
    /// The cache server was unreachable (HTTP 502) or timed out (HTTP 504).
    #[error(transparent)]
    Upstream(#[from] ClientError),
}

impl From<ProtoError> for ApiError {
    /// Maps any validation failure to a 400 error.
    fn from(e: ProtoError) -> Self {
        ApiError::BadRequest(e.to_string())
    }
}

impl IntoResponse for ApiError {
    /// Renders the error as its HTTP status and a JSON `{"error": ...}` body.
    fn into_response(self) -> HttpResponse {
        let status = match &self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::Cache(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ApiError::Upstream(ClientError::Timeout) => StatusCode::GATEWAY_TIMEOUT,
            ApiError::Upstream(_) => StatusCode::BAD_GATEWAY,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

/// Sends a command and converts an `ERR` reply into [`ApiError::Cache`].
///
/// # Arguments
/// * `client` - cache client.
/// * `cmd` - command to send.
async fn call(client: &CacheClient, cmd: Command) -> Result<Response, ApiError> {
    match client.execute(&cmd).await? {
        Response::Err(m) => Err(ApiError::Cache(m)),
        r => Ok(r),
    }
}

/// Error for a reply type that does not fit the command that was sent.
fn unexpected() -> ApiError {
    ApiError::Cache("unexpected reply from cache server".into())
}

/// Cache server health.
///
/// Pings the cache server; no parameters.
#[utoipa::path(get, path = "/healthz", responses(
    (status = 200, description = "Cache server reachable", body = HealthBody),
    (status = 503, description = "Cache server unavailable", body = HealthBody)))]
async fn health(State(c): State<Shared>) -> impl IntoResponse {
    match c.execute(&Command::Ping).await {
        Ok(Response::Pong) => (StatusCode::OK, Json(json!({ "status": "ok" }))),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "cache unavailable" })),
        ),
    }
}

#[derive(Serialize, ToSchema)]
/// JSON body returned when reading a key.
struct KeyBody {
    /// The requested key.
    key: String,
    /// The stored value.
    value: String,
    /// Remaining time to live in seconds; absent when the key never expires.
    ttl_secs: Option<i64>,
}

/// Get a value and its remaining TTL.
///
/// Path parameter `key`: the key to read.
#[utoipa::path(get, path = "/v1/keys/{key}", params(("key" = String, Path, description = "Key (max 256 bytes, no whitespace)")), responses(
    (status = 200, body = KeyBody),
    (status = 400, body = ErrorBody),
    (status = 404, description = "Key not found", body = ErrorBody),
    (status = 502, description = "Cache server unreachable", body = ErrorBody),
    (status = 504, description = "Cache server timed out", body = ErrorBody)))]
async fn get_key(
    State(c): State<Shared>,
    Path(key): Path<String>,
) -> Result<Json<KeyBody>, ApiError> {
    validate_key(&key)?;
    let value = match call(&c, Command::Get(key.clone())).await? {
        Response::Value(v) => v,
        Response::Nil => return Err(ApiError::NotFound),
        _ => return Err(unexpected()),
    };
    let ttl_secs = match call(&c, Command::Ttl(key.clone())).await? {
        Response::Int(n) if n >= 0 => Some(n),
        _ => None,
    };
    Ok(Json(KeyBody {
        key,
        value,
        ttl_secs,
    }))
}

#[derive(Deserialize, ToSchema)]
/// JSON request body for storing a value.
struct PutBody {
    /// Value to store (no line breaks, at most 1 MiB).
    value: String,
    /// Optional time to live in seconds; omit for no expiry.
    ttl_secs: Option<u64>,
}

/// Create or replace a value, optionally with a TTL.
///
/// Path parameter `key`: the key to write. Body: [`PutBody`].
#[utoipa::path(put, path = "/v1/keys/{key}", params(("key" = String, Path, description = "Key")), request_body = PutBody, responses(
    (status = 204, description = "Stored"),
    (status = 400, body = ErrorBody),
    (status = 422, description = "Rejected by cache (e.g. key limit reached)", body = ErrorBody),
    (status = 502, body = ErrorBody),
    (status = 504, body = ErrorBody)))]
async fn put_key(
    State(c): State<Shared>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<StatusCode, ApiError> {
    validate_key(&key)?;
    validate_value(&body.value)?;
    match call(
        &c,
        Command::Set {
            key,
            value: body.value,
            ttl_secs: body.ttl_secs,
        },
    )
    .await?
    {
        Response::Ok => Ok(StatusCode::NO_CONTENT),
        _ => Err(unexpected()),
    }
}

/// Delete a key.
///
/// Path parameter `key`: the key to delete.
#[utoipa::path(delete, path = "/v1/keys/{key}", params(("key" = String, Path, description = "Key")), responses(
    (status = 204, description = "Deleted"),
    (status = 400, body = ErrorBody),
    (status = 404, description = "Key not found", body = ErrorBody),
    (status = 502, body = ErrorBody),
    (status = 504, body = ErrorBody)))]
async fn delete_key(
    State(c): State<Shared>,
    Path(key): Path<String>,
) -> Result<StatusCode, ApiError> {
    validate_key(&key)?;
    match call(&c, Command::Del(key)).await? {
        Response::Int(1) => Ok(StatusCode::NO_CONTENT),
        Response::Int(_) => Err(ApiError::NotFound),
        _ => Err(unexpected()),
    }
}

#[derive(Deserialize, ToSchema)]
/// JSON request body for changing a key's TTL.
struct ExpireBody {
    /// New time to live in seconds from now.
    ttl_secs: u64,
}

/// Set a new TTL on an existing key.
///
/// Path parameter `key`: the key to update. Body: [`ExpireBody`].
#[utoipa::path(post, path = "/v1/keys/{key}/expire", params(("key" = String, Path, description = "Key")), request_body = ExpireBody, responses(
    (status = 204, description = "TTL updated"),
    (status = 400, body = ErrorBody),
    (status = 404, description = "Key not found", body = ErrorBody),
    (status = 502, body = ErrorBody),
    (status = 504, body = ErrorBody)))]
async fn expire_key(
    State(c): State<Shared>,
    Path(key): Path<String>,
    Json(body): Json<ExpireBody>,
) -> Result<StatusCode, ApiError> {
    validate_key(&key)?;
    match call(
        &c,
        Command::Expire {
            key,
            secs: body.ttl_secs,
        },
    )
    .await?
    {
        Response::Int(1) => Ok(StatusCode::NO_CONTENT),
        Response::Int(_) => Err(ApiError::NotFound),
        _ => Err(unexpected()),
    }
}

/// Atomically increment an integer value (missing keys start at 0).
///
/// Path parameter `key`: the counter key.
#[utoipa::path(post, path = "/v1/keys/{key}/incr", params(("key" = String, Path, description = "Key")), responses(
    (status = 200, body = IncrBody),
    (status = 400, body = ErrorBody),
    (status = 422, description = "Value is not an integer, overflow, or key limit reached", body = ErrorBody),
    (status = 502, body = ErrorBody),
    (status = 504, body = ErrorBody)))]
async fn incr_key(
    State(c): State<Shared>,
    Path(key): Path<String>,
) -> Result<Json<IncrBody>, ApiError> {
    validate_key(&key)?;
    match call(&c, Command::Incr(key)).await? {
        Response::Int(n) => Ok(Json(IncrBody { value: n })),
        _ => Err(unexpected()),
    }
}
