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

type Shared = Arc<CacheClient>;

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
struct ErrorBody {
    error: String,
}

#[derive(Serialize, ToSchema)]
struct HealthBody {
    status: String,
}

#[derive(Serialize, ToSchema)]
struct IncrBody {
    value: i64,
}

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
    #[error("{0}")]
    BadRequest(String),
    #[error("key not found")]
    NotFound,
    #[error("{0}")]
    Cache(String),
    #[error(transparent)]
    Upstream(#[from] ClientError),
}

impl From<ProtoError> for ApiError {
    fn from(e: ProtoError) -> Self {
        ApiError::BadRequest(e.to_string())
    }
}

impl IntoResponse for ApiError {
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

async fn call(client: &CacheClient, cmd: Command) -> Result<Response, ApiError> {
    match client.execute(&cmd).await? {
        Response::Err(m) => Err(ApiError::Cache(m)),
        r => Ok(r),
    }
}

fn unexpected() -> ApiError {
    ApiError::Cache("unexpected reply from cache server".into())
}

/// Cache server health.
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
struct KeyBody {
    key: String,
    value: String,
    ttl_secs: Option<i64>,
}

/// Get a value and its remaining TTL.
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
struct PutBody {
    value: String,
    ttl_secs: Option<u64>,
}

/// Create or replace a value, optionally with a TTL.
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
struct ExpireBody {
    ttl_secs: u64,
}

/// Set a new TTL on an existing key.
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
