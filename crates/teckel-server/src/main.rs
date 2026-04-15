mod job;
mod routes;
mod worker;

use axum::{routing, Router};
use std::net::SocketAddr;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing_subscriber::EnvFilter;

use job::JobStore;
use worker::WorkerPool;

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub store: JobStore,
    pub pool: WorkerPool,
}

#[tokio::main]
async fn main() {
    // Initialize tracing (RUST_LOG=info by default)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let host = std::env::var("TECKEL_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("TECKEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let max_concurrency: usize = std::env::var("TECKEL_MAX_CONCURRENCY")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or_else(num_cpus::get);

    let store = JobStore::new();
    let pool = WorkerPool::new(store.clone(), max_concurrency);
    let state = AppState { store, pool };

    // TECKEL_CORS_ORIGINS: comma-separated list of allowed origins, or "*" for any.
    let cors_origins = std::env::var("TECKEL_CORS_ORIGINS").unwrap_or_else(|_| "*".to_string());
    let cors = CorsLayer::new().allow_methods(Any).allow_headers(Any);
    let cors = if cors_origins.trim() == "*" {
        tracing::warn!("CORS allow_origin = * (any). Set TECKEL_CORS_ORIGINS to restrict.");
        cors.allow_origin(Any)
    } else {
        let origins: Vec<_> = cors_origins
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse().expect("invalid origin in TECKEL_CORS_ORIGINS"))
            .collect();
        tracing::info!(?origins, "CORS allow_origin restricted");
        cors.allow_origin(AllowOrigin::list(origins))
    };

    let app = Router::new()
        .route("/api/health", routing::get(routes::health))
        .route("/api/validate", routing::post(routes::validate))
        .route("/api/explain", routing::post(routes::explain))
        .route(
            "/api/jobs",
            routing::post(routes::submit_job).get(routes::list_jobs),
        )
        .route(
            "/api/jobs/{id}",
            routing::get(routes::get_job).delete(routes::cancel_job),
        )
        .route("/api/jobs/{id}/wait", routing::get(routes::wait_job))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::new(host.parse().expect("invalid host"), port);
    tracing::info!(%addr, max_concurrency, "teckel-server starting");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");

    axum::serve(listener, app).await.expect("server error");
}
