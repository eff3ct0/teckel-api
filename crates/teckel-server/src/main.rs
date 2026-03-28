mod job;
mod routes;
mod worker;

use axum::{routing, Router};
use std::net::SocketAddr;
use tower_http::cors::{Any, CorsLayer};
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

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

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
