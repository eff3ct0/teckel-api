mod backend;

pub mod proto {
    tonic::include_proto!("teckel.v1");
}

pub use backend::RemoteBackend;
pub use proto::pipeline_service_client::PipelineServiceClient;
pub use proto::pipeline_service_server::{PipelineService, PipelineServiceServer};
