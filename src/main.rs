use buddy3d_proxy::init_tracing;

fn main() {
    init_tracing();
    tracing::info!("buddy3d-proxy starting");
}
