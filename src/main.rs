use stan::common::shutdown::Shutdown;

#[tokio::main]
async fn main() {
    let shutdown = Shutdown::new();

    let signal = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("shutting down");
            signal.signal();
        }
    });

    if let Err(e) = stan::start_with(shutdown).await {
        eprintln!("Something went wrong {}", e)
    }
}
