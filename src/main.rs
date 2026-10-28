use api_gateway::{spawn_config_watcher, Gateway, GatewayConfig};
use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "api-gateway", about = "Enterprise L7 API Gateway & Reverse Proxy in Rust")]
struct Cli {
    /// Caminho do YAML de configuração.
    #[arg(short, long, default_value = "config/gateway.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cli = Cli::parse();
    let cfg = GatewayConfig::load(&cli.config)?;
    info!(
        listen = %cfg.listen,
        routes = cfg.routes.len(),
        "config carregada"
    );

    let gw = Gateway::new(cfg, cli.config.clone());
    spawn_config_watcher(gw.clone());

    #[cfg(unix)]
    {
        let gw_sighup = gw.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut sighup) = signal(SignalKind::hangup()) {
                loop {
                    sighup.recv().await;
                    info!("SIGHUP recebido! Executando hot-reload...");
                    if let Err(e) = gw_sighup.reload() {
                        error!(error = %e, "Falha no hot-reload por SIGHUP");
                    }
                }
            }
        });
    }

    gw.serve().await?;
    Ok(())
}
