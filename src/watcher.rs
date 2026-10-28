//! Observador assíncrono de mudanças no arquivo de configuração para Hot-Reload automático.
use crate::gateway::Gateway;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{error, info};

pub fn spawn_config_watcher(gw: Arc<Gateway>) {
    tokio::spawn(async move {
        let path = gw.config_path.clone();
        let mut last_mtime: Option<SystemTime> = tokio::fs::metadata(&path)
            .await
            .ok()
            .and_then(|m| m.modified().ok());

        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let Ok(meta) = tokio::fs::metadata(&path).await else {
                continue;
            };
            let Ok(mtime) = meta.modified() else {
                continue;
            };

            if Some(mtime) != last_mtime {
                last_mtime = Some(mtime);
                info!(path = %path, "Modificação detectada no YAML. Aplicando hot-reload...");
                if let Err(e) = gw.reload() {
                    error!(error = %e, "Falha ao recarregar configuração modificada");
                }
            }
        }
    });
}
