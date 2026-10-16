use std::sync::Arc;

use clap::Parser;
use phhtp_core::{App, Limits};
use phhtp_server::{cores, default_workers, serve, Cli, Config};

fn main() {
    let cli = Cli::parse();
    let workers = cli.workers.unwrap_or_else(|| default_workers(cli.runtime));
    let mut app = App::new(cli.secret.into_bytes(), cli.runtime.name()).with_limits(Limits {
        max_head: cli.max_head,
        max_body: cli.max_body,
    });
    if let Some(dir) = &cli.static_dir {
        app = app.with_static_dir(dir);
    }
    let app = Arc::new(app);
    eprintln!(
        "phhtp runtime={} bind={} workers={} cores={}",
        cli.runtime.name(),
        cli.bind,
        workers,
        cores()
    );
    let mut cfg = Config::new(cli.bind, workers);
    cfg.idle_timeout =
        (cli.idle_timeout > 0).then(|| std::time::Duration::from_secs(cli.idle_timeout));
    cfg.max_conns = (cli.max_conns > 0).then_some(cli.max_conns);
    cfg.shutdown_grace = std::time::Duration::from_secs(cli.shutdown_grace);
    if let Err(e) = serve(cli.runtime, cfg, app) {
        eprintln!("phhtp: fatal: {e}");
        std::process::exit(1);
    }
}
