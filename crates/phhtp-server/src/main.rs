use std::sync::Arc;

use clap::Parser;
use phhtp_core::{App, Limits};
use phhtp_server::{cores, default_workers, serve, Cli, Config};

fn main() {
    let cli = Cli::parse();
    let workers = cli.workers.unwrap_or_else(|| default_workers(cli.runtime));
    let app = Arc::new(
        App::new(cli.secret.into_bytes(), cli.runtime.name()).with_limits(Limits {
            max_head: cli.max_head,
            max_body: cli.max_body,
        }),
    );
    eprintln!(
        "phhtp runtime={} bind={} workers={} cores={}",
        cli.runtime.name(),
        cli.bind,
        workers,
        cores()
    );
    let cfg = Config {
        bind: cli.bind,
        workers,
        ready: None,
    };
    if let Err(e) = serve(cli.runtime, cfg, app) {
        eprintln!("phhtp: fatal: {e}");
        std::process::exit(1);
    }
}
