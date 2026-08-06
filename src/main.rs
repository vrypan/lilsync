//! Entry point. Parses CLI arguments, optionally daemonizes, initialises
//! logging, then dispatches to subcommand handlers or the sync daemon.

mod cli;
mod commands;
mod daemon;
mod endpoints;
mod entries;
mod group;
mod identity;
mod ignore;
mod logging;
mod message;
mod protocol;
mod rpc;
mod scan;
mod state;
mod sync;
mod transport;
mod tree;
mod ui;
mod watcher;

use crate::cli::{Cli, Command};
use crate::commands::{
    KEY_FILE, PID_FILE, create_invite, daemonize, dump_state_cmd, join_group, peers_cmd,
    remove_peer_cmd, status_cmd, stop_cmd,
};
use crate::daemon::run_sync;
use clap::Parser;
use std::fs;
use std::io;
use std::sync::Arc;

fn main() {
    let cli = Cli::parse();

    if let Some(log_path) = cli.daemon_log_path() {
        if let Err(err) = daemonize() {
            eprintln!("daemonize failed: {err}");
            std::process::exit(1);
        }
        // Write PID after fork so we record the grandchild's PID.
        let state_dir = log_path.parent().expect("log path has no parent");
        let _ = fs::write(state_dir.join(PID_FILE), std::process::id().to_string());
        if let Err(err) = logging::init_file_logging(log_path) {
            eprintln!("log init failed: {err}");
            std::process::exit(1);
        }
    } else {
        let default_filter = if cli.status_mode() { "warn" } else { "info" };
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
            )
            .with_level(true)
            .with_target(false)
            .init();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    if let Err(err) = rt.block_on(run(cli)) {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> io::Result<()> {
    match cli.command {
        Command::Invite {
            folder,
            expire_secs,
            endpoint,
        } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return create_invite(&state_dir, expire_secs, &endpoint);
        }
        Command::Remove { folder, target } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return remove_peer_cmd(&state_dir, &target);
        }
        Command::Peers { folder } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return peers_cmd(&state_dir);
        }
        Command::Status { folder } => {
            return status_cmd(&folder);
        }
        Command::Stop { folder } => {
            let state_dir = folder.join(".lil");
            return stop_cmd(&state_dir);
        }
        Command::DumpState { folder, prefix } => {
            return dump_state_cmd(&folder, prefix.as_deref());
        }
        Command::Join {
            folder,
            ticket,
            name,
            exit,
            status,
            port,
        } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            let identity = Arc::new(crate::identity::Identity::load_or_create(
                &state_dir.join(KEY_FILE),
            )?);
            let address_book = endpoints::new_address_book();
            join_group(Arc::clone(&identity), address_book, &state_dir, &ticket).await?;
            if exit {
                return Ok(());
            }
            run_sync(folder, name, false, 500, 10, status, port).await?;
        }
        Command::Start {
            folder,
            name,
            poll,
            interval_ms,
            announce_interval_secs,
            port,
        } => {
            run_sync(
                folder,
                name,
                poll,
                interval_ms,
                announce_interval_secs,
                false,
                port,
            )
            .await?
        }
        Command::Watch {
            folder,
            name,
            poll,
            interval_ms,
            announce_interval_secs,
            status,
            port,
        } => {
            run_sync(
                folder,
                name,
                poll,
                interval_ms,
                announce_interval_secs,
                status,
                port,
            )
            .await?
        }
    }
    Ok(())
}
