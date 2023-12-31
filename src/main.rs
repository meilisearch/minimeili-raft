use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use http::{external, internal};
use maplit::btreemap;
use openraft::error::{InitializeError, RaftError};
use openraft::{BasicNode, Config, Raft};
use raft::app::ExampleApp;
use raft::network::ExampleNetwork;

mod database;
mod http;
mod raft;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The database path where data is written.
    #[arg(short, long)]
    path: PathBuf,

    /// The HTTP interface exposed to the clients.
    #[arg(long, default_value = "0.0.0.0:7700")]
    external_addr: String,

    /// The HTTP interface exposed to members of the cluster.
    #[arg(long, default_value = "0.0.0.0:4400")]
    internal_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { path, external_addr, internal_addr } = Args::parse();

    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();

    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    std::fs::create_dir_all(&path)
        .with_context(|| format!("Could not create database directory at '{}'", path.display()))?;

    // Create a instance of where the Raft data will be stored.
    // We decided to implement the Raft traits on the whole Database,
    // It seems to be a good idea because we must apply entries to the IndexDatabase too!
    let database = database::Database::open_or_create(&path)
        .with_context(|| format!("Could not open database at '{}'", path.display()))?;

    let (node_uuid, nodes) = {
        let rtxn = database.raft.read_txn()?;
        let node_uuid = database.raft.uuid(&rtxn)?;
        let nodes = match database.raft.last_membership(&rtxn)? {
            Some(membership) => membership.nodes().map(|(a, b)| (*a, b.clone())).collect(),
            None => btreemap! { node_uuid => BasicNode { addr: internal_addr.clone() }},
        };
        (node_uuid, nodes)
    };

    let config = Arc::new(config.validate().unwrap());

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = ExampleNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(node_uuid, config.clone(), network, database.clone()).await.unwrap();

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let state =
        Arc::new(ExampleApp { id: node_uuid, addr: internal_addr.clone(), raft, database, config });

    // Initialize Raft by using the last_membership config.
    match state.raft.initialize(nodes).await {
        // It is safe to ignore, as it simply indicates that the cluster is already up
        // and running, which is ultimately the goal of this function.
        Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => (),
        e => panic!("{:?}", e),
    }

    let external_server =
        axum::Server::bind(&external_addr.parse().context("Could not parse external address")?)
            .serve(external::app(state.clone()).into_make_service());

    let cluster_server =
        axum::Server::bind(&internal_addr.parse().context("Could not parse internal address")?)
            .serve(internal::app(state).into_make_service());

    tokio::try_join!(external_server, cluster_server).context("Error on a server")?;

    Ok(())
}
