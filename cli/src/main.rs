use std::{
    fs,
    path::PathBuf,
    process::exit,
    str::FromStr,
    sync::{Arc, Mutex},
};

use clap::Parser;
use common::utils::hex_str_to_bytes;
use dirs::home_dir;
use env_logger::Env;
use eyre::{eyre, Result};

use client::{ClientBuilder, ClientType};
use config::{ChannelMsgType, CliConfig, Config, DBType};
use futures::executor::block_on;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let (config, db_type) = get_config();

    let (mut client, receiver) = match db_type {
        DBType::File => (
            ClientType::FileDB(ClientBuilder::new().config(config).build_file_db()?),
            None,
        ),
        DBType::Redis => {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<(ChannelMsgType, u64)>();
            (
                ClientType::RedisDB(
                    ClientBuilder::new()
                        .config(config)
                        .build_redis_db(Some(sender))?,
                ),
                Some(receiver),
            )
        }
        _ => {
            return Err(eyre!("not impl db type"));
        }
    };

    client.start().await?;

    let arc_client = Arc::new(client);

    arc_client.clone().receive(receiver).await;

    register_shutdown_handler(arc_client);

    std::future::pending().await
}

fn register_shutdown_handler(client_type: Arc<ClientType>) {
    // let client = Arc::new(client_type);
    let shutdown_counter = Arc::new(Mutex::new(0));

    ctrlc::set_handler(move || {
        let mut counter = shutdown_counter.lock().unwrap();
        *counter += 1;

        let counter_value = *counter;

        if counter_value == 3 {
            info!("forced shutdown");
            exit(0);
        }

        info!(
            "shutting down... press ctrl-c {} more times to force quit",
            3 - counter_value
        );

        if counter_value == 1 {
            let client = client_type.clone();
            std::thread::spawn(move || {
                block_on(client.shutdown());
                exit(0);
            });
        }
    })
    .expect("could not register shutdown handler");
}

fn get_config() -> (Config, DBType) {
    let cli = Cli::parse();

    let config_path = home_dir().unwrap().join(".helios/helios.toml");

    let cli_config: CliConfig = cli.as_cli_config();

    (
        Config::from_file(&config_path, &cli.network, &cli_config),
        cli_config.db_type,
    )
}

#[derive(Parser)]
struct Cli {
    #[clap(short, long, default_value = "mainnet")]
    network: String,
    #[clap(short = 'p', long, env)]
    rpc_port: Option<u16>,
    #[clap(short = 'w', long, env)]
    checkpoint: Option<String>,
    #[clap(short, long, env)]
    execution_rpc: Option<String>,
    #[clap(short, long, env)]
    consensus_rpc: Option<String>,
    #[clap(short, long, env)]
    /// db_type=file => "~/.helios", db_type=redis => "redis://127.0.0.1/0",
    data_dir: Option<String>,
    #[clap(long, env, default_value = "file")]
    db_type: String,
}

impl Cli {
    fn as_cli_config(&self) -> CliConfig {
        let checkpoint = match &self.checkpoint {
            Some(checkpoint) => Some(hex_str_to_bytes(&checkpoint).expect("invalid checkpoint")),
            None => self.get_cached_checkpoint(),
        };

        CliConfig {
            checkpoint,
            execution_rpc: self.execution_rpc.clone(),
            consensus_rpc: self.consensus_rpc.clone(),
            data_dir: self.get_data_dir(),
            rpc_port: self.rpc_port,
            db_type: self.get_db_type(),
        }
    }

    fn get_cached_checkpoint(&self) -> Option<Vec<u8>> {
        let data_dir = self.get_data_dir();
        let checkpoint_file = data_dir.join("checkpoint");

        if checkpoint_file.exists() {
            let checkpoint_res = fs::read(checkpoint_file);
            match checkpoint_res {
                Ok(checkpoint) => Some(checkpoint),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn get_data_dir(&self) -> PathBuf {
        if let Some(dir) = &self.data_dir {
            PathBuf::from_str(dir).expect("cannot find data dir")
        } else {
            home_dir()
                .unwrap()
                .join(format!(".helios/data/{}", self.network))
        }
    }

    fn get_db_type(&self) -> DBType {
        let db_type: DBType = self.db_type.clone().into();
        db_type
    }
}
