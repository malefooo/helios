use std::path::PathBuf;
use std::sync::Arc;

use config::networks::Network;
use ethers::prelude::{Address, U256};
use ethers::types::{Filter, Log, Transaction, TransactionReceipt, H256};
use eyre::{eyre, Result};

use common::types::BlockTag;
use config::{ChannelMsgType, Config};
use consensus::types::Header;
use execution::types::{CallOpts, ExecutionBlock};
use log::{info, warn};
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::database::{Database, FileDB, RedisDB};
use crate::node::Node;
use crate::rpc::Rpc;

pub struct Client<DB: Database> {
    node: Arc<RwLock<Node>>,
    rpc: Option<Rpc>,
    db: Option<DB>,
}

pub enum ClientType {
    FileDB(Client<FileDB>),
    RedisDB(Client<RedisDB>),
}

impl ClientType {
    pub async fn start(&mut self) -> Result<()> {
        match self {
            ClientType::FileDB(c) => c.start().await,
            ClientType::RedisDB(c) => c.start().await,
        }
    }

    pub async fn shutdown(&self) {
        match self {
            ClientType::FileDB(c) => {
                c.shutdown().await;
            }
            ClientType::RedisDB(c) => {
                c.shutdown().await;
            }
        }
    }

    pub async fn receive(
        &self,
        receiver: Option<tokio::sync::mpsc::UnboundedReceiver<(ChannelMsgType, u64)>>,
    ) {
        if let Some(mut r) = receiver {
            match self {
                ClientType::FileDB(_) => {
                    return;
                }
                ClientType::RedisDB(c) => {
                    let db = c.db.as_ref().unwrap().clone(); //safe
                    spawn(async move {
                        loop {
                            if let Some((msg, slot)) = r.recv().await {

                                match msg {
                                    ChannelMsgType::NextCommitteeLeaf(msg) => {
                                        if let Some(msg) = msg {
                                            db.write_next_committee_leaf(msg, slot)
                                                .map_err(|e| info!("write msg to redis error: {:?}", e))
                                                .unwrap_or_default(); //safe
                                        }
                                    }
                                    ChannelMsgType::Update(_) => {}
                                    ChannelMsgType::HeaderLeaf(_) => {}
                                }

                            }
                        }
                    });
                }
            }
        } else {
            return;
        }
    }
}

impl Client<FileDB> {
    fn new_file_db(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let node = Node::new(config.clone(), None)?;
        let node = Arc::new(RwLock::new(node));

        let rpc = if let Some(port) = config.rpc_port {
            Some(Rpc::new(node.clone(), port))
        } else {
            None
        };

        let data_dir = config.data_dir.clone();
        let db = if let Some(dir) = data_dir {
            Some(FileDB::new(dir))
        } else {
            None
        };

        Ok(Client { node, rpc, db })
    }
}

impl Client<RedisDB> {
    fn new_redis_db(
        config: Config,
        sender: Option<tokio::sync::mpsc::UnboundedSender<(ChannelMsgType, u64)>>,
    ) -> Result<Self> {
        let config = Arc::new(config);
        let node = Node::new(config.clone(), sender)?;
        let node = Arc::new(RwLock::new(node));

        let rpc = if let Some(port) = config.rpc_port {
            Some(Rpc::new(node.clone(), port))
        } else {
            None
        };

        let redis_address = config.data_dir.clone();
        let db = if let Some(address) = redis_address {
            let address = address.to_str().unwrap().to_string(); //safe
            if !address.contains("redis://") {
                return Err(eyre::eyre!("not redis address, address: {}", address));
            }
            Some(RedisDB::new(address.as_str())?)
        } else {
            return Err(eyre::eyre!("redis address is none"));
        };

        Ok(Client { node, rpc, db })
    }
}

pub struct ClientBuilder {
    network: Option<Network>,
    consensus_rpc: Option<String>,
    execution_rpc: Option<String>,
    checkpoint: Option<Vec<u8>>,
    rpc_port: Option<u16>,
    data_dir: Option<PathBuf>,
    config: Option<Config>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            network: None,
            consensus_rpc: None,
            execution_rpc: None,
            checkpoint: None,
            rpc_port: None,
            data_dir: None,
            config: None,
        }
    }

    pub fn network(mut self, network: Network) -> Self {
        self.network = Some(network);
        self
    }

    pub fn consensus_rpc(mut self, consensus_rpc: &str) -> Self {
        self.consensus_rpc = Some(consensus_rpc.to_string());
        self
    }

    pub fn execution_rpc(mut self, execution_rpc: &str) -> Self {
        self.execution_rpc = Some(execution_rpc.to_string());
        self
    }

    pub fn checkpoint(mut self, checkpoint: &str) -> Self {
        let checkpoint = hex::decode(checkpoint).expect("cannot parse checkpoint");
        self.checkpoint = Some(checkpoint);
        self
    }

    pub fn rpc_port(mut self, port: u16) -> Self {
        self.rpc_port = Some(port);
        self
    }

    pub fn data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    fn build(self) -> Result<Config> {
        let base_config = if let Some(network) = self.network {
            network.to_base_config()
        } else {
            let config = self
                .config
                .as_ref()
                .ok_or(eyre!("missing network config"))?;
            config.to_base_config()
        };

        let consensus_rpc = self.consensus_rpc.unwrap_or_else(|| {
            self.config
                .as_ref()
                .expect("missing consensus rpc")
                .consensus_rpc
                .clone()
        });

        let execution_rpc = self.execution_rpc.unwrap_or_else(|| {
            self.config
                .as_ref()
                .expect("missing execution rpc")
                .execution_rpc
                .clone()
        });

        let checkpoint = if let Some(checkpoint) = self.checkpoint {
            checkpoint
        } else if let Some(config) = &self.config {
            config.checkpoint.clone()
        } else {
            base_config.checkpoint
        };

        let rpc_port = if self.rpc_port.is_some() {
            self.rpc_port
        } else if let Some(config) = &self.config {
            config.rpc_port
        } else {
            None
        };

        let data_dir = if self.data_dir.is_some() {
            self.data_dir
        } else if let Some(config) = &self.config {
            config.data_dir.clone()
        } else {
            None
        };

        let config = Config {
            consensus_rpc,
            execution_rpc,
            checkpoint,
            rpc_port,
            data_dir,
            chain: base_config.chain,
            forks: base_config.forks,
            max_checkpoint_age: base_config.max_checkpoint_age,
        };

        Ok(config)
    }

    pub fn build_file_db(self) -> Result<Client<FileDB>> {
        Client::new_file_db(self.build()?)
    }

    pub fn build_redis_db(
        self,
        sender: Option<tokio::sync::mpsc::UnboundedSender<(ChannelMsgType, u64)>>,
    ) -> Result<Client<RedisDB>> {
        Client::new_redis_db(self.build()?, sender)
    }
}

impl<DB: Database> Client<DB> {
    pub async fn start(&mut self) -> Result<()> {
        if let Some(rpc) = &mut self.rpc {
            rpc.start().await?;
        }

        let res = self.node.write().await.sync().await;
        if let Err(err) = res {
            warn!("consensus error: {:?}", err);
        }

        let node = self.node.clone();
        spawn(async move {
            loop {
                let res = node.write().await.advance().await;
                if let Err(err) = res {
                    warn!("consensus error: {:?}", err);
                }

                let next_update = node.read().await.duration_until_next_update();
                sleep(next_update).await;
            }
        });

        Ok(())
    }

    pub async fn shutdown(&self) {
        let node = self.node.read().await;
        let checkpoint = if let Some(checkpoint) = node.get_last_checkpoint() {
            checkpoint
        } else {
            return;
        };

        info!("saving last checkpoint hash");
        let res = self.db.as_ref().map(|db| db.save_checkpoint(checkpoint));
        if res.is_some() && res.unwrap().is_err() {
            warn!("checkpoint save failed");
        }
    }

    pub async fn call(&self, opts: &CallOpts, block: BlockTag) -> Result<Vec<u8>> {
        self.node
            .read()
            .await
            .call(opts, block)
            .await
            .map_err(|err| err.into())
    }

    pub async fn estimate_gas(&self, opts: &CallOpts) -> Result<u64> {
        self.node
            .read()
            .await
            .estimate_gas(opts)
            .await
            .map_err(|err| err.into())
    }

    pub async fn get_balance(&self, address: &Address, block: BlockTag) -> Result<U256> {
        self.node.read().await.get_balance(address, block).await
    }

    pub async fn get_nonce(&self, address: &Address, block: BlockTag) -> Result<u64> {
        self.node.read().await.get_nonce(address, block).await
    }

    pub async fn get_code(&self, address: &Address, block: BlockTag) -> Result<Vec<u8>> {
        self.node.read().await.get_code(address, block).await
    }

    pub async fn get_storage_at(&self, address: &Address, slot: H256) -> Result<U256> {
        self.node.read().await.get_storage_at(address, slot).await
    }

    pub async fn send_raw_transaction(&self, bytes: &Vec<u8>) -> Result<H256> {
        self.node.read().await.send_raw_transaction(bytes).await
    }

    pub async fn get_transaction_receipt(
        &self,
        tx_hash: &H256,
    ) -> Result<Option<TransactionReceipt>> {
        self.node
            .read()
            .await
            .get_transaction_receipt(tx_hash)
            .await
    }

    pub async fn get_transaction_by_hash(&self, tx_hash: &H256) -> Result<Option<Transaction>> {
        self.node
            .read()
            .await
            .get_transaction_by_hash(tx_hash)
            .await
    }

    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        self.node.read().await.get_logs(filter).await
    }

    pub async fn get_gas_price(&self) -> Result<U256> {
        self.node.read().await.get_gas_price()
    }

    pub async fn get_priority_fee(&self) -> Result<U256> {
        self.node.read().await.get_priority_fee()
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        self.node.read().await.get_block_number()
    }

    pub async fn get_block_by_number(
        &self,
        block: BlockTag,
        full_tx: bool,
    ) -> Result<Option<ExecutionBlock>> {
        self.node
            .read()
            .await
            .get_block_by_number(block, full_tx)
            .await
    }

    pub async fn get_block_by_hash(
        &self,
        hash: &Vec<u8>,
        full_tx: bool,
    ) -> Result<Option<ExecutionBlock>> {
        self.node
            .read()
            .await
            .get_block_by_hash(hash, full_tx)
            .await
    }

    pub async fn chain_id(&self) -> u64 {
        self.node.read().await.chain_id()
    }

    pub async fn get_header(&self) -> Result<Header> {
        self.node.read().await.get_header()
    }
}
