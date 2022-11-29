use std::{fs, io::Write, path::PathBuf};

use eyre::Result;
use redis::{Client, Commands};

pub trait Database: Send + Sync {
    fn save_checkpoint(&self, checkpoint: Vec<u8>) -> Result<()>;
}

pub struct FileDB {
    data_dir: PathBuf,
}

impl FileDB {
    pub fn new(data_dir: PathBuf) -> Self {
        FileDB { data_dir }
    }
}

impl Database for FileDB {
    fn save_checkpoint(&self, checkpoint: Vec<u8>) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;

        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.data_dir.join("checkpoint"))?;

        f.write_all(checkpoint.as_slice())?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct RedisDB {
    client: Client,
}

impl RedisDB {
    pub fn new(address: &str) -> Result<Self> {
        let client = Client::open(address)?;
        Ok(Self { client })
    }

    pub fn write_light_client_store(&self, value: String, slot: u64) -> Result<()> {
        let key = self.create_store_key(slot);
        let mut con = self.client.get_connection()?;
        con.set(key, value)?;
        Ok(())
    }

    pub fn write_header_leaf(&self, value: String, slot: u64) -> Result<()> {
        let key = self.create_header_leaf_key(slot);
        let mut con = self.client.get_connection()?;
        con.set(key, value)?;
        Ok(())
    }

    pub fn write_next_committee_leaf(&self, value: String, slot: u64) -> Result<()> {
        let key = self.create_next_committee_leaf_key(slot);
        let mut con = self.client.get_connection()?;
        con.set(key, value)?;

        Ok(())
    }

    fn create_store_key(&self, slot: u64) -> String {
        format!("store-slot-{}", slot)
    }

    fn create_checkpoint_key(&self) -> String {
        format!("checkpoint-slot")
    }

    fn create_header_leaf_key(&self, slot: u64) -> String{
        format!("header-leaf-slot-{}",slot)
    }

    fn create_next_committee_leaf_key(&self, slot: u64) -> String{
        format!("next-committee-leaf-slot-{}",slot)
    }
}

impl Database for RedisDB {
    fn save_checkpoint(&self, checkpoint: Vec<u8>) -> Result<()> {
        let key = self.create_checkpoint_key();
        let mut con = self.client.get_connection()?;
        con.set(key, hex::encode(checkpoint))?;
        Ok(())
    }
}
