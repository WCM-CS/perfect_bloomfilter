use std::{collections::HashMap, fs, io::{self, Write}, sync::{Arc, RwLock}};
use anyhow::{Result, anyhow};
use once_cell::sync::Lazy;
use crate::FilterType;

pub static GLOBAL_IO_CACHE: Lazy<IOCache> = Lazy::new(|| IOCache::new());

// going from one to two caches reduces time from 236 to 182 seconds 
// Potentially shard this to a further degree for better performance optimization by reducing lock contention further
pub struct IOCache {
    pub(crate) outer_cache: Arc<RwLock<HashMap<u32, Vec<String>>>>,
    pub(crate) inner_cache: Arc<RwLock<HashMap<u32, Vec<String>>>>,
}

impl Default for IOCache {
    fn default() -> Self {
        IOCache::new()
    }
}

impl IOCache { 
    fn new() -> Self {
        Self {
            outer_cache: Arc::new(RwLock::new(HashMap::new())),
            inner_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub fn outer_insert_disk_io_cache(key: &str, shards: &[u32]) {
    let mut locked_io_cache = GLOBAL_IO_CACHE.outer_cache.write().unwrap();

    for idx in shards {
        locked_io_cache.entry(*idx).or_default().push(key.to_string())
    }
}

pub fn inner_insert_disk_io_cache(key: &str, shards: &[u32]) {
    let mut locked_io_cache = GLOBAL_IO_CACHE.inner_cache.write().unwrap();
 
    for idx in shards {
        locked_io_cache.entry(*idx).or_default().push(key.to_string())
    }
}


pub fn write_disk_io_cache(filter_map: HashMap<u32, Vec<String>>, filter_type: FilterType) -> Result<()> {
    let node_type = match filter_type {
        FilterType::Outer => "outer".to_string(),
        FilterType::Inner => "inner".to_string()
    };

    for (shard, keys) in filter_map {
        if let Some(parent) = std::path::Path::new("./data/pbf_data/init.txt").parent() {
            fs::create_dir_all(parent)?; // Create directory if it does not exist
        }
        let file_name = format!("./data/pbf_data/{}_{}.txt", node_type, shard);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_name)?;

        let mut writer = io::BufWriter::new(file);

        for k in keys {
            writer.write_all(k.as_bytes())?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;

    }

    Ok(())
}

