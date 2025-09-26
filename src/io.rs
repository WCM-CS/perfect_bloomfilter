use std::{collections::{HashMap, HashSet, VecDeque}, fs::{self, File}, io::{self, BufRead, BufReader, Write}, sync::{Arc, Mutex, RwLock, RwLockWriteGuard}};

use crate::{hash::{bloom_rehash, ARRAY_SHARDS}, internals::{lock_shards, ShardData, GLOBAL_PBF}, utils::FilterType};
use anyhow::Result;
use bitvec::vec::BitVec;
use bitvec::bitvec;
use once_cell::sync::{OnceCell};


//pub const GIB: usize = 1024 * 1024 * 1024;
//pub static LMDB_ENV: OnceCell<Arc<Env>> = OnceCell::new();



pub static GLOBAL_CACHE: OnceCell<Arc<Cache>> = OnceCell::new();

const REHASH_SHARD_BATCH: u32 = 5;
const REHASH_BITVEC_THRESHOLD: f64 = 19.2;

#[derive(Debug)]
pub struct Cache {
    pub(crate) outer_cache: Vec<RwLock<Vec<String>>>,
    pub(crate) inner_cache: Vec<RwLock<Vec<String>>>,

    pub(crate) outer_queue: Arc<RwLock<VecDeque<u32>>>,
    pub(crate) inner_queue: Arc<RwLock<VecDeque<u32>>>,

    pub(crate) outer_queue_list: Arc<RwLock<HashSet<u32>>>,
    pub(crate) inner_queue_list: Arc<RwLock<HashSet<u32>>>,
}

impl Default for Cache {
    fn default() -> Self {
        let outer_cache = (0..ARRAY_SHARDS)
            .map(|_| RwLock::new(Vec::new()))
            .collect();

        let inner_cache = (0..ARRAY_SHARDS)
            .map(|_| RwLock::new(Vec::new()))
            .collect();

        let outer_queue = Arc::new(RwLock::new(VecDeque::new()));
        let inner_queue = Arc::new(RwLock::new(VecDeque::new()));

        let outer_queue_list = Arc::new(RwLock::new(HashSet::new()));
        let inner_queue_list = Arc::new(RwLock::new(HashSet::new()));


        Cache {
            outer_cache,
            inner_cache,
            outer_queue,
            inner_queue,
            outer_queue_list,
            inner_queue_list
        }
    }
}

pub fn initialize_global_cache() {
    let cache = Cache::default();
    GLOBAL_CACHE.set(Arc::new(cache)).unwrap();
}


pub fn cache_insert(key: &str, shards: &[u32], filter_type: &FilterType) -> Result<()>{
    let cache = match filter_type {
        FilterType::Outer => &GLOBAL_CACHE.get().unwrap().outer_cache,
        FilterType::Inner => &GLOBAL_CACHE.get().unwrap().inner_cache,
    };

    shards.iter().for_each(|shard| {
        cache[*shard as usize].write().unwrap().push(key.to_string());
    });

    Ok(())
}

pub fn drain_cache(filter_type: &FilterType) -> Result<()> {
    let shard_vec = match filter_type {
        FilterType::Outer => &GLOBAL_CACHE.get().unwrap().outer_cache,
        FilterType::Inner => &GLOBAL_CACHE.get().unwrap().inner_cache,
    };

    let mut drain_map: HashMap<usize, Vec<String>> = HashMap::new();

    shard_vec.iter().enumerate().for_each(|(shard_idx, shard_cache)| {
        if let Ok(mut locked_shard_cache) = shard_cache.try_write() {
            let data = std::mem::take(&mut *locked_shard_cache);
            drain_map.insert(shard_idx, data);
        }
    });

    write_disk_io(drain_map, filter_type)?;
    
    Ok(())
}

pub fn force_drain(filter_type: &FilterType, shards: &[u32]) -> Result<()> {
    let cache = match filter_type {
        FilterType::Outer => &GLOBAL_CACHE.get().unwrap().outer_cache,
        FilterType::Inner => &GLOBAL_CACHE.get().unwrap().inner_cache,
    };
    let mut drain_map: HashMap<usize, Vec<String>> = HashMap::new();

    for &shard_idx in shards {
        let data = &cache[shard_idx as usize];

        if data.read().unwrap().is_empty(){
            continue
        } else {
            let data = std::mem::take(&mut *data.write().unwrap());
            drain_map.insert(shard_idx as usize, data);
        }
    }

    write_disk_io(drain_map, filter_type)?;

    Ok(())
}



fn write_disk_io(drain_map: HashMap<usize, Vec<String>>, filter_type: &FilterType) -> Result<()>{
     let node_type = match filter_type {
        FilterType::Outer => "outer".to_string(),
        FilterType::Inner => "inner".to_string()
    };

    if let Some(parent) = std::path::Path::new("./data/pbf_data/init.txt").parent() {
        fs::create_dir_all(parent)?; // Create directory if it does not exist
    }

    for (shard, keys) in drain_map{
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






/*
pub fn lmdb_init(lmdb_map_size: usize) -> Result<()> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(lmdb_map_size)
            .max_dbs(2)
            //.max_readers(10)
            .open("./data/")?
    };

    let mut wtxn = env.write_txn()?;
    let _db1: Database<&[u8], &[u8]> = env.create_database(&mut wtxn, Some("outer"))?;
    let _db2: Database<&[u8], &[u8]> = env.create_database(&mut wtxn, Some("inner"))?;
    wtxn.commit()?;


    LMDB_ENV.set(Arc::new(env)).unwrap();

    Ok(())
}



pub fn lmdb_insert(
    key: &str,
    shards: &[u32],
    filter_type: &FilterType,
    locked_shards: &[RwLockWriteGuard<'_, ShardData>]
) -> Result<()> {
    let db_name = match filter_type {
        FilterType::Outer => "outer",
        FilterType::Inner => "inner",
    };

    let lmdb_key_vec: Vec<String> = shards.iter().enumerate().map(|(idx, shard)| {
        let key_idx = &locked_shards[idx].key_count;
        format!("{}:{}", shard, key_idx)
    }).collect();


    let client = LMDB_ENV.get().unwrap().clone();

    let mut wtxn = client.write_txn()?;
    let db: Database<Str, Bytes> = client.create_database(&mut wtxn, Some(db_name))?;

    for lmdb_key in lmdb_key_vec {
        let value_bytes = key.as_bytes();
        db.put(&mut wtxn, &lmdb_key, value_bytes)?;
    }

    wtxn.commit()?;


    Ok(())
}




*/
