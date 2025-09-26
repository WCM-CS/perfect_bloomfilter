use std::{collections::{HashMap, HashSet, VecDeque}, fs::{self, File}, io::{self, BufRead, BufReader, Write}, sync::{Arc, Mutex, RwLock, RwLockWriteGuard}};

use crate::{hash::{bloom_rehash, ARRAY_SHARDS}, internals::{lock_shards,GLOBAL_PBF}, utils::FilterType};
use anyhow::Result;
use bitvec::vec::BitVec;
use bitvec::bitvec;
use once_cell::sync::{OnceCell};


//pub const GIB: usize = 1024 * 1024 * 1024;
//pub static LMDB_ENV: OnceCell<Arc<Env>> = OnceCell::new();

pub static GLOBAL_CACHE: OnceCell<Arc<Cache>> = OnceCell::new();
const REHASH_SHARD_BATCH: u32 = 5;

#[derive(Debug)]
pub struct Cache {
    outer_cache: Vec<RwLock<Vec<String>>>,
    inner_cache: Vec<RwLock<Vec<String>>>,

    outer_queue: Arc<RwLock<VecDeque<u32>>>,
    inner_queue: Arc<RwLock<VecDeque<u32>>>,

    outer_queue_list: Arc<RwLock<HashSet<u32>>>,
    inner_queue_list: Arc<RwLock<HashSet<u32>>>,
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


fn rehash(filter_type: &FilterType, ) -> Result<()> {
    let (shards, filter, mut locked_shards) = match filter_type {
        FilterType::Outer => {
            let mut rehash_shards = Vec::new();
            let filter = "outer";

            for _ in 0..REHASH_SHARD_BATCH {
                if let Some(shard) = GLOBAL_CACHE.get().unwrap().outer_queue.write().unwrap().pop_front() {
                    rehash_shards.push(shard);
                }
            }

            let locked_shards = lock_shards(&rehash_shards, &GLOBAL_PBF.get().unwrap().outer_filter.shard_vector);
            force_drain(filter_type, &rehash_shards)?;

            (rehash_shards, filter, locked_shards)
        }
        FilterType::Inner => {
            let mut rehash_shards = Vec::new();
            let filter = "inner";
            
            for _ in 0..REHASH_SHARD_BATCH {
                if let Some(shard) = GLOBAL_CACHE.get().unwrap().outer_queue.write().unwrap().pop_front() {
                    rehash_shards.push(shard);
                }
            }

            let locked_shards = lock_shards(&rehash_shards, &GLOBAL_PBF.get().unwrap().inner_filter.shard_vector);
            force_drain(filter_type, &rehash_shards)?;

            (rehash_shards, filter, locked_shards)
        },
    };

    // make sure the locked shards caches are drained 


    
    for (locked_idx, shard) in shards.iter().enumerate() {
        let file_name = format!("./data/pbf_data/{}_{}.txt", filter, shard);
        let file = File::open(file_name)?;
        let mut reader = BufReader::new(file);

        let shard = &mut locked_shards[locked_idx];

        let bloom_len_mult = shard.bloom_length_mult;

        let new_bloom_len_mult = bloom_len_mult + 1;
        let new_bloom_len = 1u64 << new_bloom_len_mult;

        let mut new_bloomfilter = bitvec![0; new_bloom_len as usize];

        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            let key_slice = line.as_bytes();
            bloom_rehash(key_slice, filter_type, new_bloom_len, &mut new_bloomfilter); // hashes up the slice & mutates the new filter

        }

        shard.bloom_length = new_bloom_len;
        shard.bloom_length_mult = new_bloom_len_mult;
        shard.filter = new_bloomfilter;

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
