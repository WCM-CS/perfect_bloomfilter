use std::{collections::{HashMap, HashSet, VecDeque}, fs, io::{self, BufRead, Cursor}, sync::{Arc, Mutex, RwLock}};
use anyhow::{Result, anyhow};
//use dashmap::DashSet;
use once_cell::sync::Lazy;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use bitvec::vec::BitVec;
use bitvec::bitvec;

use crate::{bloom::{hash::{HASH_SEED_SELECTION, INNER_BLOOM_HASH_FAMILY_SIZE, OUTER_BLOOM_HASH_FAMILY_SIZE}, init::GLOBAL_PBF, utils::{hash_remainder, power_of_two}}, metadata::meta::GLOBAL_METADATA, FilterType};



pub const BLOOM_LENGTH_PER_KEY: f64 = 19.2;
const REHASH_BATCH_SIZE: u32 = 5;

pub static GLOBAL_REHASH_QUEUE: Lazy<RehashQueue> = Lazy::new(|| RehashQueue::new());

pub struct RehashQueue {
    pub(crate) outer_queue: Arc<RwLock<VecDeque<u32>>>,
    pub(crate) inner_queue: Arc<RwLock<VecDeque<u32>>>,

    pub(crate) outer_queue_list: Arc<RwLock<HashSet<u32>>>,
    pub(crate) inner_queue_list: Arc<RwLock<HashSet<u32>>>,
}

impl Default for RehashQueue {
    fn default() -> Self {
        RehashQueue::new()
    }
}


impl RehashQueue {
    pub fn new() -> Self {
        Self {
            outer_queue: Arc::new(RwLock::new(VecDeque::new())),
            inner_queue: Arc::new(RwLock::new(VecDeque::new())),
            outer_queue_list: Arc::new(RwLock::new(HashSet::new())),
            inner_queue_list: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}


pub fn outer_insert_rehash_queue(shards: &[u32]) {
    if shards.is_empty() {
        return
    }
    
    for &idx in shards {
        {
            let locked_io_cache_list = GLOBAL_REHASH_QUEUE.outer_queue_list.read().unwrap();
            if locked_io_cache_list.contains(&idx) {
                continue; 
            }
        } 

        let mut locked_io_cache = GLOBAL_REHASH_QUEUE.outer_queue.write().unwrap();
        let mut locked_io_cache_list = GLOBAL_REHASH_QUEUE.outer_queue_list.write().unwrap();

        if !locked_io_cache_list.contains(&idx) {
            locked_io_cache.push_back(idx);
            locked_io_cache_list.insert(idx);
        }
    }
}


pub fn inner_insert_rehash_queue(shards: &[u32]) {
    if shards.is_empty() {
        return
    }

    for &idx in shards {
        {
            let locked_io_cache_list = GLOBAL_REHASH_QUEUE.inner_queue_list.read().unwrap();
            if locked_io_cache_list.contains(&idx) {
                continue; 
            }
        } 

        let mut locked_io_cache = GLOBAL_REHASH_QUEUE.inner_queue.write().unwrap();
        let mut locked_io_cache_list = GLOBAL_REHASH_QUEUE.inner_queue_list.write().unwrap();

        if !locked_io_cache_list.contains(&idx) {
            locked_io_cache.push_back(idx);
            locked_io_cache_list.insert(idx);
        }
    }
}


pub fn rehash_shards(filter_type: FilterType) -> Result<()>  {
    let (shards, filter) = match filter_type {
        FilterType::Outer => {
            let mut rehash_shards: Vec<u32> = vec![];
            let mut locked_rehash_queue = GLOBAL_REHASH_QUEUE.outer_queue.write().unwrap();

            for _ in 0..REHASH_BATCH_SIZE {
                if let Some(shard) = locked_rehash_queue.pop_front() {
                    rehash_shards.push(shard);
                }
            }

            (rehash_shards, "outer".to_string())
        },
        FilterType::Inner => {
            let mut rehash_shards: Vec<u32> = vec![];
            let mut locked_rehash_queue = GLOBAL_REHASH_QUEUE.inner_queue.write().unwrap();

            for _ in 0..REHASH_BATCH_SIZE {
                if let Some(shard) = locked_rehash_queue.pop_front() {
                    rehash_shards.push(shard);
                }
            }

            (rehash_shards, "inner".to_string())
        },
    };

    shards.par_iter().for_each(|shard| {
        let (mut locked_shard, capacity) = if &filter == "outer" {
            let locked_shard = GLOBAL_PBF.outer_filter.filters[*shard as usize].write().unwrap();
            let shard_capacity = GLOBAL_METADATA.outer_metadata.shards_metadata[*shard as usize].blooms_key_count.read().unwrap();
            
            (locked_shard, shard_capacity.clone())
        } else {
            let locked_shard = GLOBAL_PBF.inner_filter.filters[*shard as usize].write().unwrap();
            let shard_capacity = GLOBAL_METADATA.inner_metadata.shards_metadata[*shard as usize].blooms_key_count.read().unwrap();
            
            (locked_shard, shard_capacity.clone())
        };

        let (mut locked_bloom_len, mut locked_bloom_len_mult) = if &filter == "outer" {
            let locked_bloom_len = GLOBAL_METADATA.outer_metadata.shards_metadata[*shard as usize].bloom_bit_length.write().unwrap();
            let locked_bloom_len_mult = GLOBAL_METADATA.outer_metadata.shards_metadata[*shard as usize].bloom_bit_length_mult.write().unwrap();

            (locked_bloom_len, locked_bloom_len_mult)
        
        } else {
            let locked_bloom_len = GLOBAL_METADATA.inner_metadata.shards_metadata[*shard as usize].bloom_bit_length.write().unwrap();
            let locked_bloom_len_mult = GLOBAL_METADATA.inner_metadata.shards_metadata[*shard as usize].bloom_bit_length_mult.write().unwrap();
            
            (locked_bloom_len, locked_bloom_len_mult)
        };



        let file_name = format!("./data/pbf_data/{}_{}.txt", filter, shard);
        let file = fs::OpenOptions::new()
                .read(true)
                .open(&file_name).unwrap();
        let reader = io::BufReader::new(file);

        let future_bloom_length = power_of_two(*locked_bloom_len_mult + 1);
        let mut new_bloomfilter = bitvec![0; future_bloom_length as usize];

        //let key_dashmap: DashSet<String> = DashSet::with_capacity(capacity as usize);
        let key_set = Arc::new(Mutex::new(HashSet::with_capacity(capacity as usize)));
        let mut locked_key_set = key_set.lock().unwrap();

        for line in reader.lines() {
            let line_res = line.unwrap();
            locked_key_set.insert(line_res);
        }


        locked_key_set.iter().for_each(|key| {
            let hashes = bloom_rehash(&future_bloom_length, &key, &filter_type).unwrap();
            bloom_reinsert(&mut new_bloomfilter, &hashes);
        });

        *locked_shard = new_bloomfilter;
        *locked_bloom_len = future_bloom_length;
        *locked_bloom_len_mult = *locked_bloom_len_mult + 1;

        if &filter == "outer" {
            let mut locked_rehash_list = GLOBAL_REHASH_QUEUE.outer_queue_list.write().unwrap();
            locked_rehash_list.remove(shard);
        } else {
            let mut locked_rehash_list = GLOBAL_REHASH_QUEUE.inner_queue_list.write().unwrap();
            locked_rehash_list.remove(shard);
        }

    });
    

    Ok(())
}


fn bloom_rehash(bloomfilter_length: &u64, key: &str, filter_type: &FilterType) -> Result<Vec<u64>> {
    let (hash_seeds, hash_family_size) = match filter_type {
        FilterType::Outer => {
           
            ([HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], OUTER_BLOOM_HASH_FAMILY_SIZE)
        },
        FilterType::Inner => {
            
            ([HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]], INNER_BLOOM_HASH_FAMILY_SIZE)
        }
    };

    let mut key_hashes = Vec::with_capacity(hash_family_size as usize);
    let h1 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[0])?;
    let h2 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[1])?;


    for idx in 0..hash_family_size {
        let idx_u128 = idx as u128;
        let mask = bloomfilter_length - 1;
        let base = h1.wrapping_add(idx_u128.wrapping_mul(h2));
        let index = hash_remainder(base, mask as u128);

        key_hashes.push(index as u64)
    }

    Ok(key_hashes)

}

fn bloom_reinsert(filter: &mut BitVec, hashes: &Vec<u64>) {
    hashes.iter().for_each(|idx| {
        filter.set(*idx as usize, true);
    });
}



