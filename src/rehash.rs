

use std::{fs::File, io::{BufRead, BufReader}, sync::RwLockWriteGuard};

use crate::{hash::{bloom_rehash, BLOOM_HASH_FAMILY_SIZE, HASH_SEED_SELECTION}, internals::{lock_shards, ShardData, GLOBAL_PBF}, io::{force_drain, GLOBAL_CACHE}, utils::FilterType};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{anyhow, Result};


const REHASH_SHARD_BATCH: u32 = 5;
const REHASH_BITVEC_THRESHOLD: f64 = 19.2;



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



pub fn metadata_computation(locked_shards: &[RwLockWriteGuard<'_, ShardData>], shards_idx: &[u32], filter_type: &FilterType) {
    let (cache, cache_list) = match filter_type {
        FilterType::Outer => (GLOBAL_CACHE.get().unwrap().outer_queue.clone(), GLOBAL_CACHE.get().unwrap().outer_queue_list.clone()),
        FilterType::Inner => (GLOBAL_CACHE.get().unwrap().inner_queue.clone(), GLOBAL_CACHE.get().unwrap().inner_queue_list.clone()),
    };

    for (idx, shard) in locked_shards.iter().enumerate() {
        let bits_per_key = shard.bloom_length as f64 / shard.key_count as f64;

        if bits_per_key <= 19.2 {
            // insert into rehash cache if its not already there
            if cache_list.read().unwrap().contains(&shards_idx[idx]) {
                continue // do nothing as this shard does need to rehash but it it alreay in the queue
            } else {
                // push the shard idx to the queue
                // shard idx is the shards id vector, indexes to the locked shard in uses lockes shard idx maps to the sahrds idx vector
               cache_list.write().unwrap().insert(shards_idx[idx]);
               cache.write().unwrap().push_back(shards_idx[idx]);
                
            }




        }
    }


}
 
