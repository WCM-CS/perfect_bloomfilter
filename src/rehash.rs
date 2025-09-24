

use crate::{hash::{BLOOM_HASH_FAMILY_SIZE, HASH_SEED_SELECTION}, utils::FilterType};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{anyhow, Result};

const REHASH_BITVEC_THRESHOLD: f64 = 19.2;
const REHASH_BATCH_SIZE: u16 = 5;




fn bloom_rehash(key: &str, filter_type: &FilterType, bloom_length: u64, bitvec: &mut BitVec) {
    let (hash_seeds, hash_family_size) = match filter_type {
        FilterType::Outer => {
            ([HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], BLOOM_HASH_FAMILY_SIZE)
        },
        FilterType::Inner => {
            ([HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]], BLOOM_HASH_FAMILY_SIZE)
        },
    };

    let mut key_hashes = Vec::with_capacity(hash_family_size as usize);

    let key_slice: &[u8] = key.as_bytes();
    let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[0].into());
    let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[1].into());

    for idx in 0..hash_family_size {
        let idx_u128 = idx as u128;
        let mask = (bloom_length - 1) as u128;
        // Kirsch-Mitzenmacher optimization 
        let index  = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask;

        key_hashes.push(index as u64)
    }

    for hash in key_hashes {
        bitvec.set(hash as usize, true);
    }


}


/*
pub fn metadata_computation(filter_type: &FilterType, shard_idx: &Vec<u32>) {
    let (shard_vec, rehash_queue, rehash_list, active_rehash) = match filter_type {
        FilterType::Outer => (&GLOBAL_PBF.outer_filter.shard_vector, &REHASH_QUEUE.outer_queue, &REHASH_QUEUE.outer_lookup_list, &ACTIVE_STATE.outer_rehash_state),
        FilterType::Inner =>  (&GLOBAL_PBF.inner_filter.shard_vector, &REHASH_QUEUE.inner_queue, &REHASH_QUEUE.inner_lookup_list, &ACTIVE_STATE.inner_rehash_state)
    };

    for shard in shard_idx {
        let shard_key_count = shard_vec[*shard as usize].key_count.read().unwrap();
        let shard_bloom_length = shard_vec[*shard as usize].bloom_length.read().unwrap();

        if (*shard_bloom_length as f64 / *shard_key_count as f64) < REHASH_BITVEC_THRESHOLD {
            if rehash_list.read().unwrap().contains(&shard){ // this is a linear scan and make not perfrom well overtime maye change this to a hashset
                continue
            } else if active_rehash[*shard as usize].load(std::sync::atomic::Ordering::SeqCst) {
                continue
            } else {
                rehash_list.write().unwrap().insert(*shard);
                rehash_queue.write().unwrap().push_back(*shard);
            }
        }
    }

}
 */

