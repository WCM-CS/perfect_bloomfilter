use std::{collections::HashMap};
use anyhow::{Result};

use crate::{internals::{ GLOBAL_PBF}, utils::{ jump_hash_partition, process_collisions, CollisionResult, FilterType}};

pub const HASH_SEED_SELECTION: [u32; 6] = [
    0x8badf00d,
    0xdeadbabe,
    0xabad1dea,
    0xdeadbeef,
    0xcafebabe,
    0xfeedface,
];

// Starting k
pub const BLOOM_HASH_FAMILY_SIZE: u32 = 7;

// Static shard count per array
pub const ARRAY_SHARDS_MULT: u32 = 12;
pub const ARRAY_SHARDS: u32 = 1u32 << ARRAY_SHARDS_MULT;

// Starting bloom lens
pub const BLOOM_STARTING_MULT: u32 = 12;
pub const BLOOM_STARTING_LENGTH: u64 = 1u64 << BLOOM_STARTING_MULT;

pub fn array_sharding_hash(key: &str, filter_type: &FilterType) -> Result<Vec<u32>> {

    let mask = ARRAY_SHARDS - 1;
    let hash_seed = match filter_type {
        FilterType::Outer => {
            HASH_SEED_SELECTION[0]
        },
        FilterType::Inner => {
           HASH_SEED_SELECTION[1]
        }
    };

    let key_slice: &[u8] = key.as_bytes();
    let hash = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seed as u64);

    let high = (hash >> 64) as u64;
    let low = hash as u64;

    let shard_size = mask / 2;
    let xor = high^low;

    let p1 = jump_hash_partition(xor, &(mask + 1))?;
    let p2 = (p1 + shard_size) & mask;

    debug_assert!(
        p1 != p2,
        "Partitions must be unique"
    );

    Ok(vec![p1, p2])
}



pub fn bloom_hash(shards: &[u32], key: &str, filter_type: &FilterType) -> Result<HashMap<u32, Vec<u64>>> {
    let mut hash_list = HashMap::new();
    
    let (bitvec_lens, hash_seeds, hash_family_size) = match filter_type {
        FilterType::Outer => {
            let mut bitvec_lens: Vec<u64> = vec![];

            for shard in shards{
                let bit_vec_len = GLOBAL_PBF.outer_filter.shard_vector[*shard as usize].bloom_length.read().unwrap();
                bitvec_lens.push(*bit_vec_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], BLOOM_HASH_FAMILY_SIZE)
        },
        FilterType::Inner => {
            let mut bitvec_lens: Vec<u64> = vec![];
            
            for shard in shards {
                let bit_vec_len = GLOBAL_PBF.inner_filter.shard_vector[*shard as usize].bloom_length.read().unwrap();
                bitvec_lens.push(*bit_vec_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]], BLOOM_HASH_FAMILY_SIZE)
        }
    };

    let key_slice: &[u8] = key.as_bytes();
    let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[0].into());
    let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[1].into());


    for (i, shard) in shards.iter().enumerate() {
        let mut key_hashes = Vec::with_capacity(hash_family_size as usize);
        for idx in 0..hash_family_size {
            let idx_u128 = idx as u128;
            let mask = (bitvec_lens.get(i).unwrap() - 1) as u128;
            // Kirsch-Mitzenmacher optimization 
            let index  = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask;

            key_hashes.push(index as u64)
        }

        hash_list.insert(*shard, key_hashes);
    }
 
    Ok(hash_list)
}

pub fn bloom_insert(shards_hashes: &HashMap<u32, Vec<u64>>, filter_type: &FilterType) -> Result<()> {
    let filter = match filter_type {
        FilterType::Outer => &GLOBAL_PBF.outer_filter.shard_vector,
        FilterType::Inner => &GLOBAL_PBF.inner_filter.shard_vector
    };

    for (idx, hashes) in shards_hashes {
        let mut locked_filter = filter[*idx as usize].filter.write().unwrap();

        hashes.iter().for_each(|&bloom_index| {
            locked_filter.set(bloom_index as usize, true);
        });

        *filter[*idx as usize].key_count.write().unwrap() += 1;
    }

    Ok(())
}

pub fn bloom_check(map: &HashMap<u32, Vec<u64>>, filter_type: &FilterType) -> Result<CollisionResult> {
    let mut collision_map: HashMap<u32, bool> = HashMap::new();

    match filter_type {
        FilterType::Outer => {
            for (&shard, hashes) in map {
                let locked_pbf = GLOBAL_PBF.outer_filter.shard_vector[shard as usize].filter.read().unwrap();
                let all_set = hashes.iter().all(|&bloom_index| locked_pbf[bloom_index as usize]);

                collision_map.insert(shard, all_set);
            }
        }
        FilterType::Inner => {
            for (&shard, hashes) in map {
                let locked_pbf = GLOBAL_PBF.inner_filter.shard_vector[shard as usize].filter.read().unwrap();
                let all_set = hashes.iter().all(|&bloom_index| locked_pbf[bloom_index as usize]);

                collision_map.insert(shard, all_set);
            }
        }
    }

    Ok(process_collisions(&collision_map)?)
}





