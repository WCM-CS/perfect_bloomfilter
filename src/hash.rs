use std::{collections::HashMap, io::Cursor, sync::{atomic::{AtomicU32, AtomicU64, Ordering}, Arc, RwLock}};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::Lazy;

use crate::{internals::GLOBAL_PBF, utils::{compare_high_low, hash_remainder, jump_hash_partition, partition_remainder, process_collisions, shift_right_bits, CollisionResult, FilterType}};


pub const HASH_SEED_SELECTION: [u32; 6] = [
    0x8badf00d,
    0xdeadbabe,
    0xabad1dea,
    0xdeadbeef,
    0xcafebabe,
    0xfeedface,
];

pub const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;

pub const OUTER_ARRAY_SHARDS: u32 = 4096;
pub const INNER_ARRAY_SHARDS: u32 = 8192;

pub const OUTER_BLOOM_STARTING_MULT: u32 = 13;
pub const OUTER_BLOOM_STARTING_LENGTH: u64 = 1u64 << OUTER_BLOOM_STARTING_MULT;

pub const INNER_BLOOM_STARTING_MULT: u32 = 12;
pub const INNER_BLOOM_STARTING_LENGTH: u64 = 1u64 << INNER_BLOOM_STARTING_MULT;


pub fn array_sharding_hash(key: &str, filter_type: &FilterType) -> Result<Vec<u32>> {
    let (mask, hash_seed) = match filter_type {
        FilterType::Outer => {
            (OUTER_ARRAY_SHARDS - 1, HASH_SEED_SELECTION[0])
        },
        FilterType::Inner => {
            (INNER_ARRAY_SHARDS - 1, HASH_SEED_SELECTION[1])
        }
    };

    let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seed)?;

    let high = shift_right_bits(h1) as u64;
    let low = h1 as u64;
    let shard_size = mask / 2;

    let p1 = jump_hash_partition(compare_high_low(high, low), &(mask + 1))?;
    let p2 = partition_remainder(p1 + shard_size, mask);

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

            for shard in shards {
                let bit_vec_len = GLOBAL_PBF.outer_filter[*shard as usize].bloom_length.read().unwrap();
                bitvec_lens.push(*bit_vec_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], OUTER_BLOOM_HASH_FAMILY_SIZE)
        },
        FilterType::Inner => {
            let mut bitvec_lens: Vec<u64> = vec![];
            
            for shard in shards {
                let bit_vec_len = GLOBAL_PBF.inner_filter[*shard as usize].bloom_length.read().unwrap();
                bitvec_lens.push(*bit_vec_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]], INNER_BLOOM_HASH_FAMILY_SIZE)
        }
    };

    
    let h1 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[0])?;
    let h2 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[1])?;

    for (i, shard) in shards.iter().enumerate() {
        let mut key_hashes = Vec::with_capacity(hash_family_size as usize);
        for idx in 0..hash_family_size {
            let idx_u128 = idx as u128;
            let mask = bitvec_lens.get(i).unwrap() - 1;
            // Kirsch-Mitzenmacher optimization 
            let base  = h1.wrapping_add(idx_u128.wrapping_mul(h2));
            let index = hash_remainder(base, mask as u128);

            key_hashes.push(index as u64)
        }

        hash_list.insert(*shard, key_hashes);
    }

    Ok(hash_list)
}

pub fn bloom_insert(shards_hashes: &HashMap<u32, Vec<u64>>, filter_type: &FilterType) {
    for (idx, hashes) in shards_hashes {
        match filter_type {
            FilterType::Outer => {
                let mut locked_filter = GLOBAL_PBF.outer_filter[*idx as usize].filter.write().unwrap();
                
                hashes.iter().for_each(|&bloom_index| {
                    locked_filter.set(bloom_index as usize, true);
                });

                *GLOBAL_PBF.outer_filter[*idx as usize].key_count.write().unwrap() += 1;
            },
            FilterType::Inner => {
                let mut locked_filter = GLOBAL_PBF.inner_filter[*idx as usize].filter.write().unwrap();
                
                hashes.iter().for_each(|&bloom_index| {
                    locked_filter.set(bloom_index as usize, true);
                });

                *GLOBAL_PBF.inner_filter[*idx as usize].key_count.write().unwrap() += 1;
            }
        }
    }
}

pub fn bloom_check(map: &HashMap<u32, Vec<u64>>, filter_type: &FilterType) -> Result<CollisionResult> {
    let mut collision_map: HashMap<u32, bool> = HashMap::new();

    match filter_type {
        FilterType::Outer => {
            for (&shard, hashes) in map {
                let locked_pbf = GLOBAL_PBF.outer_filter[shard as usize].filter.read().unwrap();
                let all_set = hashes.iter().all(|&bloom_index| locked_pbf[bloom_index as usize]);

                collision_map.insert(shard, all_set);
            }
        }
        FilterType::Inner => {
            for (&shard, hashes) in map {
                let locked_pbf = GLOBAL_PBF.inner_filter[shard as usize].filter.read().unwrap();
                let all_set = hashes.iter().all(|&bloom_index| locked_pbf[bloom_index as usize]);

                collision_map.insert(shard, all_set);
            }
        }
    }

    Ok(process_collisions(&collision_map)?)
}


