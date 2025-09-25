use std::{collections::HashMap, sync::RwLockWriteGuard};
use anyhow::{Result};

use crate::{internals::{ ShardData, GLOBAL_PBF}, utils::{ jump_hash_partition, process_collisions, CollisionResult, FilterType}};

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
    let mask = ARRAY_SHARDS - 1; // change this to getting to from metadata
    let hash_seed = match filter_type {
        FilterType::Outer => HASH_SEED_SELECTION[0],
        FilterType::Inner => HASH_SEED_SELECTION[1],
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



pub fn bloom_hash(
    shards: &[u32], 
    key: &str, 
    filter_type: &FilterType, 
    locked_shards: &[RwLockWriteGuard<'_, ShardData>]
) -> Result<HashMap<u32, Vec<u64>>> {
    let hash_seeds = match filter_type {
        FilterType::Outer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], 
        FilterType::Inner => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
    };

    let mut hash_list = HashMap::new();
    let key_slice: &[u8] = key.as_bytes();
    let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[0].into());
    let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[1].into());


    for (i, shard) in shards.iter().enumerate() {
        let hash_family_size = &locked_shards[i].hash_family_size;
        let bloom_length = &locked_shards[i].bloom_length;

        let mut key_hashes = Vec::with_capacity(*hash_family_size as usize);
        for idx in 0..*hash_family_size {
            let idx_u128 = idx as u128;
            let mask = (bloom_length - 1) as u128;
            // Kirsch-Mitzenmacher optimization 
            let index  = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask;

            key_hashes.push(index as u64)
        }

        hash_list.insert(*shard, key_hashes);
    }
 
    Ok(hash_list)
}

pub fn bloom_insert(
    shards_hashes: &HashMap<u32, Vec<u64>>, 
    filter_type: &FilterType, 
    locked_shards: &mut [RwLockWriteGuard<'_, ShardData>]
) -> Result<()> {

    for (i, (idx, hashes)) in shards_hashes.iter().enumerate() {
        let locked_shard = &mut locked_shards[i];

        {
            let locked_filter = &mut locked_shard.filter;
            hashes.iter().for_each(|&bloom_index| {
                locked_filter.set(bloom_index as usize, true);
            });
        }
        
        locked_shard.key_count += 1;
    }
    Ok(())
}

pub fn bloom_check(
    map: &HashMap<u32, Vec<u64>>, 
    filter_type: &FilterType,
    locked_shards: &[RwLockWriteGuard<'_, ShardData>]
) -> Result<CollisionResult> {
    let mut collision_map: HashMap<u32, bool> = HashMap::new();

    for (i, (shard, hashes)) in map.iter().enumerate() {

        let all_set = hashes.iter().all(|&bloom_index| locked_shards[i].filter[bloom_index as usize]);
        collision_map.insert(*shard, all_set);
    }

    Ok(process_collisions(&collision_map)?)
}





