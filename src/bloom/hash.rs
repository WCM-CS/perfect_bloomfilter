use std::{collections::HashMap, io::Cursor};
use anyhow::{Result, anyhow};

use crate::{bloom::{self, init::GLOBAL_PBF, inner_filters::INNER_ARRAY_SHARDS, outer_filters::OUTER_ARRAY_SHARDS, utils::{compare_high_low, hash_remainder, jump_hash_partition, partition_remainder, shift_right_bits}}, metadata::meta::GLOBAL_METADATA, CollisionResult, FilterType};

pub const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;

pub const HASH_SEED_SELECTION: [u32; 6] = [
    0x8badf00d,
    0xdeadbabe,
    0xabad1dea,
    0xdeadbeef,
    0xcafebabe,
    0xfeedface,
];


pub fn array_sharding_hash(key: &str, filter_type: FilterType) -> Result<Vec<u32>> {
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

// Kirsch-Mitzenmacher optimization 
pub fn bloom_hash(shards: &[u32], key: &str, filter_type: FilterType) -> Result<HashMap<u32, Vec<u64>>> {
    let mut hash_list = HashMap::new();
    
    // get the shards bit lengths 
    let (bitvec_lens, hash_seeds, hash_family_size) = match filter_type {
        FilterType::Outer => {
            let mut bitvec_lens: Vec<u64> = vec![];

            for shard in shards {
                let bit_len = GLOBAL_METADATA.outer_metadata.shards_metadata[*shard as usize].bloom_bit_length.read().unwrap();
                bitvec_lens.push(*bit_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], OUTER_BLOOM_HASH_FAMILY_SIZE)
        },
        FilterType::Inner => {
            let mut bitvec_lens: Vec<u64> = vec![];
            
            for shard in shards {
                let bit_len = GLOBAL_METADATA.inner_metadata.shards_metadata[*shard as usize].bloom_bit_length.read().unwrap();
                bitvec_lens.push(*bit_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]], INNER_BLOOM_HASH_FAMILY_SIZE)
        }
    };

    for (i, shard) in shards.iter().enumerate() {


        let mut key_hashes = Vec::with_capacity(hash_family_size as usize);
        let h1 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[0])?;
        let h2 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[1])?;

        for idx in 0..hash_family_size {
            let idx_u128 = idx as u128;
            let mask = bitvec_lens.get(i).unwrap() - 1;
            let base  = h1.wrapping_add(idx_u128.wrapping_mul(h2));
            let index = hash_remainder(base, mask as u128);

            key_hashes.push(index as u64)
        }

        hash_list.insert(*shard, key_hashes);
 
    }

    Ok(hash_list)

}

pub fn bloom_insert(shards_hashes: &HashMap<u32, Vec<u64>>, filter_type: FilterType) {
    for (idx, hashes) in shards_hashes {
        match filter_type {
            FilterType::Outer => {
                let mut locked_filter = GLOBAL_PBF.outer_filter.filters[*idx as usize].write().unwrap();
                let mut locked_shard_key_count = GLOBAL_METADATA.outer_metadata.shards_metadata[*idx as usize].blooms_key_count.write().unwrap();
                
                hashes.iter().for_each(|&bloom_index| {
                    locked_filter.set(bloom_index as usize, true);
                    *locked_shard_key_count += 1;
                    
                });
            },
            FilterType::Inner => {
                let mut locked_filter = GLOBAL_PBF.inner_filter.filters[*idx as usize].write().unwrap();
                let mut locked_shard_key_count = GLOBAL_METADATA.inner_metadata.shards_metadata[*idx as usize].blooms_key_count.write().unwrap();
                
                hashes.iter().for_each(|&bloom_index| {
                    locked_filter.set(bloom_index as usize, true);
                    *locked_shard_key_count += 1;
                });
            }
        }
    }
}

pub fn bloom_check(map: &HashMap<u32, Vec<u64>>, filter_type: FilterType) -> Result<CollisionResult> {
    let mut collision_map: HashMap<u32, bool> = HashMap::new();

    match filter_type {
        FilterType::Outer => {
            for (&shard, hashes) in map {
                let locked_pbf = GLOBAL_PBF.outer_filter.filters[shard as usize].read().unwrap();
                let all_set = hashes.iter().all(|&bloom_index| locked_pbf[bloom_index as usize]);

                collision_map.insert(shard, all_set);
            }
        }
        FilterType::Inner => {
            for (&shard, hashes) in map {
                let locked_pbf = GLOBAL_PBF.inner_filter.filters[shard as usize].read().unwrap();
                let all_set = hashes.iter().all(|&bloom_index| locked_pbf[bloom_index as usize]);

                collision_map.insert(shard, all_set);
            }
        }
    }

    Ok(process_collisions(&collision_map)?)

}


fn process_collisions(map: &HashMap<u32, bool>) -> Result<CollisionResult> {
    let mut collided: Vec<u32> = vec![];

    map.iter().for_each(|(idx, res)| {
        if *res {
            collided.push(*idx);
        }
    });

    let collision_result = match collided.as_slice() {
        [] => CollisionResult::Zero,
        [one] => CollisionResult::Partial(*one),
        [one, two] => CollisionResult::Complete(*one, *two),
        _ => CollisionResult::Error,
    };

    Ok(collision_result)
}