use std::{collections::HashMap};
use anyhow::{Result, anyhow};


const JUMP_HASH_SHIFT: i32 = 33;
const JUMP_HASH_CONSTANT: u64 = 2862933555777941757;
const JUMP: f64 = (1u64 << 31) as f64;

//Left Shift
pub fn bitwise_left_shift(x: u32) -> u64{
   1_u64 << x
}

// Right Shift
pub fn bitwise_right_shift(x: u128) -> u128{
    x >> 64
}

// Bitwise XOR
pub fn bitwise_xor(high: u64, low: u64) -> u64{
    high ^ low
}

// Bitwise AND
pub fn bitwise_and_u32(base: u32, mask: u32) -> u32{
    base & mask
}

pub fn bitwise_and_u64(base: u128, mask: u128) -> u64{
    (base & mask) as u64
}


// JumpConsistentHash function, rust port from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
// Original algorithm founded in 2014 by google Lamping & Veach
pub fn jump_hash_partition(key: u64, buckets: u32) -> Result<u32> {
    if buckets == 0 {
        return Err(anyhow!("Number of buckets must be positive"));
    }

    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut mut_key = key;

    while j < buckets as i64 {
        b = j;
        mut_key = mut_key.wrapping_mul(JUMP_HASH_CONSTANT).wrapping_add(1);

        let shifted = (mut_key >> JUMP_HASH_SHIFT).wrapping_add(1);
        let exp = shifted.max(1) as f64; 

        j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i64;
    }

    Ok(b as u32)
}




// concurrency configuration
pub fn concurrecy_init() -> Result<usize> {
    let system_threads = match std::thread::available_parallelism() {
        Ok(c) => c.get(),
        Err(e) => {
            tracing::warn!("Failed to get the core count:{e}");
            return Err(anyhow!("Failed to get the systems thread count"));
        }
    };

    let numerator = system_threads * 8;
    let denominator = 10;
    let usable_system_threads = (numerator + denominator - 1) / denominator;

    Ok(usable_system_threads)
}


pub enum CollisionResult {
    Zero,
    Partial(u32),
    Complete(u32, u32),
    Error,
}

pub fn process_collisions(map: &HashMap<u32, bool>) -> Result<CollisionResult> {
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

#[derive(Eq, Hash, PartialEq, Clone)]
pub enum FilterType {
    Outer,
    Inner,
}