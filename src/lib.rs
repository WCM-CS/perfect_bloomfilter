use anyhow::{Result, anyhow};
use bitvec::{bitvec, vec::BitVec};
use std::{
    collections::HashMap,
    fs,
    io::{self, Cursor, Write},
    sync::{Arc, Mutex, RwLock},
    thread::{self},
    time::Duration,
};
use threadpool::ThreadPool;

pub const HASH_SEED_SELECTION: [u32; 6] = [9, 223, 372, 530, 775, 954];

const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;

pub const STATIC_VECTOR_LENGTH_OUTER: usize = 4096;
pub const STATIC_VECTOR_LENGTH_INNER: usize = 8192;

// 14, 12 = 400_000 keys, 16 MB mem
// 16, 14 = 2_454_096 keys, 53.2 MB mem   // this could be a solid starting point now using 6M keys per 53.1MB
// 18, 16 = 12_620_937, 204.3 MB
// 20, 18 = 44_587_519, 825 MB

// 14: 28.2 MB, 3_611_198 Keys before fail
// 15: 53.3 MB 200_000 15

static OUTER_BLOOM_DEFAULT_LENGTH: usize = (2_u32.pow(15)) as usize;
static INNER_BLOOM_DEFAULT_LENGTH: usize = (2_u32.pow(15)) as usize;

pub enum FilterType {
    Outer,
    Inner,
}

struct PerfectBloomFilter {
    outer_filter: Arc<OuterBlooms>,
    inner_filter: Arc<InnerBlooms>,
}

impl PerfectBloomFilter {
    pub fn new() -> Result<Self> {
        let outer_filter = Arc::new(OuterBlooms::new());
        let inner_filter = Arc::new(InnerBlooms::new());

        let drain_outer_filter = Arc::clone(&outer_filter);
        let drain_inner_filter = Arc::clone(&inner_filter);
        let resize_outer_filter = Arc::clone(&outer_filter);
        let resize_inner_filter = Arc::clone(&inner_filter);




        let drain_pool = ThreadPool::new(2);

        drain_pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(5));
                let mut locked_outer_filter = drain_outer_filter.outer_disk_cache.lock().unwrap();
                let data = std::mem::take(&mut *locked_outer_filter);

                drop(locked_outer_filter);

                let _ = write_disk_io_cache(data, FilterType::Outer);
            }
        });

        drain_pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(5));
                let mut locked_inner_filter = drain_inner_filter.inner_disk_cache.lock().unwrap();
                let data = std::mem::take(&mut *locked_inner_filter);

                drop(locked_inner_filter);

                let _ = write_disk_io_cache(data, FilterType::Inner);
            }
        });



        

        Ok(Self {
            outer_filter,
            inner_filter,
        })
    }


    pub fn contains_insert(&mut self, key: &str) -> Result<bool> {
        // sync method:: 2M keys: 180.10 seconds
        let outer_res = self.outer_filter.contains_and_insert(key)?;
        let inner_res = self.inner_filter.contains_and_insert(key)?;

        Ok(outer_res && inner_res)
    }

    pub async fn async_contains_insert(&mut self, key: &str) -> Result<bool> {
        // sync method:: 2M keys: 180.10 seconds
        let outer_res = self.outer_filter.contains_and_insert(key)?;
        let inner_res = self.inner_filter.contains_and_insert(key)?;

        Ok(outer_res && inner_res)
    }
}

pub struct OuterBlooms {
    outer_shards: Arc<RwLock<OuterShardArray>>,
    outer_metadata: Arc<RwLock<OuterMetaData>>,
    outer_disk_cache: Arc<Mutex<HashMap<u32, Vec<String>>>>,
}

impl OuterBlooms {
    pub fn new() -> Self {
        let outer_shards = Arc::new(RwLock::new(OuterShardArray::default()));
        let outer_metadata = Arc::new(RwLock::new(OuterMetaData::default()));
        let outer_disk_cache = Arc::new(Mutex::new(HashMap::new()));

        Self {
            outer_shards,
            outer_metadata,
            outer_disk_cache,
        }
    }
}

pub struct InnerBlooms {
    inner_shards: Arc<RwLock<InnerShardArray>>,
    inner_metadata: Arc<RwLock<InnerMetaData>>,
    inner_disk_cache: Arc<Mutex<HashMap<u32, Vec<String>>>>,
}

impl InnerBlooms {
    pub fn new() -> Self {
        let inner_shards = Arc::new(RwLock::new(InnerShardArray::default()));
        let inner_metadata = Arc::new(RwLock::new(InnerMetaData::default()));
        let inner_disk_cache = Arc::new(Mutex::new(HashMap::new()));

        Self {
            inner_shards,
            inner_metadata,
            inner_disk_cache,
        }
    }
}

pub trait BloomFilter {
    type CollisionResult;

    fn contains_and_insert(&self, key: &str) -> Result<bool>;
    fn bloom_hash(&self, vector_partitions: &Vec<u32>, key: &str)
    -> Result<HashMap<u32, Vec<u64>>>;
    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<Self::CollisionResult>;
    fn bloom_insert(&self, inner_hashed: &HashMap<u32, Vec<u64>>);
    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>>;
    fn drain_cache(&mut self) -> Result<HashMap<u32, Vec<String>>>;
    fn insert_disk_io_cache(&self, key: &str, shards_idx: Vec<u32>) -> Result<()>;
}

impl BloomFilter for OuterBlooms {
    type CollisionResult = OuterCollisionResult;

    fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let outer_shard_slots = self.array_partition_hash(key)?; // get the outer blooms three shard idx
        let outer_blooms_idx = self.bloom_hash(&outer_shard_slots, key)?;
        let outer_collision_result = self.bloom_check(&outer_blooms_idx)?;

        let outer_exists = match outer_collision_result {
            OuterCollisionResult::Zero => {
                self.bloom_insert(&outer_blooms_idx);
                false
            }
            OuterCollisionResult::Partial(_) => {
                self.bloom_insert(&outer_blooms_idx);
                false
            }
            OuterCollisionResult::Complete(s1, s2) => {
                tracing::info!(
                    "OUTER BLOOM: Complete collision for the bloom filter at shards: {:?} & {:?} Value: {}",
                    s1,
                    s2,
                    key
                );
                true
            }
            OuterCollisionResult::Error => {
                panic!("unexpected issue with collision result for key: {key}");
            }
        };

        self.insert_disk_io_cache(key, outer_shard_slots)?;

        Ok(outer_exists)
    }

    fn bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let inner = self.outer_metadata.read().unwrap();

        for &vector_slot in vector_partitions {
            let bloom_length = match inner.bloom_bit_length.get(&vector_slot) {
                // GET THE BLOOMIES LENGTH PER THE SHARD IDX
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for bloommmie {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[0])?;
            let h2 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[1])?;

            for idx in 0..OUTER_BLOOM_HASH_FAMILY_SIZE {
                let idx_u128 = idx as u128;
                let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) % bloom_length as u128;

                key_hashes.push(index as u64)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<OuterCollisionResult> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        let inner = self.outer_shards.read().unwrap();

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| inner.data[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        let collision_result = outer_check_collision(&collision_map)?;

        Ok(collision_result)
    }

    fn bloom_insert(&self, outer_hashed: &HashMap<u32, Vec<u64>>) {
        let mut inner = self.outer_shards.write().unwrap();
        let mut inner_met = self.outer_metadata.write().unwrap();

        for (vector_index, hashes) in outer_hashed {
            hashes.iter().for_each(|&bloom_index| {
                inner.data[*vector_index as usize].set(bloom_index as usize, true)
            });

            inner_met
                .blooms_key_count
                .entry(*vector_index)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        // two shards: jump hash p1 and use n/2 mod shard len
        // three shards: jump hash p1, use p1+n/3 mod n and p1+2n/3 mod n

        let shard_len: u32 = STATIC_VECTOR_LENGTH_OUTER as u32;

        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[2])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;
        let shard_size = shard_len / 2;

        let p1 = jump_hash_partition(low ^ high, shard_len as u32)?;
        let p2 = (p1 + shard_size) % shard_len as u32;

        debug_assert!(p1 != p2, "Partitions must be unique");

        Ok(vec![p1, p2])
    }

    fn drain_cache(&mut self) -> Result<HashMap<u32, Vec<String>>> {
        let mut locked_cache = self.outer_disk_cache.lock().unwrap();
        let data = std::mem::take(&mut *locked_cache); // deref the mutex guaard to take ownership and drain cache
        Ok(data)
    }

    fn insert_disk_io_cache(&self, key: &str, shards_idx: Vec<u32>) -> Result<()> {
        let mut inner = self.outer_disk_cache.lock().unwrap();

        for idx in shards_idx {
            inner.entry(idx).or_default().push(key.to_string());
        }

        Ok(())
    }
}

impl BloomFilter for InnerBlooms {
    type CollisionResult = InnerCollisionResult;

    fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let inner_shard_slots = self.array_partition_hash(key)?; // get the outer blooms three shard idx
        let inner_blooms_idx = self.bloom_hash(&inner_shard_slots, key)?;
        let inner_collision_result = self.bloom_check(&inner_blooms_idx)?;

        let inner_exists = match inner_collision_result {
            InnerCollisionResult::Zero => {
                self.bloom_insert(&inner_blooms_idx);
                false
            }
            InnerCollisionResult::PartialMinor(_) => {
                self.bloom_insert(&inner_blooms_idx);
                false
            }
            InnerCollisionResult::PartialMajor(s1, s2) => {
                tracing::info!(
                    "INNER BLOOM: Major collision for the bloom filter at shards: {:?} & {:?}, Value: {}",
                    s1,
                    s2,
                    key
                );
                self.bloom_insert(&inner_blooms_idx);
                false
            }
            InnerCollisionResult::Complete(s1, s2, s3) => {
                tracing::info!(
                    "INNER BLOOM: Complete collision for the bloom filter at shards: {:?} & {:?} & {:?}, Value: {}",
                    s1,
                    s2,
                    s3,
                    key
                );
                true
            }
            InnerCollisionResult::Error => {
                panic!("unexpected issue with collision result for key: {key}");
            }
        };

        self.insert_disk_io_cache(key, inner_shard_slots)?;

        Ok(inner_exists)
    }

    fn bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let inner = self.inner_metadata.read().unwrap();

        for &vector_slot in vector_partitions {
            let bloom_length = match inner.bloom_bit_length.get(&vector_slot) {
                // GET THE BLOOMIES LENGTH PER THE SHARD IDX
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for bloommmie {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[3])?;
            let h2 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[4])?;

            for idx in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let idx_u128 = idx as u128; // potentially add a prime number here to ensure its a solid dirtibution in the shards
                let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) % bloom_length as u128;

                key_hashes.push(index as u64)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<InnerCollisionResult> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        let inner = self.inner_shards.read().unwrap();

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| inner.data[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        let collision_result = inner_check_collision(&collision_map)?;

        Ok(collision_result)
    }

    fn bloom_insert(&self, inner_hashed: &HashMap<u32, Vec<u64>>) {
        let mut inner = self.inner_shards.write().unwrap();
        let mut inner_met = self.inner_metadata.write().unwrap();

        for (vector_index, hashes) in inner_hashed {
            hashes.iter().for_each(|&bloom_index| {
                inner.data[*vector_index as usize].set(bloom_index as usize, true)
            });

            inner_met
                .blooms_key_count
                .entry(*vector_index)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        // two shards: jump hash p1 and use n/2 mod shard len
        // three shards: jump hash p1, use p1+n/3 mod n and p1+2n/3 mod n

        let shard_len: u32 = STATIC_VECTOR_LENGTH_INNER as u32;

        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[5])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;
        let shard_size = shard_len / 3;

        let p1 = jump_hash_partition(high ^ low, shard_len as u32)?;
        let p2 = (p1 + shard_size) % shard_len as u32;
        let p3 = (p1 + (2 * shard_size)) % shard_len as u32;

        debug_assert!(
            p1 != p2 && p1 != p3 && p2 != p3,
            "Partitions must be unique"
        );

        Ok(vec![p1, p2, p3])
    }

    fn drain_cache(&mut self) -> Result<HashMap<u32, Vec<String>>> {
        let mut locked_cache = self.inner_disk_cache.lock().unwrap();
        let data = std::mem::take(&mut *locked_cache); // deref the mutex guaard to take ownership and drain cache
        Ok(data)
    }

    fn insert_disk_io_cache(&self, key: &str, shards_idx: Vec<u32>) -> Result<()> {
        let mut inner = self.inner_disk_cache.lock().unwrap();

        for idx in shards_idx {
            inner.entry(idx).or_default().push(key.to_string());
        }

        Ok(())
    }
}

#[derive(Debug)]
struct OuterShardArray {
    data: [BitVec; STATIC_VECTOR_LENGTH_OUTER],
}

impl Default for OuterShardArray {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; OUTER_BLOOM_DEFAULT_LENGTH];
        let data = std::array::from_fn(|_| empty_bitvec.clone());
        OuterShardArray { data }
    }
}

#[derive(Debug)]
struct InnerShardArray {
    data: [BitVec; STATIC_VECTOR_LENGTH_INNER],
}

impl Default for InnerShardArray {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; INNER_BLOOM_DEFAULT_LENGTH];
        let data = std::array::from_fn(|_| empty_bitvec.clone());
        InnerShardArray { data }
    }
}

#[derive(Debug)]
struct OuterMetaData {
    blooms_key_count: HashMap<u32, u64>,
    bloom_bit_length: HashMap<u32, u64>,
}

impl Default for OuterMetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl OuterMetaData {
    pub fn new() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_OUTER);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_OUTER);

        for partition in 0..STATIC_VECTOR_LENGTH_OUTER as u32 {
            key_count.insert(partition, 0);
            bit_length.insert(partition, OUTER_BLOOM_DEFAULT_LENGTH as u64); // the default outer bloomies len should be larger than the inner since the shard arr len is smaller, reduces cpu load via less rehashes on init
        }

        Self {
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
        }
    }
}

#[derive(Debug)]
struct InnerMetaData {
    blooms_key_count: HashMap<u32, u64>,
    bloom_bit_length: HashMap<u32, u64>,
}
impl Default for InnerMetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerMetaData {
    pub fn new() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_INNER);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_INNER);

        for partition in 0..STATIC_VECTOR_LENGTH_INNER as u32 {
            key_count.insert(partition, 0);
            bit_length.insert(partition, INNER_BLOOM_DEFAULT_LENGTH as u64); // the default outer bloomies len should be larger than the inner since the shard arr len is smaller, reduces cpu load via less rehashes on init
        }

        Self {
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
        }
    }
}

// JumpConsistentHash function, rewritten from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
fn jump_hash_partition(key: u64, buckets: u32) -> Result<u32> {
    let mut b: i128 = -1;
    let mut j: i128 = 0;

    const JUMP: f64 = (1u64 << 31) as f64;
    const CONSTANT: u64 = 2862933555777941757; // Values based on the original java implementation

    let mut mut_key = key;

    while j < buckets as i128 {
        b = j;
        mut_key = mut_key.wrapping_mul(CONSTANT).wrapping_add(1);

        let exp = ((mut_key >> 33) + 1).max(1) as f64;
        j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i128;
    }

    Ok(b as u32)
}

pub enum OuterCollisionResult {
    Zero,
    Partial(u32),
    Complete(u32, u32),
    Error,
}

pub enum InnerCollisionResult {
    Zero,
    PartialMinor(u32),
    PartialMajor(u32, u32),
    Complete(u32, u32, u32),
    Error,
}

pub fn outer_check_collision(map: &HashMap<u32, bool>) -> Result<OuterCollisionResult> {
    let mut collided = vec![];

    map.iter().for_each(|(vector_indx, boolean)| {
        if *boolean {
            collided.push(vector_indx)
        }
    });

    let collision_result = match collided.as_slice() {
        [] => OuterCollisionResult::Zero,
        [one] => OuterCollisionResult::Partial(**one),
        [one, two] => OuterCollisionResult::Complete(**one, **two),
        _ => OuterCollisionResult::Error,
    };

    Ok(collision_result)
}

pub fn inner_check_collision(map: &HashMap<u32, bool>) -> Result<InnerCollisionResult> {
    let mut collided = vec![];

    map.iter().for_each(|(vector_indx, boolean)| {
        if *boolean {
            collided.push(vector_indx)
        }
    });

    let collision_result = match collided.as_slice() {
        [] => InnerCollisionResult::Zero,
        [one] => InnerCollisionResult::PartialMinor(**one),
        [one, two] => InnerCollisionResult::PartialMajor(**one, **two),
        [one, two, three] => InnerCollisionResult::Complete(**one, **two, **three),
        _ => InnerCollisionResult::Error,
    };

    Ok(collision_result)
}

fn write_disk_io_cache(data: HashMap<u32, Vec<String>>, cache_type: FilterType) -> Result<()> {
    let node = match cache_type {
        FilterType::Outer => "outer",
        FilterType::Inner => "inner",
    };

    for (shard, keys) in data.into_iter() {
        if let Some(parent) = std::path::Path::new("./pbf_data/init.txt").parent() {
            fs::create_dir_all(parent)?; // Create directory if it does not exist
        }
        let file_name = format!("./pbf_data/{}_{}.txt", node, shard);
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;

    use once_cell::sync::Lazy;

    use crate::PerfectBloomFilter;

    /* static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });*/

    #[test]
    fn test_filter_loop_sync() -> anyhow::Result<()> {
        //Lazy::force(&TRACING);
        let count = 2_000_000;
        let mut pf = PerfectBloomFilter::new()?;

        tracing::info!("Starting Insert phase 1");
        for i in 0..count {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;

            assert_eq!(was_present, false);
        }
        tracing::info!("Completed Insert phase 1");
        //let mut locked_pf = pf.lock().await;

        tracing::info!("Starting confirmation phase 1");
        for i in 0..count {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_loop_async() -> anyhow::Result<()> {
        //Lazy::force(&TRACING);
        let count = 2_000_000;
        let mut pf = PerfectBloomFilter::new()?;

        tracing::info!("Starting Insert phase 1");
        for i in 0..count {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;

            assert_eq!(was_present, false);
        }
        tracing::info!("Completed Insert phase 1");
        //let mut locked_pf = pf.lock().await;

        tracing::info!("Starting confirmation phase 1");
        for i in 0..count {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        Ok(())
    }
}
