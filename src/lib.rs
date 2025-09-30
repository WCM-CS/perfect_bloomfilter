use bitvec::bitvec;
use bitvec::vec::BitVec;
use std::io::{BufRead, BufReader, Write};
use std::sync::RwLock;
use std::{fs, io};

pub struct PerfectBloomFilter {
    pub(crate) cartographer: Vec<RwLock<Shard>>,
    pub(crate) inheritor: Vec<RwLock<Shard>>,
}

impl PerfectBloomFilter {
    pub fn new() -> Self {
        if let Some(parent) = std::path::Path::new("./data/pbf_data/init.txt").parent() {
            fs::create_dir_all(parent).unwrap();
        }

        let shard_vector_len_mult: usize = 12; // Static: predetermined on initialization, this * vec(2) = total bloom filters
        let filter_len_mult: usize = 12; // Dynamic: Initialized the blom filters bit vec length
        let filter_hash_family_size: usize = 7;
        let shard_vector_len = 1usize << shard_vector_len_mult;

        let mut cartographer = Vec::with_capacity(shard_vector_len);
        let mut inheritor = Vec::with_capacity(shard_vector_len);

        for shard_id in 0..shard_vector_len {
            cartographer.push(RwLock::new(Shard::new_cartographer_shard(
                0,
                shard_id as u32,
                1usize << filter_len_mult,
                filter_len_mult,
                filter_hash_family_size,
            )));

            inheritor.push(RwLock::new(Shard::new_inheritor_shard(
                0,
                shard_id as u32,
                1usize << filter_len_mult,
                filter_len_mult,
                filter_hash_family_size,
            )));
        }

        Self {
            cartographer,
            inheritor,
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        let key_slice = key.as_bytes();

        let cart_exists = Self::existence_check(&self, key_slice, &ShardType::Cartographer);
        let inher_exists = Self::existence_check(&self, key_slice, &ShardType::Inheritor);

        cart_exists & inher_exists
    }

    pub fn insert(&self, key: &str) {
        let key_slice = key.as_bytes();

        Self::insert_key(&self, key_slice, &ShardType::Cartographer);
        Self::insert_key(&self, key_slice, &ShardType::Inheritor);
    }

    fn existence_check(&self, key: &[u8], shard_type: &ShardType) -> bool {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);

        for shard in shards {
            let shard = &shard_vec[shard].read().unwrap();
            let hashes = shard.bloom_hash(key);
            let exist = shard.bloom_check(&hashes);

            if !exist {
                return false;
            }
        }

        true
    }

    fn insert_key(&self, key: &[u8], shard_type: &ShardType) -> bool {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);

        for shard in shards {
            let shard = &mut shard_vec[shard].write().unwrap();
            let hashes = shard.bloom_hash(key);
            shard.bloom_insert(&hashes, key);
            let rehash = shard.rehash_check();
            shard.drain(rehash);

            rehash.then(|| shard.rehash_bloom());
        }

        true
    }

    fn array_sharding_hash(&self, key: &[u8], shard_type: &ShardType) -> Vec<usize> {
        let (hash_seed, mask) = match shard_type {
            ShardType::Cartographer => (HASH_SEED_SELECTION[0], self.cartographer.len() - 1),
            ShardType::Inheritor => (HASH_SEED_SELECTION[1], self.inheritor.len() - 1),
        };

        let hash = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seed);

        let high = (hash >> 64) as u64;
        let low = hash as u64;

        let p1 = jump_hash_partition(high ^ low, mask + 1);
        let p2 = (p1 + (mask/2)) & mask;

        debug_assert!(p1 != p2, "Partitions must be unique");

        vec![p1, p2]
    }
}

pub struct Shard {
    filter: BitVec,
    filter_layer: ShardType,
    key_count: u64,
    bloom_length: usize,
    bloom_length_mult: usize,
    hash_family_size: usize,
    key_cache: Vec<Vec<u8>>,
    file_path: String,
}

impl Shard {
    pub fn bloom_hash(&self, key: &[u8]) -> Vec<usize> {
        let hash_seeds = match self.filter_layer {
            ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
            ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
        };

        let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[0]);
        let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[1]);

        let mut hash_list = Vec::with_capacity(*&self.hash_family_size);
        for idx in 0..*&self.hash_family_size {
            let idx_u128 = idx as u128;
            let mask = (&self.bloom_length - 1) as u128;
            let index = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask; // Kirsch-Mitzenmacher optimization

            hash_list.push(index as usize)
        }

        hash_list
    }

    fn bloom_insert(&mut self, hashes: &Vec<usize>, key: &[u8]) {
        hashes.iter().for_each(|hash| self.filter.set(*hash, true));
        self.key_count += 1;
        self.key_cache.push(key.to_vec());
    }

    fn bloom_check(&self, hashes: &Vec<usize>) -> bool {
        let res = hashes.iter().all(|hash| self.filter[*hash]);
        res
    }

    fn drain(&mut self, force_drain: bool) {
        if self.key_cache.len() >= 50 || force_drain {
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.file_path)
                .unwrap();

            let mut writer = io::BufWriter::new(file);
            let keys = std::mem::take(&mut self.key_cache);

            for key in keys {
                let _ = writer.write_all(&key); 
                let _ = writer.write_all(b":");
            }

            let _ = writer.flush();
        }
    }

    fn rehash_check(&self) -> bool {
        if (self.bloom_length as f64 / self.key_count as f64) <= 15.0 { // 19.2 rehsah constant to start
            true
        } else {
            false
        }
    }

    fn rehash_bloom(&mut self) {
        let file = fs::File::open(&self.file_path).unwrap();
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();

        let new_bloom_length = 1usize << (self.bloom_length_mult + 1);
        let mut new_bloomfilter = bitvec![0; new_bloom_length];

        self.bloom_length = new_bloom_length;
        self.bloom_length_mult += 1;

        loop {
            let bytes_read = reader.read_until(b':', &mut buffer).unwrap();
            if bytes_read == 0 {
                break;
            }

            if buffer.ends_with(&[b':']) {
                buffer.pop();
            }

            let hashes = self.bloom_hash(&buffer);
            for hash in hashes {
                new_bloomfilter.set(hash, true);
            }

            buffer.clear();
        }

        self.filter = new_bloomfilter;
    }

    fn new_cartographer_shard(
        key_count: u64,
        shard_id: u32,
        filter_starting_len: usize,
        filter_starting_len_mult: usize,
        filter_starting_hash_family: usize,
    ) -> Self {
        Self {
            filter: bitvec![0; filter_starting_len],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: filter_starting_hash_family,
            filter_layer: ShardType::Cartographer,
            key_cache: vec![],
            file_path: format!(
                "./data/pbf_data/{}_{}.txt",
                ShardType::Cartographer.as_static_str(),
                shard_id
            ),
        }
    }

    fn new_inheritor_shard(
        key_count: u64,
        shard_id: u32,
        filter_starting_len: usize,
        filter_starting_len_mult: usize,
        filter_starting_hash_family: usize,
    ) -> Self {
        Self {
            filter: bitvec![0; filter_starting_len],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: filter_starting_hash_family,
            filter_layer: ShardType::Inheritor,
            key_cache: vec![],
            file_path: format!(
                "./data/pbf_data/{}_{}.txt",
                ShardType::Inheritor.as_static_str(),
                shard_id
            ),
        }
    }
    /*
    fn new_harbinger_shard(
        key_count: u64,
        shard_id: u32,
        filter_starting_len: u64,
        filter_starting_len_mult: u32,
        filter_starting_hash_family: u32
    ) -> Result<Self> {

        Ok(Self {
            filter: bitvec![0; filter_starting_len as usize],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: filter_starting_hash_family,
            filter_layer: ShardType::Harbinger,
            shard_id: shard_id,

            lock: RwLock::new(()),
        })
    }
     */
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub enum ShardType {
    Cartographer,
    Inheritor,
    // Harbinger,
}

impl ShardType {
    pub fn as_static_str(&self) -> &'static str {
        match self {
            ShardType::Cartographer => "cartographer",
            ShardType::Inheritor => "inheritor",
            // ShardType::Harbinger => "harbinger",
        }
    }
}



const JUMP_HASH_SHIFT: i32 = 33;
const JUMP_HASH_CONSTANT: u64 = 2862933555777941757;
const JUMP: f64 = (1u64 << 31) as f64;

// JumpConsistentHash function, rust port from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
// Original algorithm founded in 2014 by google Lamping & Veach
pub fn jump_hash_partition(key: u64, buckets: usize) -> usize {
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

    b as usize
}

pub const HASH_SEED_SELECTION: [u64; 6] = [
    0x8badf00d, 0xdeadbabe, 0xabad1dea, 0xdeadbeef, 0xcafebabe, 0xfeedface,
];

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use once_cell::sync::Lazy;

    use crate::PerfectBloomFilter;

    static COUNT: i32 = 2_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });

    #[test]
    fn test_insert_and_contains_function() {
        Lazy::force(&TRACING);

        match std::fs::remove_dir_all("./data/pbf_data") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }
        match std::fs::remove_dir_all("./data/metadata") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }

        tracing::info!("Creating PerfectBloomFilter instance");
        let pf = PerfectBloomFilter::new();

        tracing::info!("PerfectBloomFilter created successfully");

        tracing::info!("Contains & insert & contains check");
        for i in 0..COUNT {
            let key = i.to_string();

            let was_present_a = pf.contains(&key);

            if was_present_a {
                tracing::warn!("Fasle positive: {}", key);
            }

            assert_eq!(was_present_a, false);

            let _ = pf.insert(&key);

            let was_present_b = pf.contains(&key);

            if !was_present_b {
                tracing::warn!("Fasle negative: {}", key);
            }

            assert_eq!(was_present_b, true);
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}
