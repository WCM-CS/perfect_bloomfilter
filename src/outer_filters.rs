

pub const OUTERBLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const OUTER_ARRAY_SHARDS: u32 = 4096;

pub const OUTER_BLOOM_STARTING_MULT: u32 = 13;
pub const OUTER_BLOOM_STARTING_LENGTH: u64 = 2_u64.pow( OUTER_BLOOM_STARTING_MULT);


pub struct OuterBlooms {
    outer_shards: Arc<RwLock<InnerShardArray>>,
    outer_metadata: Arc<RwLock<MetaData>>,
    outer_disk_cache: Arc<Mutex<HashMap<u32, Vec<String>>>>, //cache struct
}


impl Default for OuterBlooms {
    fn default() -> Self {
        Self::new()
    }
}