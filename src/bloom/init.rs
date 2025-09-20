use std::sync::Arc;

use anyhow::{Result};
use once_cell::sync::Lazy;


use crate::{bloom::{inner_filters::InnerBlooms, outer_filters::OuterBlooms}};


static GLOBAL_PBF: Lazy<PerfectBloomFilter> = Lazy::new(|| {
    PerfectBloomFilter::new()
});



struct PerfectBloomFilter {
    outer_filter: Arc<OuterBlooms>,
    inner_filter: Arc<InnerBlooms>,
}

impl PerfectBloomFilter {
    pub fn new() -> Self {
        let outer_filter = Arc::new(OuterBlooms::default());
        let inner_filter = Arc::new(InnerBlooms::default());
        
        Self {
            outer_filter,
            inner_filter,
        }
    }


    pub fn contains_insert(&mut self, key: &str) -> Result<bool> {
        let outer_res = self.outer_filter.contains_and_insert(key)?;
        let inner_res = self.inner_filter.contains_and_insert(key)?;

        //Ok(outer_res && inner_res)
        Ok(true)
    }
}







// m = bloom filter len, b = bits per key threshold
//k = number of hashes, n = keys 



