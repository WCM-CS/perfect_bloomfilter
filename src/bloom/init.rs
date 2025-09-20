use std::sync::Arc;
use anyhow::{Result};
use once_cell::sync::Lazy;


use crate::{bloom::{inner_filters::InnerBlooms, outer_filters::OuterBlooms}};


pub static GLOBAL_PBF: Lazy<PerfectBloomFilter> = Lazy::new(|| {
    PerfectBloomFilter::new()
});



pub struct PerfectBloomFilter {
    pub(crate) outer_filter: Arc<OuterBlooms>,
    pub(crate) inner_filter: Arc<InnerBlooms>,
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
        let outer_res = OuterBlooms::contains_and_insert(&key)?;
        let inner_res = InnerBlooms::contains_and_insert(&key)?;

        Ok(outer_res && inner_res)
        //Ok(true)
    }
}







// m = bloom filter len, b = bits per key threshold
//k = number of hashes, n = keys 



#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;
    use anyhow::Result;

    use once_cell::sync::Lazy;

    use crate::bloom::init::PerfectBloomFilter;

    static COUNT: i32 = 1_500_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });

     #[test]
    fn test_filter_loop_sync() -> Result<()> {
       
        Lazy::force(&TRACING);

        tracing::info!("Creating PerfectBloomFilter instance");
        let mut pf = PerfectBloomFilter::new();

        tracing::info!("PerfectBloomFilter created successfully");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;
            if i % 100_000 == 0 {
                std::thread::sleep(Duration::from_millis(500));
            }
            if was_present {
                tracing::error!("Insertion Phase Failed at key {}", i);
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, false);

        }

        //let _ = std::thread::sleep(Duration::from_secs(3));
        tracing::info!("Starting confirmation phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;
            if i % 100_000 == 0 {
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        //let _ = std::thread::sleep(Duration::from_secs(3));

        
        Ok(())
    }
}