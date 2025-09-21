use std::{sync::Arc, thread, time::Duration};
use anyhow::{Result};
use once_cell::sync::Lazy;
use threadpool::ThreadPool;

use crate::bloom::{inner_filters::InnerBlooms, io::{write_disk_io_cache, GLOBAL_IO_CACHE}, outer_filters::OuterBlooms};


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

        let pool = ThreadPool::new(2);

        pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(5));


                let data = {
                    let mut locked_cache = GLOBAL_IO_CACHE.outer_cache.write().unwrap();
                    let cache_data = std::mem::take(&mut *locked_cache);
                    cache_data
                };

                let _ = write_disk_io_cache(data, crate::FilterType::Outer);
            }
        });


        pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(5));


                let data = {
                    let mut locked_cache = GLOBAL_IO_CACHE.inner_cache.write().unwrap();
                    let cache_data = std::mem::take(&mut *locked_cache);
                    cache_data
                };

                let _ = write_disk_io_cache(data, crate::FilterType::Inner);
            }
        });
        
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

    static COUNT: i32 = 5_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });

     #[test]
    fn test_filter_loop_sync() -> Result<()> {
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
        let mut pf = PerfectBloomFilter::new();

        tracing::info!("PerfectBloomFilter created successfully");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;
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

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        let _ = std::thread::sleep(Duration::from_secs(3));

        
        Ok(())
    }
}