#[derive(Debug)]
pub struct BloomFilterConfig {
    pub rehash: Option<bool>,           // True by default
    pub throughput: Option<Throughput>, // 12
    pub accuracy: Option<Accuracy>,     // 15.0
    pub initial_capacity: Option<Capacity>, // 12
    pub worker_cores: Option<Workers>,           // rehashing worker cores
                                        // NEW FEATURES ~ TO_DO()!!
                                        // pub cascade_tiers: Option<Tiers> Options: 1, 2, 3
                                        // pub persistence: Option<bool>
                                        // pub file_path: Option<String/PathBuf>  needed for persistence
}

// Shard count
#[derive(Debug, Clone, Copy)]
pub enum Throughput {
    Low,
    Medium,
    High,
}

// Rehash threshold
#[derive(Debug, Clone, Copy)]
pub enum Accuracy {
    Low,
    Medium,
    High,
}

// Initial filter length
#[derive(Debug, Clone, Copy)]
pub enum Capacity {
    Low,
    Medium,
    High,
    VeryHigh,
}

// rehashing worker cores
#[derive(Debug, Clone, Copy)]
pub enum Workers {
    Cores1,
    Cores4,
    Cores8,
    HalfSysMax,
}


impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            rehash: Some(true),
            throughput: Some(Throughput::Medium),
            accuracy: Some(Accuracy::Medium),
            initial_capacity: Some(Capacity::Medium),
            worker_cores: Some(Workers::Cores1) // default to one offloaded rehashing core, force user to spec further 
        }
    }
}

impl BloomFilterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    // setters
    pub fn with_rehash(mut self, rehash: bool) -> Self {
        self.rehash = Some(rehash);
        self
    }

    pub fn with_throughput(mut self, volume: Throughput) -> Self {
        self.throughput = Some(volume);
        self
    }

    pub fn with_accuracy(mut self, accuracy: Accuracy) -> Self {
        self.accuracy = Some(accuracy);
        self
    }

    pub fn with_initial_capacity(mut self, capacity: Capacity) -> Self {
        self.initial_capacity = Some(capacity);
        self
    }

    pub fn with_worker_cores(mut self, workers: Workers) -> Self {
        self.worker_cores = Some(workers);
        self
    }


    // getters
    pub fn rehash(&self) -> bool {
        self.rehash.unwrap_or(true)
    }

    pub fn throughput(&self) -> Throughput {
        self.throughput.unwrap_or(Throughput::Medium)
    }

    pub fn accuracy(&self) -> Accuracy {
        self.accuracy.unwrap_or(Accuracy::Medium)
    }

    pub fn initial_capacity(&self) -> Capacity {
        self.initial_capacity.unwrap_or(Capacity::Medium)
    }

    pub fn workers(&self) -> Workers {
        self.worker_cores.unwrap_or(Workers::Cores1)
    }
}