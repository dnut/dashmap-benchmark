use std::collections::HashMap;
use std::hash::Hash;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::hash_map::RandomState, ops::Deref};

use dashmap::DashMap;
use parking_lot::{RwLock, RwLockReadGuard};
use rand::Rng;

pub fn new_dashmap_fn<K: Eq + Hash, V>(shards: usize) -> impl Fn() -> DashMap<K, V> {
    move || DashMap::with_capacity_and_hasher_and_shard_amount(0, RandomState::default(), shards)
}

pub fn new_rwlock_hashmap<K: Eq + Hash, V>() -> RwLock<HashMap<K, V>> {
    RwLock::new(HashMap::new())
}

/// Initializes an outer map and fills it with inner maps
pub fn test_init_many_maps<OuterMap: Map<u64, InnerMap>, InnerMap: Map<u64, ()>>(
    entries: u64,
    new_outer: impl Fn() -> OuterMap,
    new_inner: impl Fn() -> InnerMap,
) {
    let drop_start = {
        let start = SystemTime::now();
        let dm: OuterMap = new_outer();
        let mut peak_mem_megs = 0;
        for i in 0..entries {
            let inner_map: InnerMap = new_inner();
            inner_map.insert(i, ());
            dm.insert(i, inner_map);
            if i % (entries / 100) == 0 {
                peak_mem_megs = std::cmp::max(peak_mem_megs, memory_usage().unwrap() / 1_000_000);
                print!(
                    "\rallocated {}%  | {} MB",
                    i / (entries / 100),
                    peak_mem_megs
                );
                std::io::stdout().flush().unwrap();
            }
        }
        println!("\rallocated 100%");
        print_duration(start, "Init");
        println!("dropping...");
        SystemTime::now()
    };
    print_duration(drop_start, "Drop");
}

pub fn test_contention(
    range: u64,
    prior_writes: u64,
    writes_per_second: u64,
    reads_per_second: u64,
    expensive_reads: bool,
    map: impl Map<u64, ()> + Send + Sync + 'static,
) {
    let map = Arc::new(map);
    let mut handles = vec![];
    let threads_each = usize::from(std::thread::available_parallelism().unwrap()) as u64;
    let write_gap_nanos = gap_nanos(threads_each, writes_per_second);
    let read_gap_nanos = gap_nanos(threads_each, reads_per_second);

    // Initialize the map with some data before running the benchmark
    let mut rng = rand::thread_rng();
    for _ in 0..prior_writes {
        map.insert(rng.gen_range(0..=range), ());
    }

    let start = SystemTime::now();
    for _ in 0..threads_each {
        // Attempt to write data concurrently for ~1 second at the specified rate
        if let Some(write_gap_nanos) = write_gap_nanos {
            let my_map = map.clone();
            handles.push(std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut next = unix_timestamp_nanos();
                for _ in 0..(writes_per_second / threads_each) {
                    let now = unix_timestamp_nanos();
                    if now < next {
                        std::thread::sleep(Duration::from_nanos((next - now) as u64));
                    }
                    my_map.insert(rng.gen_range(0..=range), ());
                    next += write_gap_nanos;
                }
            }));
        }
        // Attempt to read data concurrently for ~1 second at the specified rate
        if let Some(read_gap_nanos) = read_gap_nanos {
            let my_map = map.clone();
            handles.push(std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut next = unix_timestamp_nanos();
                for _ in 0..(reads_per_second / threads_each) {
                    let now = unix_timestamp_nanos();
                    if now < next {
                        std::thread::sleep(Duration::from_nanos((next - now) as u64));
                    }
                    if expensive_reads {
                        my_map.keys();
                    } else {
                        my_map.get(&rng.gen_range(0..=range));
                    }
                    next += read_gap_nanos;
                }
            }));
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
    print_duration(start, "\nContention test");
}

pub fn gap_nanos(threads: u64, rate_per_second: u64) -> Option<u128> {
    if rate_per_second == 0 {
        None
    } else {
        Some(threads as u128 * 1_000_000_000 / rate_per_second as u128)
    }
}

pub trait Map<K, V> {
    fn insert(&self, key: K, value: V);
    fn get(&self, key: &K) -> Option<impl Deref<Target = V>>;
    fn keys(&self) -> Vec<K>;
}

impl<K: Eq + Hash + Clone, V> Map<K, V> for DashMap<K, V> {
    fn insert(&self, key: K, value: V) {
        DashMap::insert(self, key, value);
    }

    fn get(&self, key: &K) -> Option<impl Deref<Target = V>> {
        DashMap::get(self, key)
    }

    fn keys(&self) -> Vec<K> {
        self.iter().map(|e| e.key().clone()).collect()
    }
}

impl<K: Eq + Hash + Clone, V> Map<K, V> for RwLock<HashMap<K, V>> {
    fn insert(&self, key: K, value: V) {
        HashMap::insert(&mut self.write(), key, value);
    }

    fn get(&self, key: &K) -> Option<impl Deref<Target = V>> {
        RwLockReadGuard::try_map(self.read(), |hm| hm.get(key)).ok()
    }

    fn keys(&self) -> Vec<K> {
        self.read().keys().cloned().collect()
    }
}

pub fn memory_usage() -> Option<u64> {
    sysinfo::System::new_all()
        .process(sysinfo::Pid::from(std::process::id() as usize))
        .map(|p| p.memory())
}

pub fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub fn print_duration(since: SystemTime, label: &str) {
    let end = SystemTime::now();
    println!(
        "{label} duration: {}s",
        end.duration_since(since).unwrap().as_millis() as f64 / 1000.0
    );
}
