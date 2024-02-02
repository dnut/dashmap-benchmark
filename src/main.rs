use clap::{command, Parser, Subcommand, ValueEnum};

use dashmap_benchmark::{
    new_dashmap_fn, new_rwlock_hashmap, test_contention, test_init_many_maps, ContentionFocus,
};

fn main() {
    let args = Args::parse();
    println!(
        "running load test: {args:#?}   dashmap_shards: {}",
        args.dashmap_shards()
    );
    match args.test {
        Test::Init {
            entries,
            inner_items,
        } => match args.map {
            MapType::Dashmap => test_init_many_maps(
                entries,
                inner_items,
                new_dashmap_fn(args.dashmap_shards()),
                new_dashmap_fn(args.dashmap_shards()),
            ),
            MapType::Hashmap => {
                test_init_many_maps(entries, inner_items, new_rwlock_hashmap, new_rwlock_hashmap)
            }
        },
        Test::Contention {
            focus,
            max_entries,
            prior_writes,
            writes_per_second,
            reads_per_second,
            cheap_reads,
        } => match args.map {
            MapType::Dashmap => test_contention(
                focus,
                max_entries.unwrap_or(prior_writes + writes_per_second),
                prior_writes,
                writes_per_second,
                reads_per_second,
                cheap_reads,
                new_dashmap_fn(args.dashmap_shards())(),
            ),
            MapType::Hashmap => test_contention(
                focus,
                max_entries.unwrap_or(prior_writes + writes_per_second),
                prior_writes,
                writes_per_second,
                reads_per_second,
                cheap_reads,
                new_rwlock_hashmap(),
            ),
        },
    }

    println!("\ndone");
}

/// Run a load test on a concurrent hash map implementation.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Map implementation to use
    #[arg(value_enum)]
    map: MapType,

    /// Number of shards to use for DashMap. Normally is `4 * core_count`
    #[arg(short, long)]
    shards: Option<usize>,

    /// Number of CPU cores to simulate. Multiplied by 4 to get the DashMap
    /// shard count, unless `shards` is set
    #[arg(short, long)]
    cores: Option<usize>,

    #[command(subcommand)]
    test: Test,
}

impl Args {
    fn dashmap_shards(&self) -> usize {
        self.shards.unwrap_or(
            (4 * self
                .cores
                .unwrap_or(std::thread::available_parallelism().unwrap().into()))
            .next_power_of_two(),
        )
    }
}

#[derive(Clone, Debug, ValueEnum)]
enum MapType {
    Dashmap,
    Hashmap,
}

#[derive(Clone, Debug, Subcommand)]
enum Test {
    /// Initialize a large number of maps, inserting them all into a single outer map.
    Init {
        /// Number of inner maps to insert into the outer map
        #[arg(short, long, default_value_t = 10_000_000)]
        entries: u64,

        /// Number of items to insert into each inner map on average (normally distributed)
        #[arg(short, long, default_value_t = 0)]
        inner_items: u64,
    },

    /// Using a single map, executes read and write operations at the specified rates.
    /// The test should take about 1 second unless there is a bottleneck.
    Contention {
        /// Imposes a cap on the size of the map by restricting keys
        /// such that they are randomly selected from 0..=max_entries.
        /// Default = `prior_writes + writes_per_second`
        #[arg(short, long)]
        max_entries: Option<u64>,

        /// Number of entries to write into the map before beginning the benchmark.
        #[arg(short, long, default_value_t = 0)]
        prior_writes: u64,

        /// Number of write operations to execute per second.
        /// This is also the total number of write operations,
        /// so the test is expected to finish in about one second.
        #[arg(short, long, default_value_t = 10_000_000)]
        writes_per_second: u64,

        /// Number of read operations to execute per second.
        /// This is also the total number of read operations,
        /// so the test is expected to finish in about one second.
        #[arg(short, long, default_value_t = 10_000_000)]
        reads_per_second: u64,

        /// If true, each read is just a single `get` from the map.
        /// If false, each read copies all keys from the map into a vec.
        #[arg(short, long, default_value_t = false)]
        cheap_reads: bool,

        /// If a focus is selected, that means the other operation will be looped infinitely.
        /// The test ends as soon as the focused operation completes.
        #[arg(short, long)]
        focus: Option<ContentionFocus>,
    },
}
