//! Execute with
//!
//! ```text
//! cargo run --example bench_memory_usage
//! ```

// Use Jemalloc to measure memory usage
// https://stackoverflow.com/questions/30869007/how-to-benchmark-memory-usage-of-a-function

use jemalloc_ctl::{epoch, stats};

use ractor::{Actor, ActorProcessingErr, ActorRef};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

struct BenchActor;

struct BenchActorMessage;

impl Actor for BenchActor {
    type Msg = BenchActorMessage;

    type State = ();

    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        let _ = myself.cast(BenchActorMessage);
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        myself.stop(None);
        Ok(())
    }
}

fn create_actors() {
    let n = 10000;

    eprintln!("Creation of {n} actors");
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    runtime.block_on(async move {
        let mut handles = vec![];
        for _ in 0..n {
            let (_, handler) = Actor::spawn(None, BenchActor, ())
                .await
                .expect("Failed to create test agent");
            handles.push(handler);
        }
        handles
    });
}

fn main() {
    loop {
        // many statistics are cached and only updated when the epoch is advanced.
        epoch::advance().unwrap();

        let allocated = stats::allocated::read().unwrap();
        let resident = stats::resident::read().unwrap();
        println!("{} bytes allocated/{} bytes resident", allocated, resident);
        create_actors();
    }
}
