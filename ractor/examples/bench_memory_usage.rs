//! Execute with
//!
//! ```text
//! cargo run --example bench_memory_usage
//! ```

// Use Jemalloc to measure memory usage
// https://stackoverflow.com/questions/30869007/how-to-benchmark-memory-usage-of-a-function

use jemalloc_ctl::{epoch, stats};

use ractor::{concurrency::JoinSet, Actor, ActorProcessingErr, ActorRef};
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinError,
};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

const N_MESSAGES: usize = 100;
const N_ACTORS: usize = 100000;
const STATE_SIZE: usize = 1024;

pub struct RootActor;

impl Actor for RootActor {
    type Msg = ();
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        _: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn post_stop(
        &self,
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        myself
            .get_cell()
            .stop_children_and_wait(Some("Root actor stopped".to_string()), None)
            .await;
        Ok(())
    }
}

struct BenchActor;

impl Actor for BenchActor {
    type Msg = String;

    type State = [u8; STATE_SIZE];

    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        for i in 0..N_MESSAGES {
            let msg = format!("Hello, world! {}", i);
            myself.send_message(msg).expect("actor alive");
        }
        Ok([0; STATE_SIZE])
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

struct Task<T> {
    join_set: JoinSet<T>,
    root: ActorRef<()>,
    runtime: Option<Runtime>,
}

impl<T> Task<T> {
    fn new(runtime: Runtime, root: ActorRef<()>, join_set: JoinSet<T>) -> Self {
        Self {
            join_set,
            root,
            runtime: Some(runtime),
        }
    }
}

impl<T: 'static> Task<T> {
    fn cancel(&mut self) {
        self.root.stop(Some("Root actor stopped".to_string()));
        let runtime = self.runtime.take().unwrap();
        runtime.block_on(async move { while self.join_set.join_next().await.is_some() {} })
    }
}

fn create_actors() -> Task<Result<(), JoinError>> {
    eprintln!(
        "Creation of {N_ACTORS} actors with {N_MESSAGES} messages and state size {STATE_SIZE}"
    );
    let runtime = Builder::new_multi_thread().build().unwrap();
    let (root, join_set) = runtime.block_on(async move {
        let mut join_set = ractor::concurrency::JoinSet::new();

        let (root, _handler) = Actor::spawn(None, RootActor, ())
            .await
            .expect("Failed to create test agent");

        let root_cell = root.get_cell();

        for _ in 0..N_ACTORS {
            let (_, handler) = Actor::spawn_linked(None, BenchActor, (), root_cell.clone())
                .await
                .expect("Failed to create test agent");
            join_set.spawn(handler);
        }
        (root, join_set)
    });
    Task::new(runtime, root, join_set)
}

fn print_memory_usage() {
    // many statistics are cached and only updated when the epoch is advanced.
    epoch::advance().unwrap();

    let allocated = stats::allocated::read().unwrap();
    let resident = stats::resident::read().unwrap();
    eprintln!("{} bytes allocated/{} bytes resident", allocated, resident);
}

fn main() {
    loop {
        print_memory_usage();
        let mut task = create_actors();
        print_memory_usage();
        task.cancel();
    }
}
