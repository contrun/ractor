// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

use criterion::{criterion_group, criterion_main, profiler::Profiler, BatchSize, Criterion};
#[cfg(feature = "cluster")]
use ractor::Message;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::{
    alloc::{GlobalAlloc, System},
    sync::atomic::{AtomicUsize, Ordering},
};

#[global_allocator]
static GLOBAL: ReportingAllocator<System> = ReportingAllocator::new(System);

struct ReportingAllocator<T: GlobalAlloc> {
    alloc: T,
    size: AtomicUsize,
}

impl<T: GlobalAlloc> ReportingAllocator<T> {
    pub const fn new(alloc: T) -> Self {
        Self {
            alloc,
            size: AtomicUsize::new(0),
        }
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    pub fn reset(&self) {
        self.size.store(0, Ordering::SeqCst);
    }
}

unsafe impl<T: GlobalAlloc> GlobalAlloc for ReportingAllocator<T> {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        self.size.fetch_add(layout.size(), Ordering::SeqCst);
        self.alloc.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        self.alloc.dealloc(ptr, layout);
    }
}

struct MemoryProfiler;

impl Profiler for MemoryProfiler {
    fn start_profiling(&mut self, _: &str, _: &std::path::Path) {
        GLOBAL.reset();
    }

    fn stop_profiling(&mut self, _: &str, _: &std::path::Path) {
        let size = GLOBAL.size() / 1024;
        println!("; allocated {} KiB", size);
    }
}

struct BenchActor;

struct BenchActorMessage;
#[cfg(feature = "cluster")]
impl Message for BenchActorMessage {}

#[cfg_attr(feature = "async-trait", ractor::async_trait)]
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

fn create_actors(c: &mut Criterion) {
    let small = 100;
    let large = 10000;

    let id = format!("Creation of {small} actors");
    #[cfg(not(feature = "async-std"))]
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    #[cfg(feature = "async-std")]
    let _ = async_std::task::block_on(async {});
    c.bench_function(&id, move |b| {
        b.iter_batched(
            || {},
            |()| {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move {
                        let mut handles = vec![];
                        for _ in 0..small {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            handles.push(handler);
                        }
                        handles
                    })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        let mut handles = vec![];
                        for _ in 0..small {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            handles.push(handler);
                        }
                        handles
                    })
                }
            },
            BatchSize::PerIteration,
        );
    });

    let id = format!("Creation of {large} actors");
    #[cfg(not(feature = "async-std"))]
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    #[cfg(feature = "async-std")]
    let _ = async_std::task::block_on(async {});
    c.bench_function(&id, move |b| {
        b.iter_batched(
            || {},
            |()| {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move {
                        let mut handles = vec![];
                        for _ in 0..large {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            handles.push(handler);
                        }
                        handles
                    })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        let mut handles = vec![];
                        for _ in 0..large {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            handles.push(handler);
                        }
                        handles
                    })
                }
            },
            BatchSize::PerIteration,
        );
    });
}

fn schedule_work(c: &mut Criterion) {
    let small = 100;
    let large = 1000;

    let id = format!("Waiting on {small} actors to process first message");
    #[cfg(not(feature = "async-std"))]
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    #[cfg(feature = "async-std")]
    let _ = async_std::task::block_on(async {});
    c.bench_function(&id, move |b| {
        b.iter_batched(
            || {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move {
                        let mut join_set = ractor::concurrency::JoinSet::new();

                        for _ in 0..small {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            join_set.spawn(handler);
                        }
                        join_set
                    })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        let mut join_set = ractor::concurrency::JoinSet::new();

                        for _ in 0..small {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            join_set.spawn(handler);
                        }
                        join_set
                    })
                }
            },
            |mut handles| {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move { while handles.join_next().await.is_some() {} })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        while handles.join_next().await.is_some() {}
                    })
                }
            },
            BatchSize::PerIteration,
        );
    });

    let id = format!("Waiting on {large} actors to process first message");
    #[cfg(not(feature = "async-std"))]
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    #[cfg(feature = "async-std")]
    let _ = async_std::task::block_on(async {});
    c.bench_function(&id, move |b| {
        b.iter_batched(
            || {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move {
                        let mut join_set = ractor::concurrency::JoinSet::new();
                        for _ in 0..large {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            join_set.spawn(handler);
                        }
                        join_set
                    })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        let mut join_set = ractor::concurrency::JoinSet::new();
                        for _ in 0..large {
                            let (_, handler) = Actor::spawn(None, BenchActor, ())
                                .await
                                .expect("Failed to create test agent");
                            join_set.spawn(handler);
                        }
                        join_set
                    })
                }
            },
            |mut handles| {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move { while handles.join_next().await.is_some() {} })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        while handles.join_next().await.is_some() {}
                    })
                }
            },
            BatchSize::PerIteration,
        );
    });
}

#[allow(clippy::async_yields_async)]
fn process_messages(c: &mut Criterion) {
    const NUM_MSGS: u64 = 100000;

    struct MessagingActor {
        num_msgs: u64,
    }

    #[cfg_attr(feature = "async-trait", ractor::async_trait)]
    impl Actor for MessagingActor {
        type Msg = BenchActorMessage;

        type State = u64;

        type Arguments = ();

        async fn pre_start(
            &self,
            myself: ActorRef<Self::Msg>,
            _: (),
        ) -> Result<Self::State, ActorProcessingErr> {
            let _ = myself.cast(BenchActorMessage);
            Ok(0u64)
        }

        async fn handle(
            &self,
            myself: ActorRef<Self::Msg>,
            _message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            *state += 1;
            if *state >= self.num_msgs {
                myself.stop(None);
            } else {
                let _ = myself.cast(BenchActorMessage);
            }
            Ok(())
        }
    }

    let id = format!("Waiting on {NUM_MSGS} messages to be processed");
    #[cfg(not(feature = "async-std"))]
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    #[cfg(feature = "async-std")]
    let _ = async_std::task::block_on(async {});
    c.bench_function(&id, move |b| {
        b.iter_batched(
            || {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move {
                        let (_, handle) =
                            Actor::spawn(None, MessagingActor { num_msgs: NUM_MSGS }, ())
                                .await
                                .expect("Failed to create test actor");
                        handle
                    })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        let (_, handle) =
                            Actor::spawn(None, MessagingActor { num_msgs: NUM_MSGS }, ())
                                .await
                                .expect("Failed to create test actor");
                        handle
                    })
                }
            },
            |handle| {
                #[cfg(not(feature = "async-std"))]
                {
                    runtime.block_on(async move {
                        let _ = handle.await;
                    })
                }
                #[cfg(feature = "async-std")]
                {
                    async_std::task::block_on(async move {
                        let _ = handle.await;
                    })
                }
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group! {
    name = actors;
    config = Criterion::default().with_profiler(MemoryProfiler);
    targets = create_actors, schedule_work, process_messages
}
criterion_main!(actors);
