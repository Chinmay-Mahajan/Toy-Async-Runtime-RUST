# Toy Async Executor in Rust

A minimal, from-scratch implementation of an async runtime: a custom `Future`, a `Waker`-based
wakeup mechanism, and a single-threaded executor loop. No `tokio`, no `async-std` — just
`std::future`, `std::task`, threads, and channels.

This exists to understand how `async`/`await` actually works under the hood in Rust, not to be
used in production.

## What's built

- **`TimerFuture`** — a custom `Future` that completes after a given `Duration`. Internally it
  spawns an OS thread that sleeps, then flips a `completed` flag and calls `Waker::wake()`.
- **`Task`** — wraps a boxed, pinned future (`Pin<Box<dyn Future<Output = String> + Send>>`) plus
  a channel sender, so a task can requeue itself when woken.
- **`TaskWaker`** — implements `std::task::Wake` for `Arc<Task>`. Calling `.wake()` on it sends
  the task back into the executor's ready queue.
- **`Executor`** — pulls tasks off a channel (`Receiver<Arc<Task>>`) and polls them in a loop.
- **`Spawner`** — wraps the channel's `SyncSender`, used to push new tasks in.

## How it works (the actual mechanics)

1. `spawner.spawn(future)` boxes and pins the future, wraps it in a `Task`, and sends it into the
   channel immediately. First poll happens as soon as the executor picks it up.
2. `executor.run()` blocks on `ready_queue.recv()`. For each task received, it builds a
   `Waker` from a fresh `Arc<TaskWaker { task }>` and calls `future.poll(&mut context)`.
3. Inside `TimerFuture::poll`:
   - If the timer already fired (`completed == true`), return `Poll::Ready(...)`.
   - Otherwise, clone the current `Waker` out of the `Context`, stash it in `shared_state.waker`,
     and return `Poll::Pending`.
4. The executor does **not** retry a pending task on its own. It just moves on to `recv()` again.
   The task only re-enters the queue when *something else* wakes it.
5. On the timer thread: after `sleep(duration)` finishes, it locks the same `shared_state`, sets
   `completed = true`, and calls `waker.wake()`. That call routes through `TaskWaker::wake`,
   which sends `Arc<Task>` back down the channel.
6. The executor's `recv()` unblocks, picks the task back up, polls it again — this time
   `completed` is `true`, so it resolves and the `async` block resumes past the `.await`.
7. `drop(spawner)` closes the channel's sender side, so once all tasks finish and no one is
   holding a `Spawner`, `recv()` returns `Err` and `run()`'s `while let` loop exits.

The key idea: a `Future` doing "slow work" doesn't block the executor. It returns `Pending` and
hands the executor a `Waker` — a callback address, essentially — that lets it re-announce
"I'm ready, poll me again" whenever the real work finishes, on whatever thread that happens to be.

### Why the `Mutex` around `shared_state`

`TimerFuture` and its background thread both need to touch `completed` and `waker`. Because the
check-and-set (`poll`'s "check completed, else store waker") and the background thread's
"set completed, then wake" both happen under the *same* lock, there's no window where a wakeup
could be lost — the state transition and the decision to store/consume the waker are atomic
relative to each other.

## Where this is slightly incorrect

This mimics the *shape* of a real runtime but cuts corners a production executor like tokio,
async-std, or smol would not:

- **Thread-per-timer, not a reactor.** Every `TimerFuture` spawns its own OS thread that just
  sleeps. Real runtimes have a single reactor (backed by `epoll`/`kqueue`/IOCP, or a timer wheel
  for pure timers) that multiplexes thousands of pending timers/IO events on one or a few threads.
  Spawning an OS thread per timer doesn't scale past a few thousand.

- **No I/O integration at all.** This only demonstrates timers. There's no notion of polling
  sockets, files, or other OS resources — which is most of what a real async runtime spends its
  time doing. `TimerFuture` sleeping on a thread is a stand-in for "some slow external event," not
  a general-purpose readiness mechanism.

- **Single-threaded executor, no work-stealing.** `Executor::run` processes one task at a time on
  one thread. Tokio's default runtime runs a multi-threaded, work-stealing scheduler across
  several OS threads with per-worker local queues plus a global queue, so tasks execute in
  parallel and idle workers steal work from busy ones.

- **Locking the future on every poll.** `Task.future` is a `Mutex<Pin<Box<dyn Future...>>>`.
  A real runtime doesn't need a lock here — a task is only ever polled by one worker at a time by
  construction (it's removed from the queue while being polled), so the mutex is pure overhead
  introduced by this design rather than a necessity.

- **No fairness / poll budget.** Tokio caps how much work a single task can do per poll before
  being forced to yield, to stop one greedy task from starving the rest of the queue. This
  executor has no such budget — a pathological future that's always immediately ready could
  monopolize the loop.

- **No panic isolation.** If a task's future panics inside `poll`, this unwinds through
  `Executor::run` and takes the whole executor down. Production runtimes wrap task polling in
  `catch_unwind` so one panicking task doesn't kill the process or other tasks.

- **No backpressure, just a bounded channel that panics.** `spawn` calls
  `.expect("Queue full")` — hitting the 10,000-task limit crashes the sender. Real spawners
  either apply backpressure, block, or grow the queue rather than panicking.

- **No `JoinHandle`.** `spawn` is fire-and-forget; there's no way to get the task's return value,
  detect that it panicked, or cancel it. Tokio's `spawn` returns a `JoinHandle<T>` you can
  `.await` for the result or `.abort()`.

- **No cancellation / `Drop` semantics.** Dropping a task mid-flight here has no defined behavior
  beyond normal Rust drop order. Real runtimes treat dropping a task's future as the cancellation
  mechanism, and I/O resources are expected to clean up correctly when that happens mid-poll.

## Running it

```bash
cargo run
```

Expect Task 2 (1s timer) to resume before Task 1 (3s timer), interleaved with their "Step A"
prints, since both are spawned before the executor starts polling either.