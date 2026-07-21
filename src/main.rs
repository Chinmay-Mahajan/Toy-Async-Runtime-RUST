use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
    thread,
};

use std::sync::mpsc::{sync_channel, Receiver, SyncSender}; 


// The shared state between our future and the background timer thread 
// this is basically our state machine 
pub struct TimerState<T> {
    completed: bool, // whether or not the task is done or not 
    waker: Option<Waker>, // this has the address of the executor's queue (if a task when polled returns pending we set waker to hold the address of the executor loops so that when the task is done the async block automatically pushes itself into the executor's queue)
    result: Option<T>, // the result of the future once it is done.
}

// wrapping the state machine in Arc and Mutex so that it can be shared across threads safely 
pub struct TimerFuture<T> {
    shared_state: Arc<Mutex<TimerState<T>>>,
}

impl<T: Send + 'static> TimerFuture<T> {
    pub fn new(duration: Duration, f: impl FnOnce() -> T + Send + 'static) -> Self {
        let shared_state = Arc::new(Mutex::new(TimerState {
            completed: false,
            waker: None, // initialized as None as mentioned above it is only set to the executor queue if it returned Poll::Pending when it was polled by the executor
            result: None,
        }));
        let thread_shared_state = shared_state.clone();
        thread::spawn(move || {
            thread::sleep(duration); // Simulating the slow work
            let mut shared_state = thread_shared_state.lock().unwrap(); // moving the clone of shared_state to the spawned thread.
            
            //the slow operation is done
            let result = f(); // execute the function passed to the timer future and get the result 
            shared_state.result = Some(result); // store the result in the shared state
            shared_state.completed = true;
            if let Some(waker) = shared_state.waker.take() {
                waker.wake(); // Wake up the executor to poll the future again  
                // it is the futures job to wake the executor once it is finished (in the meantime the executor is free to do other work)
            }
            
        });

        TimerFuture { shared_state } // return in parrallel while the spawned thread keeps on executing
    }
}

impl<T> Future for TimerFuture<T> {
    type Output = T; // the output dtype the future would return once it is done. 
    // now trying to make this return any dtype 

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // cx : Context<'_> is a wrapper , wrapping the waker 
        let mut shared_state = self.shared_state.lock().unwrap();
        if shared_state.completed { 
            // return Poll::Ready("Timer Finished".to_string());
            // if the future is completed return Poll::Ready with the result of the future
            if let Some(value) = shared_state.result.take() {
                return Poll::Ready(value);
            }
            Poll::Pending
        } else {
            let waker = cx.waker().clone(); 
            shared_state.waker = Some(waker);
            Poll::Pending
        }
    }
} 


struct Task {
    // Pin<Box<...>> locks the future's memory address in space.
    // Mutex allows multiple threads to safely look at it.
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    task_sender: SyncSender<Arc<Task>>, // having the sender as a field so that whenever that future returns a ready the task can push itself to the sender's channel
}


// To bridge the custom `Task` struct with Rust's standard library `Waker`,
// we need a placeholder struct that implements `std::task::Wake`.
struct TaskWaker {
    task: Arc<Task>,
}

impl std::task::Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        // when called sends the task itself down the sender's channel
        let _ = self.task.task_sender.send(self.task.clone());
    }
}

struct Executor {
    ready_queue: Receiver<Arc<Task>>,
}

impl Executor {
    fn run(&self) {
        // This loop blocks and waits for any tasks arriving on the channel queue
        while let Ok(task) = self.ready_queue.recv() {
            let mut future = task.future.lock().unwrap();
            
            // Construct a standard library Waker from TaskWaker struct
            let waker = Waker::from(Arc::new(TaskWaker { task: task.clone() }));
            let mut context = Context::from_waker(&waker); // about the lifetime ,<'_> , since it is pointing to waker in the memory and to avoid dangling pointer (when the data is deleted but a pointer to it remains). 
            // The context must be alive (not cleaned) till waker is alive . 
            
            let _ = future.as_mut().poll(&mut context);
        }
    }
}

struct Spawner {
    task_sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            task_sender: self.task_sender.clone(),
        });
        
        // Push the newly created task straight into the execution queue channel
        self.task_sender.send(task).expect("Queue full");
    }
}

fn main() {
    let (task_sender, ready_queue) = sync_channel(10_000);
    let executor = Executor { ready_queue };
    let spawner = Spawner { task_sender };

    spawner.spawn(async {
        println!("[Task 1] Step A: Initiating 3-second delay...");
        let result: String = TimerFuture::new(Duration::from_secs(3), || "Task 1 Done!".to_string()).await;
        println!("[Task 1] Step B: Resumed successfully with result: {}", result);
    });
    
    spawner.spawn(async {
        println!("[Task 2] Step A: Initiating 1-second delay...");
        let result: u32 = TimerFuture::new(Duration::from_secs(1), || 42).await;
        println!("[Task 2] Step B: Resumed successfully with result: {}", result);
    });
    
    drop(spawner); // dropping the spawner closes the queue channel , so the above written while loop ends
    executor.run();
    println!("--- ALL TASKS EXECUTED SUCCESSFULLY ---");
}