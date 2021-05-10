use crossbeam_channel::{Receiver, Sender};
use std::{
    thread::JoinHandle,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub(crate) enum TimerType {
    OneShot(Box<dyn FnOnce(ScheduleToken) + Send + 'static>),
    Recurring(Duration, Box<dyn FnMut(ScheduleToken) + Send + 'static>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScheduleToken {
    uuid: Uuid,
}

impl ScheduleToken {
    fn new() -> Self {
        Self { uuid: Uuid::new_v4() }
    }
}

enum ThreadMessage {
    Shutdown,
    Cancel(ScheduleToken),
    Run(TimerEntry),
}

struct TimerEntry {
    schedule_token: ScheduleToken,
    scheduled_at: Instant,
    timer_type: TimerType,
}

#[derive(Debug)]
pub(crate) struct TimerHandle {
    join_handle: JoinHandle<()>,
    timer_ref: TimerRef,
}

#[derive(Debug, Clone)]
pub(crate) struct TimerRef {
    thread_tx: Sender<ThreadMessage>,
}

impl TimerRef {
    pub fn run_once<F>(&mut self, delay: Duration, callback: F) -> ScheduleToken
    where
        F: FnOnce(ScheduleToken) + Send + 'static,
    {
        let now = Instant::now();
        let schedule_token = ScheduleToken::new();

        let timer_entry = TimerEntry {
            schedule_token,
            scheduled_at: now + delay,
            timer_type: TimerType::OneShot(Box::new(callback)),
        };

        let _ = self.thread_tx.send(ThreadMessage::Run(timer_entry));

        schedule_token
    }

    pub(crate) fn run_recurring<F>(
        &mut self,
        delay: Duration,
        interval: Duration,
        callback: F,
    ) -> ScheduleToken
    where
        F: FnMut(ScheduleToken) + Send + 'static,
    {
        let now = Instant::now();
        let schedule_token = ScheduleToken::new();

        let timer_entry = TimerEntry {
            schedule_token,
            scheduled_at: now + delay,
            timer_type: TimerType::Recurring(interval, Box::new(callback)),
        };

        let _ = self.thread_tx.send(ThreadMessage::Run(timer_entry));

        schedule_token
    }

    pub fn cancel(&mut self, schedule_token: ScheduleToken) {
        let _ = self.thread_tx.send(ThreadMessage::Cancel(schedule_token));
    }

    pub fn shutdown(&mut self) {
        let _ = self.thread_tx.send(ThreadMessage::Shutdown);
    }
}

impl TimerHandle {
    pub fn new(timer_resolution: Duration) -> Self {
        let (thread_tx, thread_rx) = crossbeam_channel::unbounded();

        let mut timer_thread = TimerThread::new(timer_resolution, thread_rx);

        let join_handle = std::thread::Builder::new()
            .name("TonariActorTimer".to_string())
            .spawn(move || {
                timer_thread.run();
            })
            .expect("Couldn't spawn tonari actor timer thread");

        let timer_ref = TimerRef { thread_tx };

        Self { join_handle, timer_ref }
    }

    pub fn timer_ref(&self) -> TimerRef {
        self.timer_ref.clone()
    }
}

pub struct TimerThread {
    timer_resolution: Duration,
    thread_rx: Receiver<ThreadMessage>,
    entries: Vec<TimerEntry>,
}

impl TimerThread {
    fn new(timer_resolution: Duration, thread_rx: Receiver<ThreadMessage>) -> Self {
        Self { timer_resolution, thread_rx, entries: Vec::new() }
    }

    fn run(&mut self) {
        loop {
            // Try to timer entries or control messages from the channel.
            if let Ok(msg) = self.thread_rx.try_recv() {
                match msg {
                    ThreadMessage::Shutdown => {
                        break;
                    },
                    ThreadMessage::Cancel(schedule_token) => {
                        if let Some(pos) =
                            self.entries.iter().position(|e| e.schedule_token == schedule_token)
                        {
                            self.entries.swap_remove(pos);
                        }
                    },
                    ThreadMessage::Run(entry) => {
                        self.entries.push(entry);
                    },
                }
            }

            // Expire and run all timers
            let now = Instant::now();

            // We want to iterate over entries and possibly remove them as we go.
            // It's easy to do this via reverse iteration with calls to `swap_remove()`.
            // We can do this because order doesn't matter in our case.
            for i in (0..self.entries.len()).rev() {
                if now >= self.entries[i].scheduled_at {
                    let entry = self.entries.swap_remove(i);
                    let schedule_token = entry.schedule_token;

                    match entry.timer_type {
                        TimerType::OneShot(closure) => {
                            closure(schedule_token);
                        },
                        TimerType::Recurring(interval, mut closure) => {
                            closure(schedule_token);

                            let new_entry = TimerEntry {
                                schedule_token,
                                scheduled_at: entry.scheduled_at + interval,
                                timer_type: TimerType::Recurring(interval, closure),
                            };

                            self.entries.push(new_entry);
                        },
                    }
                }
            }

            spin_sleep::sleep(self.timer_resolution);
        }
    }
}
