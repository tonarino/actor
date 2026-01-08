//! A realistic but toy example of using 2 async and 1 sync actor:
//! - the [`Crawler`] async actor concurrently fetches websites, emits chunks of their data to:
//! - the [`Sorter`] async actor, which "sorts" the chunks from all websites by delaying each chunk
//!   according to the value of its first character.
//! - the [`Collector`] sync actor prints chunks prefixed by the website as they come.

use anyhow::{Error, Result};
use env_logger::Env;
use futures_util::{StreamExt, TryStreamExt, future::join_all, stream};
use std::{mem, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use tonari_actor::{Actor, AsyncActor, BareContext, Context, Recipient, System};

enum CrawlerMessage {
    Crawl { hosts: Vec<String> },
    Finish,
}

/// An actor that concurrently fetches pages using bare HTTP, and passes chunks of them to the
/// next actor.
struct Crawler {
    sorter: Recipient<SorterMessage>,
}

impl AsyncActor for Crawler {
    type Error = Error;
    type Message = CrawlerMessage;

    async fn handle(
        &mut self,
        _context: &BareContext<CrawlerMessage>,
        message: CrawlerMessage,
    ) -> Result<()> {
        match message {
            CrawlerMessage::Crawl { hosts } => {
                // Future combinators are sufficient for concurrent operation within a single actor
                // message. As we don't spawn an async task, we only start handling the _next_
                // message once we fully process this current message though.
                let stream = stream::iter(hosts);
                let limit = None;
                stream
                    .map(Ok)
                    .try_for_each_concurrent(limit, |host| crawl_host(host, self.sorter.clone()))
                    .await?;
            },
            CrawlerMessage::Finish => {
                log::debug!("Crawler finished, propagating to Sorter...");
                self.sorter.send(SorterMessage::Finish)?;
            },
        }

        Ok(())
    }
}

/// Fetches a page from `host` and sends the response to `sorter`, in chunks.
async fn crawl_host(host: String, sorter: Recipient<SorterMessage>) -> Result<()> {
    log::debug!("Connecting to {host}...");
    let mut stream = TcpStream::connect(format!("{}:80", host)).await?;
    log::debug!("Connected to {host}");
    stream.write_all(b"GET / HTTP/1.0\r\n\r\n").await?;
    log::debug!("HTTP request sent to {host}");

    // Read up to 5 chunks, each up to 100 bytes long.
    for i in 0..5 {
        let mut buffer = vec![0u8; 100];
        let timeout_result = timeout(Duration::from_secs(3), stream.read(&mut buffer)).await;
        let Ok(read_result) = timeout_result else {
            sorter.send(SorterMessage::Chunk(Chunk {
                host: host.clone(),
                text: format!("<timed out reading chunk {i}>"),
            }))?;
            continue;
        };

        let read_bytes = read_result?;
        if read_bytes == 0 {
            break;
        }

        let text = String::from_utf8_lossy(&buffer[..read_bytes]).into_owned();
        let chunk = Chunk { host: host.clone(), text };
        sorter.send(SorterMessage::Chunk(chunk))?;
    }

    log::debug!("Closing connection to {host}.");
    Ok(())
}

#[derive(Debug)]
struct Chunk {
    host: String,
    text: String,
}

enum SorterMessage {
    Chunk(Chunk),
    Finish,
}

/// An actor that sorts incoming web page chunks (mostly) alphabetically using a crude algorithm:
/// it delays processing of each chunk depending on its first character value.
struct Sorter {
    collector: Recipient<Chunk>,
    pending_tasks: Vec<JoinHandle<Result<(), Error>>>,
    first_message_reception: Option<Instant>,
}

impl Sorter {
    fn new(collector: Recipient<Chunk>) -> Self {
        Self { collector, pending_tasks: vec![], first_message_reception: None }
    }
}

impl AsyncActor for Sorter {
    type Error = Error;
    type Message = SorterMessage;

    const DEFAULT_CAPACITY_NORMAL: usize = 50;

    async fn handle(
        &mut self,
        context: &BareContext<SorterMessage>,
        message: SorterMessage,
    ) -> Result<()> {
        match message {
            SorterMessage::Chunk(chunk) => {
                let first_message_reception =
                    *self.first_message_reception.get_or_insert(Instant::now());

                // Because we want to process many _messages_ concurrently, we must use tokio::spawn().
                let collector = self.collector.clone();
                let task = tokio::spawn(async move {
                    let first_char = chunk.text.chars().next();
                    let first_char_value = first_char.map_or(0, u64::from);
                    let delay = Duration::from_millis(first_char_value * 20);
                    sleep_until(first_message_reception + delay).await;
                    collector.send(chunk)?;

                    Ok(())
                });
                self.pending_tasks.push(task);
            },
            SorterMessage::Finish => {
                // Even though everything is concurrent, the message delivery order is still
                // guaranteed by the actor system. By the time we receive the `Finish` message,
                // it is guaranteed that we won't receive any new `Chunk` messages sent prior to
                // that, thus `self.tasks` is complete at this point.
                let initial_task_count = self.pending_tasks.len();
                self.pending_tasks.retain(|task| !task.is_finished());
                let pending_task_count = self.pending_tasks.len();
                log::debug!(
                    "Waiting for {pending_task_count} pending tasks in Sorter before shutting \
                     down. {} tasks already finished.",
                    initial_task_count - pending_task_count
                );

                let results = join_all(mem::take(&mut self.pending_tasks)).await;
                // Propagate errors from the tasks:
                results.into_iter().try_for_each(|result| result.map_err(Error::from).flatten())?;

                log::debug!("Tasks finished, shutting down the actor system.");
                context.system_handle.shutdown()?;
            },
        }

        Ok(())
    }
}

/// A simple sync actor that collects page chunks as they arrive and prints them.
struct Collector;

impl Actor for Collector {
    type Context = Context<Chunk>;
    type Error = Error;
    type Message = Chunk;

    fn handle(&mut self, _context: &mut Context<Chunk>, message: Chunk) -> Result<()> {
        let Chunk { host, text } = message;

        println!("{host:>20}: {text:?}");
        Ok(())
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
    let mut system = System::new("async actors example");

    let collector = system.spawn(Collector)?;
    let sorter = system.spawn_async(Sorter::new(collector.recipient()))?;
    let crawler = system.spawn_async(Crawler { sorter: sorter.recipient() })?;

    let hosts = vec![
        "google.com".to_string(),
        "captive.apple.com".to_string(),
        "httpforever.com".to_string(),
    ];
    crawler.send(CrawlerMessage::Crawl { hosts })?;
    crawler.send(CrawlerMessage::Finish)?;

    system.run()?;
    Ok(())
}
