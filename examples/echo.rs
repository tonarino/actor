//! A simple actor system that implements a working "echo" audio effect.
//!
//! Provided you have PulseAudio installed, run it as:
//!
//! parec --latency=2940 | ./target/debug/examples/echo | pacat --latency=2940
//!
//! BEWARE, running is prone to create loud audio feedback. Headphones ease that.
//!
//!            +-------+                   +--------+
//! stdin +----> Input +-+             +---> Output +---> stdout
//!            +-------+ |   +-------+ |   +--------+
//!                      +---> Mixer +-+
//!                   +------>       +--------+
//!                   |      +-------+        |
//!               +--------+              +---v----+
//!               | Delay  <--------------+ Damper |
//!               +--------+              +--------+

use actor::{Actor, Addr, Context, Recipient, System};
use anyhow::Error;
use env_logger::Env;
use log::trace;
use std::{
    convert::TryInto,
    io::{stdin, stdout, BufWriter, Read, Write},
    iter::repeat,
    sync::Arc,
};

/// One audio sample. Defaults of pacat are --format=s16ne (signed 16bit) and --chanels=2 (stereo).
type Sample = [i16; 2];

/// Number of bytes one encoded sample has. This is same as the size of its memory representation.
const SAMPLE_BYTES: usize = std::mem::size_of::<Sample>();

/// Number of samples per chunk. pacat defaults to --rate=44100, let's go for 60 chunks per second.
/// For best results, set --latency=<CHUNK_SAMPLES>*<SAMPLE_BYTES> for both parec and pacat.
const CHUNK_SAMPLES: usize = 44100 / 60;

/// A chunk of audio, an array of samples.
type Chunk = Arc<[Sample; CHUNK_SAMPLES]>;

/// Number of chunks the "Delay" actor (effect) has.
const DELAY_CHUNKS: usize = 60;

/// A chunk of samples that represents the "dry" (original, authentic) signal.
struct DryChunk(Chunk);

/// A chunk of sample that represents the "wet" (processed, edited) signal.
struct WetChunk(Chunk);

/// DryChunk converts to (unspecified) Chunk (but not the other way around).
impl From<DryChunk> for Chunk {
    fn from(orig: DryChunk) -> Self {
        orig.0
    }
}

/// WetChunk converts to (unspecified) Chunk (but not the other way around).
impl From<WetChunk> for Chunk {
    fn from(orig: WetChunk) -> Self {
        orig.0
    }
}

/// Dummy trigger for [`Input`] to read next chunk.
struct ReadNext;

fn silence_chunk() -> Chunk {
    Arc::new([[0i16; 2]; CHUNK_SAMPLES])
}

/// Actor to read and decode input stream (stdin) and produce sound [`DryChunk`]s.
struct Input {
    next: Recipient<DryChunk>,
}

impl Actor for Input {
    type Error = Error;
    type Message = ReadNext;

    fn name() -> &'static str {
        "Input"
    }

    fn handle(&mut self, context: &Context<Self>, _message: ReadNext) -> Result<(), Self::Error> {
        // Read data from stdin and decode into correct format.
        let mut bytes = [0u8; CHUNK_SAMPLES * SAMPLE_BYTES];
        stdin().read_exact(&mut bytes)?;
        let chunk_slice: Arc<[Sample]> = bytes
            .chunks(SAMPLE_BYTES)
            // default PulseAudio format is s16ne, signed 16 bit native endian.
            .map(|b| [i16::from_ne_bytes([b[0], b[1]]), i16::from_ne_bytes([b[2], b[3]])])
            .collect();
        let chunk: Chunk = chunk_slice.try_into().expect("sample count is correct");
        trace!("[Input] decoded chunk: {:?}...", &chunk[..5]);

        // Send the parsed chunk to the next actor.
        self.next.try_send(DryChunk(chunk))?;

        // Trigger a loop to read the next chunk.
        context.myself.try_send(ReadNext).map_err(Error::from)
    }
}

/// Actor to encode and write sound chunks to output stream (stdout). Consumes [`Chunk`]s,
struct Output;

impl Actor for Output {
    type Error = Error;
    type Message = Chunk;

    fn name() -> &'static str {
        "Output"
    }

    fn handle(&mut self, _context: &Context<Self>, message: Chunk) -> Result<(), Self::Error> {
        let mut buffered_stdout = BufWriter::new(stdout());
        for sample in message.iter() {
            for bytes in sample.iter().map(|s| s.to_ne_bytes()) {
                buffered_stdout.write_all(&bytes)?;
            }
        }
        buffered_stdout.flush().map_err(Error::from)
    }
}

/// A chunk that knows whether it is dry or wet.
enum MixerInput {
    /// The original signal.
    Dry(DryChunk),
    /// Signal from an effect.
    Wet(WetChunk),
}

impl From<DryChunk> for MixerInput {
    fn from(orig: DryChunk) -> Self {
        Self::Dry(orig)
    }
}

impl From<WetChunk> for MixerInput {
    fn from(orig: WetChunk) -> Self {
        Self::Wet(orig)
    }
}

/// Audio mixer actor. Mixes 2 inputs (dry, wet) together, provides 2 equal outputs.
/// Consumer either [`DryChunk`]s or [`WetChunk`]s and produces [`Chunk`]s.
struct Mixer {
    out_1: Recipient<Chunk>,
    out_2: Recipient<Chunk>,
    dry_buffer: Option<DryChunk>,
    wet_buffer: Option<WetChunk>,
}

impl Mixer {
    fn new(out_1: Recipient<Chunk>, out_2: Recipient<Chunk>) -> Self {
        // Start with buffers filled, so that output is produced right for the first message.
        Self {
            out_1,
            out_2,
            dry_buffer: Some(DryChunk(silence_chunk())),
            wet_buffer: Some(WetChunk(silence_chunk())),
        }
    }
}

impl Actor for Mixer {
    type Error = Error;
    type Message = MixerInput;

    fn name() -> &'static str {
        "Mixer"
    }

    fn handle(&mut self, _context: &Context<Self>, message: MixerInput) -> Result<(), Error> {
        // Naive implementation that simply overwrites on overflow.
        match message {
            MixerInput::Dry(chunk) => self.dry_buffer = Some(chunk),
            MixerInput::Wet(chunk) => self.wet_buffer = Some(chunk),
        }

        // if both buffers are full, mix them and send out.
        if let (Some(dry), Some(wet)) = (&self.dry_buffer, &self.wet_buffer) {
            let mixed_slice: Arc<[Sample]> = dry
                .0
                .iter()
                .zip(wet.0.iter())
                .map(|(a, b)| [a[0].saturating_add(b[0]), a[1].saturating_add(b[1])])
                .collect();
            let mixed: Chunk = mixed_slice.try_into().expect("sample count is correct");

            self.out_1.try_send(Arc::clone(&mixed))?;
            self.out_2.try_send(mixed)?;

            self.dry_buffer = None;
            self.wet_buffer = None;
        }
        Ok(())
    }
}

/// Delay audio effect actor. Technically just a fixed circular buffer.
/// Consumes [`Chunk`]s and produces [`WetChunk`]s.
struct Delay {
    next: Recipient<WetChunk>,
    buffer: Vec<Chunk>,
    index: usize,
}

impl Delay {
    fn new(next: Recipient<WetChunk>) -> Self {
        let buffer: Vec<Chunk> = repeat(silence_chunk()).take(DELAY_CHUNKS).collect();
        Self { next, buffer, index: 0 }
    }
}

impl Actor for Delay {
    type Error = Error;
    type Message = Chunk;

    fn name() -> &'static str {
        "Delay"
    }

    fn handle(&mut self, _context: &Context<Self>, message: Chunk) -> Result<(), Error> {
        self.buffer[self.index] = message;

        // Advance index, reset to zero on overflow.
        self.index = (self.index + 1) % self.buffer.len();

        // Send out the least recent chunk.
        self.next.try_send(WetChunk(Arc::clone(&self.buffer[self.index]))).map_err(Error::from)
    }
}

/// Audio damper actor. Attenuates audio level a bit. Consumes [`Chunk`]s and produces [`WetChunk`]s.
struct Damper {
    next: Recipient<WetChunk>,
}

impl Actor for Damper {
    type Error = Error;
    type Message = Chunk;

    fn name() -> &'static str {
        "Damper"
    }

    fn handle(&mut self, _context: &Context<Self>, message: Chunk) -> Result<(), Error> {
        // Halve the signal.
        let chunk_slice: Arc<[Sample]> = message.iter().map(|s| [s[0] / 2, s[1] / 2]).collect();
        let chunk: Chunk = chunk_slice.try_into().expect("sample count is correct");

        // Pass it right on.
        self.next.try_send(WetChunk(chunk)).map_err(Error::from)
    }
}

fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let mut system = System::new("Echo System");

    // Start creating actors. Because actors "point forward", start with the last one.
    // Set larger message channel capacity for Output actor for some cushion.
    let output_addr = system.prepare(Output).with_capacity(60).spawn()?;

    // Create Mixer address explicitly in order to break the circular dependency loop.
    let mixer_addr = Addr::default();

    // Delay feeds back into Mixer.
    let delay_addr = system.spawn(Delay::new(mixer_addr.recipient()))?;

    // Damper goes into Delay effect.
    let damper_addr = system.spawn(Damper { next: delay_addr.recipient() })?;

    // We can finally spawn the Mixer. Feeds into Output and Delay effect.
    system
        .prepare_fn(move || Mixer::new(output_addr.recipient(), damper_addr.recipient()))
        .with_addr(mixer_addr.clone())
        .spawn()?;

    // Input feeds into Mixer.
    let input_addr = system.spawn(Input { next: mixer_addr.recipient() })?;

    // Kick off the pipeline.
    input_addr.try_send(ReadNext)?;

    // Let the system run, block until it finishes.
    system.run().map_err(Error::from)
}
