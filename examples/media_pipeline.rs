use crate::actors::*;
use actor::{Addr, System, SystemCallbacks};
use failure::Error;

#[derive(Debug, Clone)]
pub struct MetricsSender {}

impl MetricsSender {
    pub fn new() -> Self {
        Self {}
    }
}

/// A simplistic representation of a MediaFrame, they just hold frame counters.
pub enum MediaFrame {
    Video(usize),
    Audio(usize),
}

/// A simplistic representation of an encoded MediaFrame, they just hold frame counters.
pub enum EncodedMediaFrame {
    Video(usize),
    Audio(usize),
}

mod actors {
    use crate::{EncodedMediaFrame, MediaFrame};
    use actor::{Actor, Context, Recipient};
    use failure::{bail, Error};
    use std::{thread, time::Duration};

    // Messages
    pub enum VideoCaptureMessage {
        Capture,
    }

    pub enum AudioCaptureMessage {
        Capture,
    }

    // Plumbing
    pub struct ShutdownActor;

    impl Actor for ShutdownActor {
        type Error = Error;
        type Message = ();

        fn name() -> &'static str {
            "ShutdownActor"
        }

        fn handle(
            &mut self,
            context: &Context<Self>,
            _msg: Self::Message,
        ) -> Result<(), Self::Error> {
            context.system_handle.shutdown().expect("ShutdownActor failed to shutdown system");
            Ok(())
        }
    }

    // Egress pipeline
    pub struct VideoCaptureActor {
        frame_counter: usize,
        next: Recipient<MediaFrame>,
    }

    impl VideoCaptureActor {
        pub fn new(next: Recipient<MediaFrame>) -> Self {
            Self { frame_counter: 0, next }
        }
    }

    impl Actor for VideoCaptureActor {
        type Error = Error;
        type Message = VideoCaptureMessage;

        fn name() -> &'static str {
            "VideoCaptureActor"
        }

        fn handle(
            &mut self,
            context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                VideoCaptureMessage::Capture => {
                    // Simulate a video frame capture
                    std::thread::sleep(Duration::from_millis(16));

                    self.next.send(MediaFrame::Video(self.frame_counter))?;
                    self.frame_counter += 1;

                    context.myself.send(VideoCaptureMessage::Capture)?;
                },
            }
            Ok(())
        }
    }

    pub struct VideoEncodeActor {
        next: Recipient<EncodedMediaFrame>,
    }

    impl VideoEncodeActor {
        pub fn new(next: Recipient<EncodedMediaFrame>) -> Self {
            Self { next }
        }
    }

    impl Actor for VideoEncodeActor {
        type Error = Error;
        type Message = MediaFrame;

        fn name() -> &'static str {
            "VideoEncodeActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                MediaFrame::Video(frame_counter) => {
                    // Simulate some encoding work
                    thread::sleep(Duration::from_millis(7));
                    self.next.send(EncodedMediaFrame::Video(frame_counter))?;
                },
                MediaFrame::Audio(_) => {
                    bail!("Why did you give the VideoEncodeActor an audio MediaFrame?");
                },
            }

            Ok(())
        }
    }

    pub struct AudioCaptureActor {
        frame_counter: usize,
        next: Recipient<MediaFrame>,
    }

    impl AudioCaptureActor {
        pub fn new(next: Recipient<MediaFrame>) -> Self {
            Self { frame_counter: 0, next }
        }
    }

    impl Actor for AudioCaptureActor {
        type Error = Error;
        type Message = AudioCaptureMessage;

        fn name() -> &'static str {
            "AudioCaptureActor"
        }

        fn handle(
            &mut self,
            context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                AudioCaptureMessage::Capture => {
                    // Simulate an audio frame capture
                    std::thread::sleep(Duration::from_millis(10));

                    self.next.send(MediaFrame::Audio(self.frame_counter))?;
                    self.frame_counter += 1;

                    context.myself.send(AudioCaptureMessage::Capture)?;
                },
            }

            Ok(())
        }
    }

    pub struct AudioEncodeActor {
        next: Recipient<EncodedMediaFrame>,
    }

    impl AudioEncodeActor {
        pub fn new(next: Recipient<EncodedMediaFrame>) -> Self {
            Self { next }
        }
    }

    impl Actor for AudioEncodeActor {
        type Error = Error;
        type Message = MediaFrame;

        fn name() -> &'static str {
            "AudioEncodeActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                MediaFrame::Audio(frame_counter) => {
                    // Simulate some encoding work
                    thread::sleep(Duration::from_millis(3));
                    self.next.send(EncodedMediaFrame::Audio(frame_counter))?;
                },
                MediaFrame::Video(_) => {
                    bail!("Why did you give the AudioEncodeActor a video MediaFrame?");
                },
            }

            Ok(())
        }
    }

    pub struct NetworkSenderActor {
        next: Recipient<EncodedMediaFrame>,
    }

    impl NetworkSenderActor {
        pub fn new(next: Recipient<EncodedMediaFrame>) -> Self {
            Self { next }
        }
    }

    impl Actor for NetworkSenderActor {
        type Error = Error;
        type Message = EncodedMediaFrame;

        fn name() -> &'static str {
            "NetworkSenderActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            // Add some fake packetization and network latency here
            thread::sleep(Duration::from_millis(30));
            self.next.send(message)?;

            Ok(())
        }
    }

    // Ingress pipeline
    pub struct NetworkReceiverActor {
        audio_next: Recipient<EncodedMediaFrame>,
        video_next: Recipient<EncodedMediaFrame>,
    }

    impl NetworkReceiverActor {
        pub fn new(
            audio_next: Recipient<EncodedMediaFrame>,
            video_next: Recipient<EncodedMediaFrame>,
        ) -> Self {
            Self { audio_next, video_next }
        }
    }

    impl Actor for NetworkReceiverActor {
        type Error = Error;
        type Message = EncodedMediaFrame;

        fn name() -> &'static str {
            "NetworkReceiverActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                EncodedMediaFrame::Video(_) => {
                    self.video_next.send(message)?;
                },
                EncodedMediaFrame::Audio(_) => {
                    self.audio_next.send(message)?;
                },
            }

            Ok(())
        }
    }

    pub struct VideoDecodeActor {
        next: Recipient<MediaFrame>,
    }

    impl VideoDecodeActor {
        pub fn new(next: Recipient<MediaFrame>) -> Self {
            Self { next }
        }
    }

    impl Actor for VideoDecodeActor {
        type Error = Error;
        type Message = EncodedMediaFrame;

        fn name() -> &'static str {
            "VideoDecodeActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                EncodedMediaFrame::Video(frame_counter) => {
                    self.next.send(MediaFrame::Video(frame_counter))?;
                },
                EncodedMediaFrame::Audio(_) => {
                    bail!("Why did you give the VideoDecodeActor an audio EncodedMediaFrame?");
                },
            }

            Ok(())
        }
    }

    pub struct AudioDecodeActor {
        next: Recipient<MediaFrame>,
    }

    impl AudioDecodeActor {
        pub fn new(next: Recipient<MediaFrame>) -> Self {
            Self { next }
        }
    }

    impl Actor for AudioDecodeActor {
        type Error = Error;
        type Message = EncodedMediaFrame;

        fn name() -> &'static str {
            "AudioDecodeActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                EncodedMediaFrame::Audio(frame_counter) => {
                    self.next.send(MediaFrame::Audio(frame_counter))?;
                },
                EncodedMediaFrame::Video(_) => {
                    bail!("Why did you give the AudioDecodeActor a video EncodedMediaFrame?");
                },
            }

            Ok(())
        }
    }

    pub struct AudioPlaybackActor {}

    impl AudioPlaybackActor {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl Actor for AudioPlaybackActor {
        type Error = Error;
        type Message = MediaFrame;

        fn name() -> &'static str {
            "AudioPlaybackActor"
        }

        fn handle(
            &mut self,
            _context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                MediaFrame::Audio(frame_counter) => {
                    println!("🔊 Playing back audio frame {}", frame_counter);
                },
                MediaFrame::Video(_) => {
                    bail!("Why did you give the AudioPlaybackActor a video MediaFrame?");
                },
            }

            Ok(())
        }
    }

    pub struct VideoDisplayActor {}

    impl VideoDisplayActor {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl Actor for VideoDisplayActor {
        type Error = Error;
        type Message = MediaFrame;

        fn name() -> &'static str {
            "VideoDisplayActor"
        }

        fn handle(
            &mut self,
            context: &Context<Self>,
            message: Self::Message,
        ) -> Result<(), Self::Error> {
            match message {
                MediaFrame::Video(frame_counter) => {
                    println!("🖼 Display video frame {}", frame_counter);

                    if frame_counter >= 360 {
                        println!(
                            "We've received 360 video frames, shutting down the actor system!"
                        );
                        let _ = context.system_handle.shutdown();
                    }
                },
                MediaFrame::Audio(_) => {
                    bail!("Why did you give the VideoDisplayActor an audio MediaFrame?");
                },
            }

            Ok(())
        }
    }
}

impl actor::MetricsHandler for MetricsSender {
    // This function is called at each actor thread in between
    // every handle() call
    fn handle(&mut self, _actor_name: &'static str, _metrics: actor::Metrics) {
        // Do stuff with the metrics from the actor system here
    }
}

fn main() -> Result<(), Error> {
    simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Debug).init().unwrap();

    let system_callbacks = SystemCallbacks {
        preshutdown: Some(Box::new(move || {
            println!("The actor system is stopping, this is the preshutdown hook");
            Ok(())
        })),
        ..SystemCallbacks::default()
    };

    let mut system: System<MetricsSender> = System::with_callbacks("main", system_callbacks);

    // TODO - Add some extra "config" actors to adjust things like video capture exposure,
    //        or playback volume.

    // Handle Ctrl-C
    let shutdown_addr = system.spawn(ShutdownActor {})?;
    ctrlc::set_handler(move || {
        shutdown_addr.send(()).expect("failed to send shutdown message");
    })?;

    // Wire up the actors
    let display_addr = Addr::default();

    // Receiving side
    let audio_playback_actor = system.spawn(AudioPlaybackActor::new())?;

    let video_decode_addr = system.spawn(VideoDecodeActor::new(display_addr.recipient()))?;
    let audio_decode_addr =
        system.spawn(AudioDecodeActor::new(audio_playback_actor.recipient()))?;

    let network_receiver_addr = system.spawn(NetworkReceiverActor::new(
        audio_decode_addr.recipient(),
        video_decode_addr.recipient(),
    ))?;

    // Sending side
    let network_sender_addr =
        system.spawn(NetworkSenderActor::new(network_receiver_addr.recipient()))?;

    let video_encode_addr = system.spawn(VideoEncodeActor::new(network_sender_addr.recipient()))?;
    let audio_encode_addr = system.spawn(AudioEncodeActor::new(network_sender_addr.recipient()))?;

    let video_capture_addr = system.spawn(VideoCaptureActor::new(video_encode_addr.recipient()))?;
    let audio_capture_addr = system.spawn(AudioCaptureActor::new(audio_encode_addr.recipient()))?;

    // Kick off the pipeline
    audio_capture_addr.send(AudioCaptureMessage::Capture)?;
    video_capture_addr.send(VideoCaptureMessage::Capture)?;

    // The display actor may spawn an OS window which in some cases must run
    // on the main application thread.
    let display_actor = VideoDisplayActor::new();
    system.run_on_main(display_actor, display_addr)?;

    Ok(())
}
