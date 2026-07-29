//! AudioStream: play a WAV file into a live Bus at the pace it would be
//! spoken.
//!
//! The realtime producer half of an audio pipeline: reads the dropped
//! file's bytes, parses the WAV header (16-bit PCM mono only, loud on
//! anything else), and sends one raw-PCM frame per [`CHUNK_MS`] of
//! audio, paced by a wall-clock interval, onto an EPHEMERAL BYTES bus
//! (audio is a live byte stream: no base64, no JSON envelope; the
//! journal keeps windowed rollups, not the bytes). The stream-level
//! facts ride the bus's creator metadata, so a consumer knows the
//! format before the first frame:
//!
//! ```json
//! { "sample_rate": 16000, "encoding": "pcm_s16le" }
//! ```
//!
//! The bus closes when the file ends (or on any error: the handle from
//! `ctx.open_bus` closes on every exit), and that close IS the
//! end-of-stream signal a reader waits on.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;

use weft::bus::{BusOptions, BusPayloadKind};
use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{node_error, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct AudioStreamNode;

/// One frame of audio per message. 250ms keeps the message rate low
/// (4/s) while staying comfortably "live" for a realtime consumer.
const CHUNK_MS: u64 = 250;

/// The bus's in-RAM window, in frames: how far behind a consumer may
/// attach and still catch the start (it covers a consumer's own startup:
/// opening its connection, dialing its session).
const WINDOW_CHUNKS: usize = 256;

/// A parsed WAV: the PCM bytes and their rate. Pure parser (header
/// walking only), loud on every shape this streamer does not speak.
pub fn parse_wav(bytes: &[u8]) -> Result<(u32, Vec<u8>), String> {
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return Err("not a WAV file (no RIFF/WAVE header)".into());
    }
    let u16_at = |o: usize| u16::from_le_bytes([bytes[o], bytes[o + 1]]);
    let u32_at = |o: usize| u32::from_le_bytes([bytes[o], bytes[o + 1], bytes[o + 2], bytes[o + 3]]);
    let mut pos = 12;
    let mut format: Option<(u16, u16, u32, u16)> = None;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let size = u32_at(pos + 4) as usize;
        let body = pos + 8;
        if body + size > bytes.len() {
            return Err(format!(
                "truncated WAV: chunk '{}' declares {size} bytes but only {} remain; \
                 re-encode it (e.g. `ffmpeg -i in.wav -ar 16000 -ac 1 out.wav`)",
                String::from_utf8_lossy(id),
                bytes.len() - body
            ));
        }
        match id {
            b"fmt " if size >= 16 => {
                let mut fmt = u16_at(body); // audio format (1 = PCM)
                // WAVE_FORMAT_EXTENSIBLE (0xFFFE): a wrapper ffmpeg and
                // Windows tooling emit even for plain PCM. The real
                // format tag is the first two bytes of the sub-format
                // GUID at body offset +24.
                if fmt == 0xFFFE {
                    if size < 40 {
                        return Err(format!(
                            "extensible WAV fmt chunk too short ({size} bytes, needs 40); \
                             re-encode it (e.g. `ffmpeg -i in.wav -ar 16000 -ac 1 out.wav`)"
                        ));
                    }
                    fmt = u16_at(body + 24);
                }
                format = Some((
                    fmt,
                    u16_at(body + 2), // channels
                    u32_at(body + 4), // sample rate
                    u16_at(body + 14), // bits per sample
                ));
            }
            b"data" => {
                let Some((fmt, channels, rate, bits)) = format else {
                    return Err("WAV data chunk before its fmt chunk".into());
                };
                if fmt != 1 || bits != 16 {
                    return Err(format!(
                        "only 16-bit PCM WAV is streamable (this file: format {fmt}, \
                         {bits}-bit); convert it (e.g. `ffmpeg -i in -ar 16000 -ac 1 out.wav`)"
                    ));
                }
                if channels != 1 {
                    return Err(format!(
                        "only mono WAV is streamable (this file has {channels} channels); \
                         convert it (e.g. `ffmpeg -i in -ar 16000 -ac 1 out.wav`)"
                    ));
                }
                if size == 0 {
                    return Err(
                        "the WAV's audio data is empty (a data chunk with a header but no \
                         samples); there is nothing to stream"
                            .into(),
                    );
                }
                return Ok((rate, bytes[body..body + size].to_vec()));
            }
            _ => {}
        }
        // Chunks are word-aligned: an odd size is padded by one byte.
        pos = body + size + (size % 2);
    }
    Err("WAV file has no data chunk".into())
}

/// Bytes per [`CHUNK_MS`] frame at `rate`. Counts whole 16-bit samples
/// first, bytes second, so a chunk can never end mid-sample (a bytes-
/// first formula rounds odd at e.g. 22050 Hz and every chunk boundary
/// would split a sample, turning the stream to garbage).
fn chunk_bytes(rate: u32) -> usize {
    let samples_per_chunk = rate as u64 * CHUNK_MS / 1000;
    (samples_per_chunk * 2) as usize
}

#[async_trait]
impl Node for AudioStreamNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let file = ctx.inputs.get("audio")?;
        let (_meta, bytes) = ctx.storage(StorageScope::Project).get_bytes(&file).await?;
        let (rate, pcm) = parse_wav(&bytes).map_err(node_error)?;

        let bus = ctx
            .open_bus(
                "stream",
                BusOptions {
                    ephemeral: true,
                    window: Some(WINDOW_CHUNKS),
                    payload: BusPayloadKind::Bytes,
                    meta: json!({ "sample_rate": rate, "encoding": "pcm_s16le" }),
                    ..Default::default()
                },
                "audio_source",
            )
            .await?;

        // A wall-clock interval, not a per-chunk sleep: ticks that fall
        // behind (a slow send) catch up instead of drifting.
        let mut pace = tokio::time::interval(Duration::from_millis(CHUNK_MS));
        for chunk in pcm.chunks(chunk_bytes(rate)) {
            pace.tick().await;
            bus.send_bytes("audio", chunk.to_vec()).node_err("stream an audio frame")?;
        }
        drop(bus); // the close IS the end-of-stream signal

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal in-RAM WAV: 16-bit mono PCM at `rate` with `samples`.
    fn wav(rate: u32, channels: u16, bits: u16, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data.len() as u32).to_le_bytes());
        out.extend_from_slice(b"WAVE");
        out.extend_from_slice(b"fmt ");
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
        out.extend_from_slice(&channels.to_le_bytes());
        out.extend_from_slice(&rate.to_le_bytes());
        out.extend_from_slice(&(rate * u32::from(channels) * u32::from(bits) / 8).to_le_bytes());
        out.extend_from_slice(&(channels * bits / 8).to_le_bytes());
        out.extend_from_slice(&bits.to_le_bytes());
        out.extend_from_slice(b"data");
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }

    /// An in-RAM WAVE_FORMAT_EXTENSIBLE WAV: a 40-byte fmt chunk whose
    /// sub-format GUID starts with `sub_format` (the real format tag).
    fn extensible_wav(rate: u32, sub_format: u16, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(60 + data.len() as u32).to_le_bytes());
        out.extend_from_slice(b"WAVE");
        out.extend_from_slice(b"fmt ");
        out.extend_from_slice(&40u32.to_le_bytes());
        out.extend_from_slice(&0xFFFEu16.to_le_bytes()); // extensible
        out.extend_from_slice(&1u16.to_le_bytes()); // mono
        out.extend_from_slice(&rate.to_le_bytes());
        out.extend_from_slice(&(rate * 2).to_le_bytes()); // byte rate
        out.extend_from_slice(&2u16.to_le_bytes()); // block align
        out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        out.extend_from_slice(&22u16.to_le_bytes()); // extension size
        out.extend_from_slice(&16u16.to_le_bytes()); // valid bits
        out.extend_from_slice(&0u32.to_le_bytes()); // channel mask
        out.extend_from_slice(&sub_format.to_le_bytes()); // GUID: real tag
        out.extend_from_slice(&[0u8; 14]); // GUID: remainder
        out.extend_from_slice(b"data");
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
        out
    }

    #[test]
    fn a_valid_wav_parses_to_its_pcm() {
        let (rate, pcm) = parse_wav(&wav(16_000, 1, 16, &[1, 2, 3, 4])).unwrap();
        assert_eq!(rate, 16_000);
        assert_eq!(pcm, vec![1, 2, 3, 4]);
    }

    #[test]
    fn unstreamable_shapes_refuse_loudly() {
        assert!(parse_wav(b"not audio").unwrap_err().contains("RIFF"));
        let stereo = parse_wav(&wav(16_000, 2, 16, &[0; 8])).unwrap_err();
        assert!(stereo.contains("mono"), "{stereo}");
        let eight_bit = parse_wav(&wav(16_000, 1, 8, &[0; 8])).unwrap_err();
        assert!(eight_bit.contains("16-bit"), "{eight_bit}");
        let empty = parse_wav(&wav(16_000, 1, 16, &[])).unwrap_err();
        assert!(empty.contains("empty"), "{empty}");
    }

    #[test]
    fn an_extensible_pcm_wav_parses() {
        let (rate, pcm) = parse_wav(&extensible_wav(22_050, 1, &[9, 8, 7, 6])).unwrap();
        assert_eq!(rate, 22_050);
        assert_eq!(pcm, vec![9, 8, 7, 6]);
    }

    #[test]
    fn an_extensible_non_pcm_wav_refuses_loudly() {
        // Sub-format 3 is IEEE float, not PCM.
        let err = parse_wav(&extensible_wav(22_050, 3, &[0; 4])).unwrap_err();
        assert!(err.contains("16-bit PCM"), "{err}");
    }

    #[test]
    fn a_truncated_data_chunk_refuses_with_the_remedy() {
        // Declare 100 data bytes but supply only 4.
        let mut bytes = wav(16_000, 1, 16, &[1, 2, 3, 4]);
        let data_size_at = bytes.len() - 8;
        bytes[data_size_at..data_size_at + 4].copy_from_slice(&100u32.to_le_bytes());
        let err = parse_wav(&bytes).unwrap_err();
        assert!(err.contains("'data'"), "{err}");
        assert!(err.contains("100"), "{err}");
        assert!(err.contains("4 remain"), "{err}");
        assert!(err.contains("ffmpeg"), "{err}");
    }

    #[test]
    fn chunk_bytes_never_splits_a_sample() {
        // The rates the ElevenLabs realtime endpoint accepts; restated
        // from SUPPORTED_RATES in catalog/ai/elevenlabs/transcribe/mod.rs
        // (a sibling node, not importable from here).
        for rate in [8_000, 16_000, 22_050, 24_000, 44_100, 48_000] {
            assert_eq!(chunk_bytes(rate) % 2, 0, "odd chunk at {rate} Hz");
        }
    }
}
