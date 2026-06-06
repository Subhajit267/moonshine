/*
------------------------------------------------------------
Author: Subhajit Halder (adaptation from Moonshine project)
Date Last Modified: 2026-06-07
Module:      Moonshine TTS
File:        moonshine-tts.h
About:       Unified TTS public API (Kokoro & Piper backends).
             Provides batch synthesis and, as of 2026, a
             streaming callback interface for low‑latency use.
Revisions:
- 2026-06    Added ChunkCallback and synthesize_streaming()
             for per‑phoneme‑chunk streaming.
------------------------------------------------------------
*/

#ifndef MOONSHINE_TTS_MOONSHINE_TTS_H
#define MOONSHINE_TTS_MOONSHINE_TTS_H

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <functional>   /* std::function for streaming callbacks */

#include "moonshine-g2p-options.h"
#include "moonshine-tts-options.h"

namespace moonshine_tts {

/* ================= LANGUAGE SUPPORT ================= */

bool kokoro_tts_lang_supported(std::string_view lang_cli,
                               const MoonshineG2POptions& g2p_opt = {});

/* ================= TTS ENGINE ================= */

/// Unified TTS: **Kokoro** and **Piper** ONNX backends; shared ``MoonshineG2P`` where applicable.
class MoonshineTTS {
 public:
  MoonshineTTS(std::string_view language, const MoonshineTTSOptions& opt);
  MoonshineTTS(const MoonshineTTS&) = delete;
  MoonshineTTS& operator=(const MoonshineTTS&) = delete;
  MoonshineTTS(MoonshineTTS&&) noexcept;
  MoonshineTTS& operator=(MoonshineTTS&&) noexcept;
  ~MoonshineTTS();

  static constexpr int kSampleRateHz = 24000;

  /* ================= BATCH SYNTHESIS ================= */

  std::vector<float> synthesize(std::string_view text);

  /// Synthesize with per‑call option overrides.
  /// Recognized keys: ``speed``, ``normalize_audio``
  /// (legacy alias ``piper_normalize_audio``),
  /// ``output_volume`` (legacy alias ``piper_output_volume``).
  /// Other entries are ignored.
  std::vector<float> synthesize(
      std::string_view text,
      const std::vector<std::pair<std::string, std::string>>& option_overrides);

  /* ================= STREAMING API ================= */

  /*
   * Callback type for streaming synthesis.
   *
   * Parameters:
   *   phoneme_chunk – Kokoro‑normalised phoneme string for the chunk.
   *   pcm           – raw float32 PCM at 24 kHz mono, no post‑effects.
   *
   * The callback is invoked once per phoneme chunk *after* ONNX
   * inference, while the synthesis mutex is held.  It must not call
   * any other MoonshineTTS method, and must return quickly.
   */
  using ChunkCallback = std::function<void(
      const std::string& phoneme_chunk,
      std::vector<float> pcm)>;

  /*
   * Streaming variant of synthesize().
   *
   * Returns the complete waveform (with effects applied) exactly like
   * synthesize(), so existing code can migrate without changing return
   * handling.  The callback, if provided, receives each phoneme chunk
   * as soon as it is inferred.
   */
  std::vector<float> synthesize_streaming(
      std::string_view text,
      ChunkCallback on_chunk);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/* ================= WAV OUTPUT ================= */

void write_wav_mono_pcm16(const std::filesystem::path& path,
                          const std::vector<float>& samples);

/* ================= DEPENDENCY CATALOG ================= */

/// Kokoro or Piper vocoder asset keys only (no G2P), relative to ``g2p_root``.
/// With default ``MoonshineTTSOptions{}``, uses ``vocoder_engine=auto`` and default voice layout.
std::vector<std::string> moonshine_catalog_tts_vocoder_only_dependency_keys(
    std::string_view language_cli);

/// Uses ``voice`` (optional ``kokoro_`` / ``piper_`` prefix sets vocoder),
/// Piper/Kokoro file map entries, and ``g2p_options`` (e.g. Spanish narrow
/// obstruents for auto engine) like ``MoonshineTTS``.
std::vector<std::string> moonshine_catalog_tts_vocoder_only_dependency_keys(
    std::string_view language_cli, const MoonshineTTSOptions& options);

/// Union of vocoder keys across all languages in
/// ``moonshine_asset_catalog_all_registered_language_tags``.
std::vector<std::string> moonshine_catalog_all_tts_vocoder_dependency_keys_union();

/* ================= VOICE AVAILABILITY ================= */

/// One Kokoro or Piper voice id and whether the asset is available
/// (on disk or in-memory file map).
struct MoonshineTtsVoiceAvailability {
  std::string id;
  bool available = false;
};

/// All known voices for ``language_cli`` with availability, using the same
/// path layout rules as ``moonshine_catalog_tts_vocoder_only_dependency_keys``.
/// Returned ``id`` values are prefixed with ``kokoro_`` or ``piper_``.
/// When vocoder is ``auto``, Kokoro and Piper catalogs are merged (both prefixes).
/// Kokoro uses the upstream Kokoro-82M voice catalog (VOICES.md) plus any extra
/// ``*.kokorovoice`` under the resolved voices directory. Piper uses the language
/// default ONNX stem plus any ``*.onnx`` in the resolved voices directory.
/// The ``voice`` field in ``options`` does not filter the list.
std::vector<MoonshineTtsVoiceAvailability> moonshine_list_tts_voices_with_availability(
    std::string_view language_cli, const MoonshineTTSOptions& options);

}  // namespace moonshine_tts

#endif  // MOONSHINE_TTS_MOONSHINE_TTS_H
