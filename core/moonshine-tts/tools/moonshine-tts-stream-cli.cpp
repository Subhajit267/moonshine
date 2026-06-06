/*
------------------------------------------------------------
Author: Subhajit Halder
Date Created: 2026-06-04
Date Last Modified: 2026-06-07

Project: Moonshine Streaming TTS Extension
Module: Moonshine Streaming TTS
File: moonshine-tts-stream-cli.cpp

About:
New file – streaming TTS daemon.

Accepts text over a Unix domain socket, synthesises
speech using MoonshineTTS::synthesize_streaming(),
streams real-time PCM audio to PipeWire, and publishes
phoneme chunks over a dedicated Unix domain socket.

Architecture:
                 Text Socket
                      |
                      v
       MoonshineTTS::synthesize_streaming()
                      |
        +-------------+-------------+
        |                           |
        v                           v
   Audio Ring Buffer          Phoneme Queue
        |                           |
        v                           v
 PipeWire Audio Output       Phoneme Socket
        |                           |
        v                           v
  Audio Playback           JSON Phoneme Stream

Thread Model:
- Main Thread
  Argument parsing, socket creation, text client handling

- Synthesis Thread
  Text processing, streaming synthesis, audio generation,
  phoneme event generation

- Phoneme Writer Thread
  JSON phoneme event output

- Phoneme Accept Thread
  Phoneme listener connection management

- PipeWire Thread (optional)
  Real-time audio playback

Features:
- Unix domain socket text input
- Unix domain socket phoneme output
- Streaming synthesis callback API
- Lock-free SPSC audio ring buffer
- PipeWire audio playback
- JSON phoneme event streaming

Revisions:
- 2026-06-04  Initial streaming daemon implementation
- 2026-06-06  Added PipeWire audio output and phoneme
              streaming support
- 2026-06-07  Increased audio length duration to 
              roughly 20secs
------------------------------------------------------------
*/
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <cerrno>
#include <cstddef>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#ifdef MOONSHINE_WITH_PIPEWIRE
#include <pipewire/pipewire.h>
#include <spa/param/audio/format-utils.h>
#endif

#include "moonshine-tts.h"
#include "spsc_ringbuffer.h"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr size_t kAudioRingCapacity  = 524288;  // power of two, ~21.8 s @ 24 kHz
static constexpr size_t kPipeWireLeftoverMax = 8192;   // generous upper bound for leftover samples
static constexpr size_t kMaxMessageSize      = 65536;

// ---------------------------------------------------------------------------
// Generic thread‑safe queue (for text and phoneme strings — not real‑time)
// ---------------------------------------------------------------------------
template <typename T>
class SafeQueue {
public:
    void push(T value) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            q_.push_back(std::move(value));
        }
        cv_.notify_one();
    }

    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this] { return !q_.empty() || closed_; });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop_front();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            closed_ = true;
        }
        cv_.notify_all();
    }

private:
    std::deque<T>           q_;
    std::mutex              mu_;
    std::condition_variable cv_;
    bool                    closed_ = false;
};

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------
static std::atomic<bool> g_stop{false};

static std::unique_ptr<moonshine_tts::MoonshineTTS> g_tts;

// Queues and ring buffer
static SafeQueue<std::string>                           g_text_queue;
static SafeQueue<std::pair<int, std::string>>           g_phoneme_queue;  // (seq, phoneme_chunk)
static SPSCRingBuffer<int16_t, kAudioRingCapacity>      g_audio_ring;

// Phoneme client file descriptor (atomic for safe cross‑thread access)
static std::atomic<int> g_phoneme_fd{-1};

// Listening socket file descriptors (for shutdown)
static int g_text_server_fd    = -1;
static int g_phoneme_server_fd = -1;

// Socket paths (for unlinking on exit)
static std::string g_text_sock_path;
static std::string g_phoneme_sock_path;

// PipeWire globals (if enabled)
#ifdef MOONSHINE_WITH_PIPEWIRE
struct PipeWireContext {
    struct pw_main_loop* loop          = nullptr;
    struct pw_stream*    stream        = nullptr;
    int16_t              leftover[kPipeWireLeftoverMax];
    size_t               leftover_len  = 0;
};
static PipeWireContext g_pw;
static bool g_pipewire_enabled = true;  // may be set to false by --no-pipewire
#endif

// ---------------------------------------------------------------------------
// JSON string escaping
// ---------------------------------------------------------------------------
static std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 4);
    for (unsigned char c : s) {
        if (c == '"')       out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else                out += static_cast<char>(c);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Unix socket helpers
// ---------------------------------------------------------------------------
static int make_unix_server(const std::string& path) {
    unlink(path.c_str());

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "[sock] socket() failed: " << strerror(errno) << "\n";
        return -1;
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "[sock] bind(" << path << ") failed: " << strerror(errno) << "\n";
        close(fd);
        return -1;
    }

    if (listen(fd, 4) < 0) {
        std::cerr << "[sock] listen() failed: " << strerror(errno) << "\n";
        close(fd);
        return -1;
    }

    return fd;
}

static bool write_all(int fd, const char* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t ret = write(fd, data + written, len - written);
        if (ret <= 0) {
            if (ret < 0 && errno == EINTR) continue;
            return false;
        }
        written += static_cast<size_t>(ret);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Signal handler
// ---------------------------------------------------------------------------
static void signal_handler(int) {
    g_stop.store(true, std::memory_order_relaxed);
    g_text_queue.close();
#ifdef MOONSHINE_WITH_PIPEWIRE
    if (g_pw.loop) pw_main_loop_quit(g_pw.loop);
#endif
    // Close server sockets to unblock accept() calls.
    if (g_text_server_fd >= 0) {
        close(g_text_server_fd);
        g_text_server_fd = -1;
    }
    if (g_phoneme_server_fd >= 0) {
        close(g_phoneme_server_fd);
        g_phoneme_server_fd = -1;
    }
}

// ---------------------------------------------------------------------------
// Thread functions — forward declarations
// ---------------------------------------------------------------------------
static void synthesis_thread_func();
static void phoneme_writer_thread_func();
static void phoneme_accept_thread_func(int server_fd);
#ifdef MOONSHINE_WITH_PIPEWIRE
static void pipewire_audio_thread_func();
#endif

// ---------------------------------------------------------------------------
// PipeWire real‑time process callback
//   - NO heap allocations, NO locks, NO blocking calls.
// ---------------------------------------------------------------------------
#ifdef MOONSHINE_WITH_PIPEWIRE
static void on_pipewire_process(void* userdata) {
    auto* ctx = static_cast<PipeWireContext*>(userdata);

    struct pw_buffer* pw_buf = pw_stream_dequeue_buffer(ctx->stream);
    if (!pw_buf) return;

    struct spa_buffer* spa = pw_buf->buffer;
    int16_t* dst = static_cast<int16_t*>(spa->datas[0].data);
    if (!dst) {
        pw_stream_queue_buffer(ctx->stream, pw_buf);
        return;
    }

    const uint32_t max_samples = spa->datas[0].maxsize / sizeof(int16_t);
    uint32_t filled = 0;

    // 1. Drain leftover samples from the previous callback.
    if (ctx->leftover_len > 0) {
        uint32_t to_copy = std::min<uint32_t>(max_samples, ctx->leftover_len);
        std::memcpy(dst, ctx->leftover, to_copy * sizeof(int16_t));
        filled += to_copy;
        ctx->leftover_len -= to_copy;
        if (ctx->leftover_len > 0) {
            std::memmove(ctx->leftover,
                         ctx->leftover + to_copy,
                         ctx->leftover_len * sizeof(int16_t));
        }
    }

    // 2. Pull fresh samples from the ring buffer.
    while (filled < max_samples) {
        int16_t tmp[512];
        size_t n = g_audio_ring.pop(tmp, std::min<size_t>(max_samples - filled, 512));
        if (n == 0) break;

        if (filled + n <= max_samples) {
            std::memcpy(dst + filled, tmp, n * sizeof(int16_t));
            filled += static_cast<uint32_t>(n);
        } else {
            uint32_t fit = max_samples - filled;
            std::memcpy(dst + filled, tmp, fit * sizeof(int16_t));
            filled = max_samples;

            size_t remaining = n - fit;
            if (remaining <= kPipeWireLeftoverMax) {
                std::memcpy(ctx->leftover, tmp + fit, remaining * sizeof(int16_t));
                ctx->leftover_len = remaining;
            } else {
                // Extremely unlikely overflow — discard.
                ctx->leftover_len = 0;
            }
        }
    }

    // 3. Pad with silence if we still have room.
    if (filled < max_samples) {
        std::memset(dst + filled, 0, (max_samples - filled) * sizeof(int16_t));
    }

    spa->datas[0].chunk->offset = 0;
    spa->datas[0].chunk->stride = sizeof(int16_t);
    spa->datas[0].chunk->size   = max_samples * sizeof(int16_t);

    pw_stream_queue_buffer(ctx->stream, pw_buf);
}
static void on_pipewire_state_changed(void* userdata,
                                      enum pw_stream_state old,
                                      enum pw_stream_state state,
                                      const char* error) {
    (void)userdata;
    fprintf(stderr,
            "[pw] state changed: %s -> %s (error: %s)\n",
            pw_stream_state_as_string(old),
            pw_stream_state_as_string(state),
            error ? error : "none");
}
#endif

// ---------------------------------------------------------------------------
// PipeWire audio thread
// ---------------------------------------------------------------------------
#ifdef MOONSHINE_WITH_PIPEWIRE
static void pipewire_audio_thread_func() {
    // Pre‑initialise leftover buffer length.
    g_pw.leftover_len = 0;

    pw_init(nullptr, nullptr);

    g_pw.loop = pw_main_loop_new(nullptr);
    if (!g_pw.loop) {
        std::cerr << "[pipewire] pw_main_loop_new failed\n";
        return;
    }

    struct pw_properties* props = pw_properties_new(
        PW_KEY_MEDIA_TYPE,     "Audio",
        PW_KEY_MEDIA_CATEGORY, "Playback",
        PW_KEY_MEDIA_ROLE,     "Speech",
        PW_KEY_APP_NAME,       "moonshine-tts-stream",
        nullptr);

    // Build stream events struct — C++17 aggregate initialisation, no designated initializers.
    struct pw_stream_events events;
    events.version       = PW_VERSION_STREAM_EVENTS;
    events.process       = on_pipewire_process;
    events.destroy       = nullptr;
    events.param_changed = nullptr;
    events.io_changed    = nullptr;
    events.drained       = nullptr;
    events.command       = nullptr;
    events.trigger_done  = nullptr;
    events.state_changed = on_pipewire_state_changed;
    g_pw.stream = pw_stream_new_simple(
        pw_main_loop_get_loop(g_pw.loop),
        "moonshine-tts",
        props,  // stream takes ownership
        &events,
        &g_pw);

    if (!g_pw.stream) {
        std::cerr << "[pipewire] pw_stream_new_simple failed\n";
        pw_main_loop_destroy(g_pw.loop);
        g_pw.loop = nullptr;
        pw_deinit();
        return;
    }

    // Build audio format: S16, 24000 Hz, mono.  No designated initializers.
    uint8_t pod_buffer[256];
    struct spa_pod_builder b = SPA_POD_BUILDER_INIT(pod_buffer, sizeof(pod_buffer));
    const struct spa_pod* params[1];

    struct spa_audio_info_raw raw_info;
    raw_info.format   = SPA_AUDIO_FORMAT_S16;
    raw_info.rate     = static_cast<uint32_t>(moonshine_tts::MoonshineTTS::kSampleRateHz);
    raw_info.channels = 1;

    params[0] = spa_format_audio_raw_build(&b, SPA_PARAM_EnumFormat, &raw_info);

    int ret = pw_stream_connect(
        g_pw.stream,
        PW_DIRECTION_OUTPUT,
        PW_ID_ANY,
        static_cast<pw_stream_flags>(
            PW_STREAM_FLAG_AUTOCONNECT |
            PW_STREAM_FLAG_MAP_BUFFERS |
            PW_STREAM_FLAG_RT_PROCESS),
        params, 1);

    if (ret < 0) {
        std::cerr << "[pipewire] pw_stream_connect failed: " << strerror(-ret) << "\n";
        pw_stream_destroy(g_pw.stream);
        g_pw.stream = nullptr;
        pw_main_loop_destroy(g_pw.loop);
        g_pw.loop = nullptr;
        pw_deinit();
        return;
    }
    
    pw_stream_set_active(g_pw.stream, true);
    
    std::cerr << "[pipewire] stream started (S16, "
              << moonshine_tts::MoonshineTTS::kSampleRateHz << " Hz, mono)\n";

    // Block until pw_main_loop_quit() is called.
    pw_main_loop_run(g_pw.loop);

    // Shutdown.
    pw_stream_destroy(g_pw.stream);
    g_pw.stream = nullptr;
    pw_main_loop_destroy(g_pw.loop);
    g_pw.loop = nullptr;
    pw_deinit();
    std::cerr << "[pipewire] shutdown complete\n";
}

#endif

// ---------------------------------------------------------------------------
// Synthesis thread
// ---------------------------------------------------------------------------
static void synthesis_thread_func() {
    std::string text;
    int seq = 0;  // monotonic chunk index across all utterances

    while (g_text_queue.pop(text)) {
        if (text.empty()) continue;

        try {
            g_tts->synthesize_streaming(text,
                [&](const std::string& phoneme_chunk,
                    std::vector<float> pcm_float)
                {
                    // Convert float PCM → int16_t.
                    std::vector<int16_t> pcm_int16(pcm_float.size());
                    for (size_t i = 0; i < pcm_float.size(); ++i) {
                        float x = pcm_float[i];
                        if (x >  1.0f) x =  1.0f;
                        if (x < -1.0f) x = -1.0f;
                        pcm_int16[i] = static_cast<int16_t>(x * 32767.0f);
                    }

                    // Push audio into lock‑free ring buffer.
                    g_audio_ring.push(pcm_int16.data(), pcm_int16.size());

                    // Push phoneme chunk for the IPC channel.
                    g_phoneme_queue.push({seq, phoneme_chunk});
                    ++seq;
                });
        } catch (const std::exception& e) {
            std::cerr << "[synth] error: " << e.what() << "\n";
        }
    }
}

// ---------------------------------------------------------------------------
// Phoneme writer thread
// ---------------------------------------------------------------------------
static void phoneme_writer_thread_func() {
    std::pair<int, std::string> item;
    while (g_phoneme_queue.pop(item)) {
        const int seq = item.first;
        const std::string& chunk = item.second;

        // Build JSON line: {"seq":N,"chunk":"..."}
        std::string line = "{\"seq\":" + std::to_string(seq)
                         + ",\"chunk\":\"" + json_escape(chunk) + "\"}\n";

        int fd = g_phoneme_fd.load(std::memory_order_acquire);
        if (fd < 0) continue;  // no client connected

        if (!write_all(fd, line.data(), line.size())) {
            // Write failed — client disconnected.
            int old = g_phoneme_fd.exchange(-1, std::memory_order_acq_rel);
            if (old >= 0) close(old);
            std::cerr << "[phoneme] client disconnected\n";
        }
    }
}

// ---------------------------------------------------------------------------
// Phoneme accept thread
// ---------------------------------------------------------------------------
static void phoneme_accept_thread_func(int server_fd) {
    while (!g_stop.load(std::memory_order_acquire)) {
        int client = accept(server_fd, nullptr, nullptr);
        if (client < 0) {
            if (errno == EINTR) continue;
            break;  // socket closed or fatal error
        }

        // Atomically install the new client and close the old one.
        int old = g_phoneme_fd.exchange(client, std::memory_order_acq_rel);
        if (old >= 0) close(old);
        std::cerr << "[phoneme] new client connected (fd=" << client << ")\n";
    }
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------
static void usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " [options]\n"
        << "  --model-root DIR       TTS asset root directory (default: cwd)\n"
        << "  --lang LANG            Language tag (default: en_us)\n"
        << "  --voice VOICE          Voice id, e.g. af_heart, kokoro_af_heart (default: af_heart)\n"
        << "  --speed N              Playback speed (default: 1.0)\n"
        << "  --text-sock PATH       Unix socket for text input  (default: /tmp/moonshine-tts-text.sock)\n"
        << "  --phoneme-sock PATH    Unix socket for phoneme out (default: /tmp/moonshine-tts-phonemes.sock)\n"
        << "  --no-pipewire          Disable PipeWire audio output\n"
        << "  --help, -h             Show this help\n";
}

// ---------------------------------------------------------------------------
// main()
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    // Install signal handlers.
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    // Default paths.
    g_text_sock_path    = "/tmp/moonshine-tts-text.sock";
    g_phoneme_sock_path = "/tmp/moonshine-tts-phonemes.sock";

#ifdef MOONSHINE_WITH_PIPEWIRE
    g_pipewire_enabled = true;
#endif

    // Argument parsing.
    moonshine_tts::MoonshineTTSOptions opt;
    std::string lang = "en_us";
    bool lang_set = false;
    std::vector<std::pair<std::string, std::string>> tts_pairs;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "-h" || a == "--help") {
            usage(argv[0]);
            return 0;
        }
        if (a == "--text-sock" && i + 1 < argc) {
            g_text_sock_path = argv[++i];
        } else if (a == "--phoneme-sock" && i + 1 < argc) {
            g_phoneme_sock_path = argv[++i];
#ifdef MOONSHINE_WITH_PIPEWIRE
        } else if (a == "--no-pipewire") {
            g_pipewire_enabled = false;
#endif
        } else if (a.rfind("--", 0) == 0 && i + 1 < argc) {
            // Collect remaining --key value pairs for MoonshineTTSOptions.
            tts_pairs.emplace_back(a.substr(2), argv[i + 1]);
            ++i;
        } else {
            // Positional arguments are ignored (text comes from socket).
        }
    }

    // Parse TTS options.
    try {
        opt.parse_options(tts_pairs, &lang, &lang_set);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing options: " << e.what() << "\n";
        usage(argv[0]);
        return 2;
    }

    // Create the MoonshineTTS engine.
    try {
        g_tts = std::make_unique<moonshine_tts::MoonshineTTS>(lang, opt);
    } catch (const std::exception& e) {
        std::cerr << "Error initialising TTS engine: " << e.what() << "\n";
        return 1;
    }
    std::cerr << "[main] TTS engine ready (lang=" << lang << ")\n";

    // Create listening sockets.
    g_text_server_fd = make_unix_server(g_text_sock_path);
    if (g_text_server_fd < 0) return 1;
    std::cerr << "[main] text socket: " << g_text_sock_path << "\n";

    g_phoneme_server_fd = make_unix_server(g_phoneme_sock_path);
    if (g_phoneme_server_fd < 0) {
        close(g_text_server_fd);
        unlink(g_text_sock_path.c_str());
        return 1;
    }
    std::cerr << "[main] phoneme socket: " << g_phoneme_sock_path << "\n";

    // Launch threads.
    std::thread synth_thread(synthesis_thread_func);
    std::thread phoneme_thread(phoneme_writer_thread_func);
    std::thread phoneme_accept_thread(phoneme_accept_thread_func, g_phoneme_server_fd);
    std::thread pw_thread;
#ifdef MOONSHINE_WITH_PIPEWIRE
    if (g_pipewire_enabled) {
        pw_thread = std::thread(pipewire_audio_thread_func);
        std::cerr << "[main] PipeWire audio enabled\n";
    } else {
        std::cerr << "[main] PipeWire audio disabled (--no-pipewire)\n";
    }
#endif

    // Main loop: accept text clients.
    std::cerr << "[main] ready — send text to " << g_text_sock_path << "\n";

    while (!g_stop.load(std::memory_order_acquire)) {
        int client = accept(g_text_server_fd, nullptr, nullptr);
        if (client < 0) {
            if (errno == EINTR) continue;
            break;  // socket closed or error
        }

        std::cerr << "[main] text client connected (fd=" << client << ")\n";

        // Read newline‑delimited sentences.
        std::string buf;
        char ch;
        while (!g_stop.load(std::memory_order_acquire)) {
            ssize_t n = read(client, &ch, 1);
            if (n <= 0) {
                if (n < 0 && errno == EINTR) continue;
                break;  // disconnect or error
            }
            if (ch == '\n') {
                if (!buf.empty()) {
                    // Trim trailing carriage return (for \r\n clients).
                    if (!buf.empty() && buf.back() == '\r') buf.pop_back();
                    if (!buf.empty()) g_text_queue.push(buf);
                    buf.clear();
                }
            } else {
                if (buf.size() < kMaxMessageSize) {
                    buf += ch;
                }
            }
        }
        // Flush any remaining partial line.
        if (!buf.empty()) {
            if (!buf.empty() && buf.back() == '\r') buf.pop_back();
            if (!buf.empty()) g_text_queue.push(buf);
        }
        close(client);
        std::cerr << "[main] text client disconnected\n";
    }

    // ---- Shutdown ----
    std::cerr << "[main] shutting down...\n";

    // Close queues to unblock threads.
    g_text_queue.close();
    g_phoneme_queue.close();

    // Close server sockets to unblock accept().
    if (g_text_server_fd >= 0) {
        close(g_text_server_fd);
        g_text_server_fd = -1;
    }
    if (g_phoneme_server_fd >= 0) {
        close(g_phoneme_server_fd);
        g_phoneme_server_fd = -1;
    }

    // Quit PipeWire main loop (signal handler may have already done this).
#ifdef MOONSHINE_WITH_PIPEWIRE
    if (g_pw.loop) pw_main_loop_quit(g_pw.loop);
#endif

    // Join threads.
    if (synth_thread.joinable()) synth_thread.join();
    if (phoneme_thread.joinable()) phoneme_thread.join();
    if (phoneme_accept_thread.joinable()) phoneme_accept_thread.join();
#ifdef MOONSHINE_WITH_PIPEWIRE
    if (pw_thread.joinable()) pw_thread.join();
#endif

    // Close any remaining phoneme client.
    int pfd = g_phoneme_fd.exchange(-1, std::memory_order_acq_rel);
    if (pfd >= 0) close(pfd);

    // Remove socket files.
    unlink(g_text_sock_path.c_str());
    unlink(g_phoneme_sock_path.c_str());

    // Release the TTS engine before static destructors run.
    g_tts.reset();

    std::cerr << "[main] clean exit\n";
    return 0;
}
