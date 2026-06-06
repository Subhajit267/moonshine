# 🚀 Moonshine Streaming TTS Extension

### Real-Time Streaming Text-to-Speech Daemon for Moonshine Voice

<div align="center">

![C++](https://img.shields.io/badge/Language-C%2B%2B17-blue)
![Linux](https://img.shields.io/badge/Platform-Linux-green)
![PipeWire](https://img.shields.io/badge/Audio-PipeWire-orange)
![IPC](https://img.shields.io/badge/IPC-Unix%20Domain%20Sockets-yellow)
![Status](https://img.shields.io/badge/Status-Completed-success)

**Streaming Text-to-Speech Extension developed for Moonshine Voice**

</div>

---

# 📖 Table of Contents

1. Overview
2. Original Task Requirements
3. Design Objectives
4. Original Moonshine Architecture
5. New Streaming Architecture
6. IPC Evaluation and Design Decisions
7. Audio Architecture
8. Threading Architecture
9. Implementation Details
10. File Modifications
11. Build Instructions
12. Usage
13. Validation and Testing
14. Known Limitations
15. Future Improvements
16. Author

---

# 🔍 Overview

This repository extends the Moonshine Voice Text-to-Speech subsystem by introducing a dedicated streaming execution mode.

The original Moonshine TTS workflow was primarily designed around batch synthesis:

```text
Input Text
    │
    ▼
Generate Full Waveform
    │
    ▼
Return / Save WAV
```

While this approach works well for offline synthesis, it does not provide:

* Continuous text ingestion
* Real-time audio playback
* Real-time phoneme publication
* Persistent daemon operation

To address these limitations, a new executable named:

```text
moonshine-tts-stream
```

was implemented.

The daemon remains active continuously and provides:

* Text input through Unix Domain Sockets
* Real-time phoneme publication
* Direct PipeWire audio playback
* Thread-safe processing pipeline
* Lock-free audio transport

---

# 📋 Original Task Requirements

The implementation was created based on the following requirements:

### Input

Receive text continuously using:

```text
Named Pipe
or
Unix Domain Socket
```

### Outputs

1. Raw PCM Audio
2. Stream of generated phonemes

### Audio Backend

```text
PipeWire
```

### Execution Model

```text
Persistent daemon
```

instead of a single-shot command-line tool.

---

# 🎯 Design Objectives

The implementation was designed around the following goals.

## Goal 1 — Continuous Service Operation

The process should remain active and handle multiple synthesis requests without restarting.

---

## Goal 2 — Real-Time Audio Delivery

Generated speech should be delivered immediately to the audio subsystem.

---

## Goal 3 — Phoneme Visibility

External applications should be able to observe generated phonemes for:

* Robotics
* Lip Sync
* Avatar Animation
* Speech Diagnostics

---

## Goal 4 — Thread Isolation

Audio playback must never be blocked by:

* Slow clients
* Socket operations
* Text processing

---

## Goal 5 — Future Extensibility

The design should allow future enhancements such as:

* Multiple clients
* Alternative audio backends
* Lower latency chunking
* Network transport

without major architectural changes.

---

# 🏗 Original Moonshine Architecture

The original Moonshine TTS workflow can be simplified as:

```text
Text
 │
 ▼
MoonshineTTS
 │
 ▼
Full Waveform
 │
 ▼
WAV Output
```

Characteristics:

| Feature            | Supported |
| ------------------ | --------- |
| Batch Synthesis    | ✔         |
| WAV Generation     | ✔         |
| Streaming Playback | ✖         |
| Continuous Service | ✖         |
| Phoneme Streaming  | ✖         |

---

# 🚀 New Streaming Architecture

The new architecture introduces a dedicated streaming daemon.

```text
                ┌──────────────────┐
                │   Text Client    │
                └────────┬─────────┘
                         │
                         ▼
             Unix Domain Socket
                         │
                         ▼
              moonshine-tts-stream
                         │
                Text Work Queue
                         │
                         ▼
                Synthesis Thread
                         │
            ┌────────────┴────────────┐
            │                         │
            ▼                         ▼
      Phoneme Queue           Audio Ring Buffer
            │                         │
            ▼                         ▼
   Phoneme Socket            PipeWire Playback
            │                         │
            ▼                         ▼
       Client App                Speakers
```

---

# ⚙️ IPC Evaluation and Design Decisions

The task allowed either:

```text
Named Pipes
```

or

```text
Unix Domain Sockets
```

for communication.

Both approaches were evaluated before implementation.

---

## Option 1 — Named Pipes (FIFO)

Example:

```bash
mkfifo /tmp/moonshine-tts.fifo
```

### Advantages

* Simple
* Lightweight
* Standard Unix primitive

### Limitations

* Less flexible client management
* More cumbersome for multiple communication channels
* Not naturally connection-oriented

### Assessment

Named Pipes satisfy the task requirements but become less convenient when separate text and phoneme channels are required.

---

## Option 2 — Unix Domain Sockets

Example:

```cpp
int fd = socket(AF_UNIX, SOCK_STREAM, 0);
```

### Advantages

* Connection-oriented
* Independent channels
* Multiple client support in future
* Very low IPC overhead
* Clean service architecture

### Assessment

Unix Domain Sockets provide a cleaner implementation for:

```text
Text Input Socket
Phoneme Output Socket
```

and were therefore selected.

---

# 🔊 Audio Architecture

A major design requirement was ensuring that audio playback never blocks synthesis.

To achieve this, synthesis and playback operate independently.

---

## Audio Producer

The synthesis thread generates PCM audio and writes it into a lock-free buffer.

Example:

```cpp
size_t pushed =
    g_audio_ring.push(
        pcm_int16.data(),
        pcm_int16.size());
```

---

## Audio Consumer

The PipeWire callback continuously consumes audio samples.

Example:

```cpp
size_t n =
    g_audio_ring.pop(
        tmp,
        requested);
```

---

## Why a Lock-Free Ring Buffer?

A traditional queue protected by mutexes can introduce:

* Thread contention
* Scheduling delays
* Audio glitches

The SPSC (Single Producer Single Consumer) ring buffer avoids these issues and is suitable for real-time audio delivery.

---

# 🧵 Threading Architecture

The daemon uses multiple dedicated threads.

---

## Main Thread

Responsibilities:

```text
Argument Parsing
Socket Creation
Text Client Acceptance
Lifecycle Management
```

---

## Synthesis Thread

Responsibilities:

```text
Receive Text
Generate Phonemes
Generate Audio
Publish Chunks
Push PCM to Ring Buffer
```

---

## Phoneme Writer Thread

Responsibilities:

```text
Serialize Phoneme Events
Write JSON Messages
Publish to Clients
```

Example payload:

```json
{
  "seq": 0,
  "chunk": "həlˈoʊ wˈɝld"
}
```

---

## Phoneme Accept Thread

Responsibilities:

```text
Accept New Phoneme Subscribers
Replace Existing Connections
Manage Listener Lifecycle
```

---

## PipeWire Thread

Responsibilities:

```text
Consume Audio Ring Buffer
Feed PipeWire Stream
Maintain Playback
```

The audio callback performs:

* No synthesis
* No memory allocation
* No socket operations

ensuring real-time safety.

---

# 🔧 Streaming API Extension

The original Moonshine API exposed only batch synthesis.

Original usage:

```cpp
auto audio = tts.synthesize(text);
```

To support streaming, the API was extended.

Added to:

```text
core/moonshine-tts/include/moonshine-tts.h
```

Example:

```cpp
using ChunkCallback =
    std::function<void(
        const std::string&,
        std::vector<float>)>;
```

```cpp
std::vector<float> synthesize_streaming(
    const std::string& text,
    ChunkCallback callback);
```

This allows synthesized chunks to be emitted during synthesis rather than only after completion.

---

# 📂 File Modifications

## New Files

### `core/moonshine-tts/tools/moonshine-tts-stream-cli.cpp`

Purpose:

Main streaming daemon implementation.

Responsibilities:

* Socket server
* Text ingestion
* Audio streaming
* Phoneme publication
* Thread management
* PipeWire integration

---

### `core/moonshine-tts/tools/spsc_ringbuffer.h`

Purpose:

Lock-free audio transport layer.

Responsibilities:

* Single Producer
* Single Consumer
* Real-time safe buffering

---

## Modified Files

### `core/moonshine-tts/include/moonshine-tts.h`

Reason:

Expose streaming synthesis API.

Added:

* ChunkCallback
* synthesize_streaming()

---

### `core/moonshine-tts/src/moonshine-tts.cpp`

Reason:

Implement callback-driven synthesis.

Added:

* Streaming synthesis path
* Callback dispatching
* Chunk emission support

---

### `core/moonshine-tts/CMakeLists.txt`

Reason:

Introduce a dedicated executable target.

Added:

```cmake
add_executable(moonshine_tts_stream ...)
```

Also added:

* PipeWire detection
* Conditional PipeWire linking
* Include directory configuration

---

# 🔧 Build Instructions

Dependencies:

```bash
sudo apt install \
    build-essential \
    cmake \
    pkg-config \
    libpipewire-0.3-dev
```

Build:

```bash
cd core

mkdir build
cd build

cmake ..

cmake --build . --target moonshine_tts_stream -j$(nproc)
```

---

# 🚀 Usage

Start daemon:

```bash
./moonshine-tts-stream \
    --model-root ../data \
    --lang en_us \
    --voice af_heart
```

---

Connect phoneme monitor:

```bash
nc -U /tmp/moonshine-tts-phonemes.sock
```

---

Send text:

```bash
printf "Hello world\n" | nc -U /tmp/moonshine-tts-text.sock
```

---

Expected phoneme output:

```json
{"seq":0,"chunk":"həlˈoʊ wˈɝld"}
```

---

# 🧪 Validation and Testing

The implementation was validated on:

```text
Ubuntu / Zorin OS
GCC 11
C++17
PipeWire
```

Verified functionality:

✔ Successful compilation

✔ Streaming daemon startup

✔ Unix socket communication

✔ Phoneme generation

✔ Phoneme publication

✔ PipeWire playback

✔ Continuous operation

✔ Graceful shutdown

---

# ⚠️ Known Limitations

The streaming infrastructure is fully operational.

However, the current Kokoro backend generally synthesizes short utterances as a single chunk.

Therefore:

```text
Streaming IPC         ✔
Streaming Playback    ✔
Streaming Audio       ✔

Fine-Grained Chunking
Limited by current Kokoro chunk strategy
```

This affects perceived first-audio latency for short utterances but does not affect correctness of the streaming architecture itself.

---

# 🛤 Future Improvements

Potential future enhancements:

* Adaptive phoneme chunk sizing
* Reduced first-audio latency
* Multi-client support
* Network transport
* WebSocket interface

---

# 👨‍💻 Author

**Subhajit Halder**

Moonshine Streaming TTS Extension

Developed as a streaming execution layer for Moonshine Voice to provide real-time text ingestion, phoneme publication, and audio playback through PipeWire.

---

<div align="center">

**Moonshine Streaming TTS Extension — Design Document & Implementation Overview**

</div>
