/*
------------------------------------------------------------
Author: Subhajit Halder
Date Created: 2026-06-07
Date Last Modified: 2026-06-07
Module: Moonshine Streaming TTS
File: spsc_ringbuffer.h
About: Lock‑free single‑producer single‑consumer ring buffer
       for real‑time audio transfer in the streaming TTS
       daemon.  Fixed capacity (power of two), C++17, header-
       only, no dynamic allocation.
Revisions:
- 2026-06-07  Initial ring buffer for PipeWire audio path
------------------------------------------------------------
*/

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

template <typename T, size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,
                  "T must be trivially copyable for lock-free safety");

    static constexpr size_t kMask = Capacity - 1;

    std::array<T, Capacity> buf_{};
    std::atomic<size_t>     write_idx_{0};
    std::atomic<size_t>     read_idx_{0};

public:
    SPSCRingBuffer() = default;

    // Non-copyable, non-movable
    SPSCRingBuffer(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;

    // Push up to `count` elements from `data`. Returns number actually written.
    // Non-blocking; if insufficient space, writes as many as fit and returns that count.
    size_t push(const T* data, size_t count) {
        const size_t read  = read_idx_.load(std::memory_order_acquire);
        const size_t write = write_idx_.load(std::memory_order_relaxed);

        const size_t used   = write - read;
        const size_t space  = Capacity - used;
        const size_t to_write = std::min(count, space);

        if (to_write == 0) return 0;

        size_t idx = write & kMask;
        const size_t first_chunk = std::min(to_write, Capacity - idx);
        for (size_t i = 0; i < first_chunk; ++i) {
            buf_[idx + i] = data[i];
        }
        if (to_write > first_chunk) {
            const size_t remainder = to_write - first_chunk;
            for (size_t i = 0; i < remainder; ++i) {
                buf_[i] = data[first_chunk + i];
            }
        }

        write_idx_.store(write + to_write, std::memory_order_release);
        return to_write;
    }

    // Pop up to `max_count` elements into `out`. Returns number actually read.
    // Non-blocking; returns 0 if the buffer is empty.
    size_t pop(T* out, size_t max_count) {
        const size_t write = write_idx_.load(std::memory_order_acquire);
        const size_t read  = read_idx_.load(std::memory_order_relaxed);

        const size_t available = write - read;
        const size_t to_read   = std::min(max_count, available);

        if (to_read == 0) return 0;

        size_t idx = read & kMask;
        const size_t first_chunk = std::min(to_read, Capacity - idx);
        for (size_t i = 0; i < first_chunk; ++i) {
            out[i] = buf_[idx + i];
        }
        if (to_read > first_chunk) {
            const size_t remainder = to_read - first_chunk;
            for (size_t i = 0; i < remainder; ++i) {
                out[first_chunk + i] = buf_[i];
            }
        }

        read_idx_.store(read + to_read, std::memory_order_release);
        return to_read;
    }

    // Approximate number of elements currently in the buffer.
    size_t size() const {
        const size_t w = write_idx_.load(std::memory_order_acquire);
        const size_t r = read_idx_.load(std::memory_order_acquire);
        return w - r;
    }
};
```
