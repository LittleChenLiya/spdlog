// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/details/log_msg_buffer.h>
#include <spdlog/details/flush_state.h>
#include <spdlog/details/mpmc_blocking_q.h>
#include <spdlog/details/os.h>

#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

SPDLOG_NAMESPACE_BEGIN
class async_logger;

namespace details {

using async_logger_ptr = std::shared_ptr<async_logger>;

enum class async_msg_type { log, flush_ack, terminate };

// RAII holder for the flush epoch a message belongs to. Releasing the holder
// (message delivered, evicted by overrun_oldest, or rejected by discard_new)
// settles the in-flight bookkeeping exactly once.
class async_flush_info {
public:
    async_flush_info() = default;
    explicit async_flush_info(std::shared_ptr<flush_state> state)
        : state_(std::move(state)) {}

    async_flush_info(const async_flush_info &) = delete;
    async_flush_info &operator=(const async_flush_info &) = delete;

    async_flush_info(async_flush_info &&other) SPDLOG_NOEXCEPT
        : state_(std::move(other.state_)) {
        other.state_.reset();
    }

    async_flush_info &operator=(async_flush_info &&other) SPDLOG_NOEXCEPT {
        if (this != &other) {
            release_();
            state_ = std::move(other.state_);
            other.state_.reset();
        }
        return *this;
    }

    ~async_flush_info() { release_(); }

    void assign(std::shared_ptr<flush_state> state) {
        release_();
        state_ = std::move(state);
    }

    const std::shared_ptr<flush_state> &get() const { return state_; }
    explicit operator bool() const { return static_cast<bool>(state_); }

    // The message reached the sinks: settle as delivered (not lost) and detach.
    void settle_delivered() {
        if (state_) {
            state_->log_delivered();
            state_.reset();
        }
    }

    // Detach without loss accounting (used by the flush barrier itself).
    void detach() { state_.reset(); }

private:
    // Any other release (overwrite or rejection) is a lost message.
    void release_() {
        if (state_) {
            state_->log_lost();
            state_.reset();
        }
    }

    std::shared_ptr<flush_state> state_;
};

// Async msg to move to/from the queue
// Movable only. should never be copied
struct async_msg : log_msg_buffer {
    async_msg_type msg_type{async_msg_type::log};
    async_logger_ptr worker_ptr;
    async_flush_info flush_info;

    async_msg() = default;
    ~async_msg() = default;

    // should only be moved in or out of the queue..
    async_msg(const async_msg &) = delete;

// support for vs2013 move
#if defined(_MSC_VER) && _MSC_VER <= 1800
    async_msg(async_msg &&other)
        : log_msg_buffer(std::move(other)),
          msg_type(other.msg_type),
          worker_ptr(std::move(other.worker_ptr)),
          flush_info(std::move(other.flush_info)) {}

    async_msg &operator=(async_msg &&other) {
        *static_cast<log_msg_buffer *>(this) = std::move(other);
        msg_type = other.msg_type;
        worker_ptr = std::move(other.worker_ptr);
        flush_info = std::move(other.flush_info);
        return *this;
    }
#else  // (_MSC_VER) && _MSC_VER <= 1800
    async_msg(async_msg &&) = default;
    async_msg &operator=(async_msg &&) = default;
#endif

    // construct from log_msg with given type
    async_msg(async_logger_ptr &&worker, async_msg_type the_type, const details::log_msg &m)
        : log_msg_buffer{m},
          msg_type{the_type},
          worker_ptr{std::move(worker)} {}

    async_msg(async_logger_ptr &&worker, async_msg_type the_type)
        : log_msg_buffer{},
          msg_type{the_type},
          worker_ptr{std::move(worker)} {}

    explicit async_msg(async_msg_type the_type)
        : async_msg{nullptr, the_type} {}
};

class SPDLOG_API thread_pool {
public:
    using item_type = async_msg;
    using q_type = details::mpmc_blocking_queue<item_type>;

    thread_pool(size_t q_max_items,
                size_t threads_n,
                std::function<void()> on_thread_start,
                std::function<void()> on_thread_stop);
    thread_pool(size_t q_max_items, size_t threads_n, std::function<void()> on_thread_start);
    thread_pool(size_t q_max_items, size_t threads_n);

    // message all threads to terminate gracefully and join them
    ~thread_pool();

    thread_pool(const thread_pool &) = delete;
    thread_pool &operator=(thread_pool &&) = delete;

    void post_log(async_logger_ptr &&worker_ptr,
                  const details::log_msg &msg,
                  async_overflow_policy overflow_policy);
    std::future<flush_result_t> post_flush(async_logger_ptr &&worker_ptr);
    size_t overrun_counter();
    void reset_overrun_counter();
    size_t discard_counter();
    void reset_discard_counter();
    size_t queue_size();

private:
    q_type q_;

    std::vector<std::thread> threads_;

    void post_async_msg_(async_msg &&new_msg, async_overflow_policy overflow_policy);
    void worker_loop_();

    // process next message in the queue
    // return true if this thread should still be active (while no terminate msg
    // was received)
    bool process_next_msg_();
};

}  // namespace details
SPDLOG_NAMESPACE_END

#ifdef SPDLOG_HEADER_ONLY
#include "thread_pool-inl.h"
#endif
