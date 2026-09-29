// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

// Fast asynchronous logger.
// Uses pre allocated queue.
// Creates a single back thread to pop messages from the queue and log them.
//
// Upon each log write the logger:
//    1. Checks if its log level is enough to log the message
//    2. Push a new copy of the message to a queue (or block the caller until
//    space is available in the queue)
// Upon destruction, logs all remaining messages in the queue before
// destructing..

#include <spdlog/logger.h>
#include <spdlog/details/flush_state.h>

#include <future>

SPDLOG_NAMESPACE_BEGIN

// Async overflow policy - block by default.
enum class async_overflow_policy {
    block,           // Block until message can be enqueued
    overrun_oldest,  // Discard oldest message in the queue if full when trying to
                     // add new item.
    discard_new      // Discard new message if the queue is full when trying to add new item.
};

namespace details {
class thread_pool;
struct async_msg;
}

class SPDLOG_API async_logger final : public std::enable_shared_from_this<async_logger>,
                                      public logger {
    friend class details::thread_pool;

public:
    template <typename It>
    async_logger(std::string logger_name,
                 It begin,
                 It end,
                 std::weak_ptr<details::thread_pool> tp,
                 async_overflow_policy overflow_policy = async_overflow_policy::block)
        : logger(std::move(logger_name), begin, end),
          thread_pool_(std::move(tp)),
          overflow_policy_(overflow_policy) {}

    async_logger(std::string logger_name,
                 sinks_init_list sinks_list,
                 std::weak_ptr<details::thread_pool> tp,
                 async_overflow_policy overflow_policy = async_overflow_policy::block);

    async_logger(std::string logger_name,
                 sink_ptr single_sink,
                 std::weak_ptr<details::thread_pool> tp,
                 async_overflow_policy overflow_policy = async_overflow_policy::block);

    std::shared_ptr<logger> clone(std::string new_name) override;

    // Asynchronously flush this logger and return a handle that completes
    // only once every message received by the logger before this call has
    // been processed (or recorded as lost) and the sinks finished flushing.
    std::future<flush_result_t> flush_async() override;

protected:
    void sink_it_(const details::log_msg &msg) override;
    void flush_() override;
    void backend_sink_it_(const details::log_msg &incoming_log_msg);
    void backend_flush_();

private:
    // Stamp a log message with the logger's current flush epoch. Called by
    // the thread pool under the queue lock at enqueue time.
    void stamp_flush_epoch_(details::async_msg &msg);

    // Record a message that was never queued because the overflow policy had
    // to drop it. Called under the queue lock.
    void record_rejected_();

    // Seal the current flush epoch (turning it into the pending barrier) and
    // start a new open epoch for subsequent messages. Called by the thread
    // pool under the queue lock when enqueuing a flush barrier.
    std::shared_ptr<details::flush_state> install_flush_barrier_();
    std::weak_ptr<details::thread_pool> thread_pool_;
    async_overflow_policy overflow_policy_;
    std::shared_ptr<details::flush_state> flush_epoch_;
};
SPDLOG_NAMESPACE_END

#ifdef SPDLOG_HEADER_ONLY
#include "async_logger-inl.h"
#endif
