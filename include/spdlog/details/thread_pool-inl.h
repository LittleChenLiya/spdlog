// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#ifndef SPDLOG_HEADER_ONLY
#include <spdlog/details/thread_pool.h>
#endif

#include <cassert>
#include <spdlog/common.h>

SPDLOG_NAMESPACE_BEGIN
namespace details {

SPDLOG_INLINE thread_pool::thread_pool(size_t q_max_items,
                                       size_t threads_n,
                                       std::function<void()> on_thread_start,
                                       std::function<void()> on_thread_stop)
    : q_(q_max_items) {
    if (q_max_items == 0) {
        throw_spdlog_ex("thread_pool(): invalid q_max_items param (must be greater than 0)");
    }
    if (threads_n == 0 || threads_n > 1000) {
        throw_spdlog_ex(
            "thread_pool(): invalid threads_n param (valid "
            "range is 1-1000)");
    }
    for (size_t i = 0; i < threads_n; i++) {
        threads_.emplace_back([this, on_thread_start, on_thread_stop] {
            on_thread_start();
            this->thread_pool::worker_loop_();
            on_thread_stop();
        });
    }
}

SPDLOG_INLINE thread_pool::thread_pool(size_t q_max_items,
                                       size_t threads_n,
                                       std::function<void()> on_thread_start)
    : thread_pool(q_max_items, threads_n, std::move(on_thread_start), [] {}) {}

SPDLOG_INLINE thread_pool::thread_pool(size_t q_max_items, size_t threads_n)
    : thread_pool(q_max_items, threads_n, [] {}, [] {}) {}

// message all threads to terminate gracefully join them
SPDLOG_INLINE thread_pool::~thread_pool() {
    SPDLOG_TRY {
        for (size_t i = 0; i < threads_.size(); i++) {
            q_.enqueue(async_msg(async_msg_type::terminate));
        }

        for (auto &t : threads_) {
            t.join();
        }
    }
    SPDLOG_CATCH_STD
}

void SPDLOG_INLINE thread_pool::post_log(async_logger_ptr &&worker_ptr,
                                         const details::log_msg &msg,
                                         async_overflow_policy overflow_policy) {
    async_msg async_m(std::move(worker_ptr), async_msg_type::log, msg);
    post_async_msg_(std::move(async_m), overflow_policy);
}

std::future<flush_result_t> SPDLOG_INLINE thread_pool::post_flush(
    async_logger_ptr &&worker_ptr) {
    // The barrier is enqueued with the blocking policy so it can never be
    // dropped or overwritten. The logger's current flush epoch is sealed and
    // linked to its previous epoch atomically (under the queue lock) at the
    // exact position of the barrier in the queue.
    async_msg async_m(std::move(worker_ptr), async_msg_type::flush_ack);
    std::future<flush_result_t> result_future;
    q_.enqueue_barrier(std::move(async_m), [&] {
        auto state = async_m.worker_ptr->install_flush_barrier_();
        async_m.flush_info.assign(state);
        result_future = state->get_future();
    });
    return result_future;
}

size_t SPDLOG_INLINE thread_pool::overrun_counter() { return q_.overrun_counter(); }

void SPDLOG_INLINE thread_pool::reset_overrun_counter() { q_.reset_overrun_counter(); }

size_t SPDLOG_INLINE thread_pool::discard_counter() { return q_.discard_counter(); }

void SPDLOG_INLINE thread_pool::reset_discard_counter() { q_.reset_discard_counter(); }

size_t SPDLOG_INLINE thread_pool::queue_size() { return q_.size(); }

void SPDLOG_INLINE thread_pool::post_async_msg_(async_msg &&new_msg,
                                                async_overflow_policy overflow_policy) {
    auto *worker = new_msg.worker_ptr.get();
    auto stamp = [worker](async_msg &msg) { worker->stamp_flush_epoch_(msg); };
    auto is_barrier = [](const async_msg &msg) {
        return msg.msg_type != async_msg_type::log;
    };
    auto on_reject = [worker](async_msg &) { worker->record_rejected_(); };

    if (overflow_policy == async_overflow_policy::block) {
        q_.enqueue_log_blocking(std::move(new_msg), stamp);
    } else if (overflow_policy == async_overflow_policy::overrun_oldest) {
        q_.enqueue_log_nowait(std::move(new_msg), stamp, is_barrier, on_reject);
    } else {
        assert(overflow_policy == async_overflow_policy::discard_new);
        q_.enqueue_log_if_have_room(std::move(new_msg), stamp, on_reject);
    }
}

void SPDLOG_INLINE thread_pool::worker_loop_() {
    while (process_next_msg_()) {
    }
}

// process next message in the queue
// returns true if this thread should still be active (while no terminated msg was received)
bool SPDLOG_INLINE thread_pool::process_next_msg_() {
    async_msg incoming_async_msg;
    q_.dequeue(incoming_async_msg);

    switch (incoming_async_msg.msg_type) {
        case async_msg_type::log: {
            incoming_async_msg.worker_ptr->backend_sink_it_(incoming_async_msg);
            incoming_async_msg.flush_info.settle_delivered();
            return true;
        }
        case async_msg_type::flush_ack: {
            auto state = incoming_async_msg.flush_info.get();
            incoming_async_msg.flush_info.detach();
            incoming_async_msg.worker_ptr->backend_flush_();
            // Park until every earlier message settled; then fulfill the
            // future with the flush result.
            state->barrier_reached();
            return true;
        }

        case async_msg_type::terminate: {
            return false;
        }

        default: {
            assert(false);
        }
    }

    return true;
}

}  // namespace details
SPDLOG_NAMESPACE_END
