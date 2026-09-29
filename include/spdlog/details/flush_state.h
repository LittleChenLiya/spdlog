// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

// Internal bookkeeping for an asynchronous flush barrier (flush_async).
//
// A flush_state tracks all log messages stamped with it while they are still
// "in flight" (enqueued or being processed by a worker). The associated flush
// barrier completes only after every stamped message was delivered to the
// sinks (or recorded as lost by an overflow policy) and the barrier's own sink
// flush finished.
//
// Flush states form a per-logger chain: each new flush seals the logger's
// previous epoch. A barrier therefore also waits for all earlier barriers of
// the same logger, which guarantees correct ordering even when several worker
// threads dequeue and process messages out of order.

#include <spdlog/common.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <vector>

SPDLOG_NAMESPACE_BEGIN
namespace details {

class flush_state : public std::enable_shared_from_this<flush_state> {
public:
    flush_state() = default;

    flush_state(const flush_state &) = delete;
    flush_state &operator=(const flush_state &) = delete;

    // Create a new epoch state linked as the successor of the previous state.
    // Must be called while the queue lock is held. Pass nullptr for the very
    // first state of a logger.
    static std::shared_ptr<flush_state> create(
        const std::shared_ptr<flush_state> &prev) {
        std::shared_ptr<flush_state> state(new flush_state());
        state->prev_ = prev;
        if (prev) {
            std::lock_guard<std::mutex> prev_lock(prev->mutex_);
            prev->next_.push_back(std::weak_ptr<flush_state>(state));
        }
        return state;
    }

    // Obtain the future that becomes ready once this barrier (and all earlier
    // barriers of the same logger) finished.
    std::future<flush_result_t> get_future() {
        std::lock_guard<std::mutex> lock(mutex_);
        return promise_.get_future();
    }

    // Account for a new in-flight message stamped with this state.
    // Must be called while the queue lock is held; the state mutex is only
    // taken very briefly.
    void add_inflight() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++inflight_;
    }

    // A stamped message was delivered to the sinks. Called by the worker that
    // processed it (no queue lock required).
    void log_delivered() { settle_(); }

    // A stamped message was dropped by an overflow policy.
    // Called while the queue lock is held.
    void log_lost(size_t count = 1) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            lost_ += count;
        }
        settle_();
    }

    // Record a message that was dropped before it ever entered the queue.
    // Such a message has no in-flight entry, so only the loss is counted.
    // Called while the queue lock is held.
    void record_loss(size_t count = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        lost_ += count;
    }

    // Block until this state's barrier finished. Used by successor barriers.
    void wait_until_ready() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock,
                 [this] { return finished_.load(std::memory_order_acquire); });
    }

    // Called by a worker once it reached the flush barrier and performed the
    // actual sink flush. Waits for earlier barriers and for every stamped
    // in-flight message (possibly processed by other workers) to settle, then
    // fulfills the flush result.
    void barrier_reached() {
        std::shared_ptr<flush_state> prev;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            prev = prev_;
        }
        // Earlier epochs must be fully flushed before this barrier completes.
        if (prev) {
            prev->wait_until_ready();
        }

        std::vector<std::shared_ptr<flush_state>> to_notify;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            barrier_seen_ = true;
            cv_.wait(lock, [this] { return inflight_ == 0; });
            if (!finished_.exchange(true, std::memory_order_acq_rel)) {
                flush_result_t result;
                result.lost_messages = lost_;
                try {
                    promise_.set_value(result);
                } catch (const std::future_error &) {
                    // No future was retrieved (fire-and-forget flush_async).
                }
                for (auto &weak_next : next_) {
                    if (auto next = weak_next.lock()) {
                        to_notify.push_back(std::move(next));
                    }
                }
            }
        }

        for (auto &next : to_notify) {
            next->cv_.notify_all();
        }
        cv_.notify_all();
    }

private:
    using weak_state = std::weak_ptr<flush_state>;

    // Decrement the in-flight count and finalize if the barrier was already
    // reached and this was the last pending message.
    void settle_() {
        std::vector<std::shared_ptr<flush_state>> to_notify;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --inflight_;
            if (barrier_seen_ && inflight_ == 0 &&
                !finished_.load(std::memory_order_acquire)) {
                finished_.store(true, std::memory_order_release);
                flush_result_t result;
                result.lost_messages = lost_;
                try {
                    promise_.set_value(result);
                } catch (const std::future_error &) {
                    // No future was retrieved.
                }
                for (auto &weak_next : next_) {
                    if (auto next = weak_next.lock()) {
                        to_notify.push_back(std::move(next));
                    }
                }
            }
        }

        // Wake a barrier parked in barrier_reached(), plus any successor
        // barriers whose turn has come.
        cv_.notify_all();
        for (auto &next : to_notify) {
            next->cv_.notify_all();
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::promise<flush_result_t> promise_;
    std::shared_ptr<flush_state> prev_;
    std::vector<weak_state> next_;
    size_t inflight_{0};
    size_t lost_{0};
    bool barrier_seen_{false};
    std::atomic<bool> finished_;
};

}  // namespace details
SPDLOG_NAMESPACE_END
