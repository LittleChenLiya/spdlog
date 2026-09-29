// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

// multi producer-multi consumer blocking queue.
// enqueue(..) - will block until room found to put the new message.
// enqueue_nowait(..) - enqueue immediately. overruns oldest message if no
// room left.
// dequeue_for(..) - will block until the queue is not empty or timeout have
// passed.
//
// The *_log / *_barrier variants invoke callbacks under the queue lock so
// the flush epoch of a message can be stamped in exact enqueue order. They
// also guarantee that a barrier item is never overwritten: if the oldest
// item is a barrier an incoming overrun message is rejected instead.

#include <spdlog/details/circular_q.h>

#include <atomic>
#include <condition_variable>
#include <mutex>

SPDLOG_NAMESPACE_BEGIN
namespace details {

template <typename T>
class mpmc_blocking_queue {
public:
    using item_type = T;
    explicit mpmc_blocking_queue(size_t max_items)
        : q_(max_items) {}

#ifndef __MINGW32__
    // try to enqueue and block if no room left
    void enqueue(T &&item) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            pop_cv_.wait(lock, [this] { return !this->q_.full(); });
            q_.push_back(std::move(item));
        }
        push_cv_.notify_one();
    }

    // enqueue immediately. overrun oldest message in the queue if no room left.
    void enqueue_nowait(T &&item) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            q_.push_back(std::move(item));
        }
        push_cv_.notify_one();
    }

    void enqueue_if_have_room(T &&item) {
        bool pushed = false;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (!q_.full()) {
                q_.push_back(std::move(item));
                pushed = true;
            }
        }

        if (pushed) {
            push_cv_.notify_one();
        } else {
            ++discard_counter_;
        }
    }
    // dequeue with a timeout.
    // Return true, if succeeded dequeue item, false otherwise
    bool dequeue_for(T &popped_item, std::chrono::milliseconds wait_duration) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (!push_cv_.wait_for(lock, wait_duration, [this] { return !this->q_.empty(); })) {
                return false;
            }
            popped_item = std::move(q_.front());
            q_.pop_front();
        }
        pop_cv_.notify_one();
        return true;
    }

    // blocking dequeue without a timeout.
    void dequeue(T &popped_item) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            push_cv_.wait(lock, [this] { return !this->q_.empty(); });
            popped_item = std::move(q_.front());
            q_.pop_front();
        }
        pop_cv_.notify_one();
    }

#else
    // apparently mingw deadlocks if the mutex is released before cv.notify_one(),
    // so release the mutex at the very end each function.

    // try to enqueue and block if no room left
    void enqueue(T &&item) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        pop_cv_.wait(lock, [this] { return !this->q_.full(); });
        q_.push_back(std::move(item));
        push_cv_.notify_one();
    }

    // enqueue immediately. overrun oldest message in the queue if no room left.
    void enqueue_nowait(T &&item) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        q_.push_back(std::move(item));
        push_cv_.notify_one();
    }

    void enqueue_if_have_room(T &&item) {
        bool pushed = false;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!q_.full()) {
            q_.push_back(std::move(item));
            pushed = true;
        }

        if (pushed) {
            push_cv_.notify_one();
        } else {
            ++discard_counter_;
        }
    }

    // dequeue with a timeout.
    // Return true, if succeeded dequeue item, false otherwise
    bool dequeue_for(T &popped_item, std::chrono::milliseconds wait_duration) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!push_cv_.wait_for(lock, wait_duration, [this] { return !this->q_.empty(); })) {
            return false;
        }
        popped_item = std::move(q_.front());
        q_.pop_front();
        pop_cv_.notify_one();
        return true;
    }

    // blocking dequeue without a timeout.
    void dequeue(T &popped_item) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        push_cv_.wait(lock, [this] { return !this->q_.empty(); });
        popped_item = std::move(q_.front());
        q_.pop_front();
        pop_cv_.notify_one();
    }

#endif

    // blocking enqueue for a log message. before_push runs under the queue
    // lock right before the message is stored, stamping its flush epoch in the
    // exact enqueue order.
    template <typename BeforePush>
    void enqueue_log_blocking(T &&item, BeforePush before_push) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        pop_cv_.wait(lock, [this] { return !this->q_.full(); });
        before_push(item);
        q_.push_back(std::move(item));
        push_cv_.notify_one();
    }

    // blocking enqueue for a flush barrier. before_push seals the logger's
    // flush epoch and links the barrier to it, atomically with the enqueue.
    template <typename BeforePush>
    void enqueue_barrier(T &&item, BeforePush before_push) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        pop_cv_.wait(lock, [this] { return !this->q_.full(); });
        before_push();
        q_.push_back(std::move(item));
        push_cv_.notify_one();
    }

    // overrun_oldest enqueue for a log message. The message is stamped only if
    // it will actually be stored. If the queue is full and the oldest item is
    // a barrier, the incoming message is rejected (never overwriting the
    // barrier). Note: the overwritten message settles its own bookkeeping via
    // its RAII flush-info destructor when its slot is replaced.
    template <typename BeforePush, typename IsBarrier, typename OnReject>
    void enqueue_log_nowait(T &&item,
                            BeforePush before_push,
                            IsBarrier is_barrier,
                            OnReject on_reject) {
        bool rejected = false;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (q_.full() && is_barrier(q_.front())) {
            on_reject(item);
            rejected = true;
        } else {
            before_push(item);
            q_.push_back(std::move(item));
        }
        if (rejected) {
            ++discard_counter_;
        } else {
            push_cv_.notify_one();
        }
    }

    // discard_new enqueue for a log message. The message is stamped only if it
    // will actually be stored; a rejected message is left untouched.
    template <typename BeforePush, typename OnReject>
    void enqueue_log_if_have_room(T &&item,
                                  BeforePush before_push,
                                  OnReject on_reject) {
        bool pushed = false;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!q_.full()) {
            before_push(item);
            q_.push_back(std::move(item));
            pushed = true;
        } else {
            on_reject(item);
        }

        if (pushed) {
            push_cv_.notify_one();
        } else {
            ++discard_counter_;
        }
    }

    size_t overrun_counter() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return q_.overrun_counter();
    }

    size_t discard_counter() { return discard_counter_.load(std::memory_order_relaxed); }

    size_t size() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return q_.size();
    }

    void reset_overrun_counter() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        q_.reset_overrun_counter();
    }

    void reset_discard_counter() { discard_counter_.store(0, std::memory_order_relaxed); }

private:
    std::mutex queue_mutex_;
    std::condition_variable push_cv_;
    std::condition_variable pop_cv_;
    circular_q<T> q_;
    std::atomic<size_t> discard_counter_{0};
};
}  // namespace details
SPDLOG_NAMESPACE_END
