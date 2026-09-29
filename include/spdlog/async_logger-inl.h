// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#ifndef SPDLOG_HEADER_ONLY
#include <spdlog/async_logger.h>
#endif

#include <spdlog/details/thread_pool.h>
#include <spdlog/sinks/sink.h>

#include <memory>
#include <string>

SPDLOG_NAMESPACE_BEGIN

SPDLOG_INLINE async_logger::async_logger(std::string logger_name,
                                                 sinks_init_list sinks_list,
                                                 std::weak_ptr<details::thread_pool> tp,
                                                 async_overflow_policy overflow_policy)
    : async_logger(std::move(logger_name),
                   sinks_list.begin(),
                   sinks_list.end(),
                   std::move(tp),
                   overflow_policy) {}

SPDLOG_INLINE async_logger::async_logger(std::string logger_name,
                                                 sink_ptr single_sink,
                                                 std::weak_ptr<details::thread_pool> tp,
                                                 async_overflow_policy overflow_policy)
    : async_logger(
          std::move(logger_name), {std::move(single_sink)}, std::move(tp), overflow_policy) {}

// send the log message to the thread pool
SPDLOG_INLINE void async_logger::sink_it_(const details::log_msg &msg){
    SPDLOG_TRY{if (auto pool_ptr = thread_pool_.lock()){
        pool_ptr -> post_log(shared_from_this(), msg, overflow_policy_);
}
else {
    throw_spdlog_ex("async log: thread pool doesn't exist anymore");
}
}
SPDLOG_LOGGER_CATCH(msg.source)
}

// send flush request to the thread pool
SPDLOG_INLINE void async_logger::flush_(){
    SPDLOG_TRY{if (auto pool_ptr = thread_pool_.lock()){
        pool_ptr->post_flush(shared_from_this());
}
else {
    throw_spdlog_ex("async flush: thread pool doesn't exist anymore");
}
}
SPDLOG_LOGGER_CATCH(source_loc())
}

// send an awaitable flush barrier to the thread pool
SPDLOG_INLINE std::future<flush_result_t> async_logger::flush_async() {
    if (auto pool_ptr = thread_pool_.lock()) {
        return pool_ptr->post_flush(shared_from_this());
    }
    throw_spdlog_ex("async flush: thread pool doesn't exist anymore");
}

// Stamp a log message with the logger's current flush epoch (under queue
// lock, so this matches the exact position of the message in the queue).
SPDLOG_INLINE void async_logger::stamp_flush_epoch_(details::async_msg &msg) {
    if (!flush_epoch_) {
        flush_epoch_ = details::flush_state::create(nullptr);
    }
    msg.flush_info.assign(flush_epoch_);
    flush_epoch_->add_inflight();
}

SPDLOG_INLINE void async_logger::record_rejected_() {
    if (!flush_epoch_) {
        flush_epoch_ = details::flush_state::create(nullptr);
    }
    // Count the loss against the current epoch without an in-flight entry:
    // this message was never queued and so there is nothing to wait for.
    flush_epoch_->record_loss();
}

// Turn the current epoch into the pending flush barrier and open a new epoch.
// Called while the queue lock is held, right as the barrier is stored.
SPDLOG_INLINE std::shared_ptr<details::flush_state>
async_logger::install_flush_barrier_() {
    if (!flush_epoch_) {
        flush_epoch_ = details::flush_state::create(nullptr);
    }
    auto barrier = flush_epoch_;
    flush_epoch_ = details::flush_state::create(barrier);
    return barrier;
}

//
// backend functions - called from the thread pool to do the actual job
//
SPDLOG_INLINE void async_logger::backend_sink_it_(const details::log_msg &incoming_log_msg) {
    for (auto &sink : sinks_) {
        if (sink->should_log(incoming_log_msg.level)) {
            SPDLOG_TRY { sink->log(incoming_log_msg); }
            SPDLOG_LOGGER_CATCH(incoming_log_msg.source)
        }
    }

    if (should_flush_(incoming_log_msg)) {
        backend_flush_();
    }
}

SPDLOG_INLINE void async_logger::backend_flush_() {
    for (auto &sink : sinks_) {
        SPDLOG_TRY { sink->flush(); }
        SPDLOG_LOGGER_CATCH(source_loc())
    }
}

SPDLOG_INLINE std::shared_ptr<logger> async_logger::clone(std::string new_name) {
    auto cloned = std::make_shared<async_logger>(*this);
    cloned->name_ = std::move(new_name);
    cloned->flush_epoch_.reset();
    return cloned;
}

SPDLOG_NAMESPACE_END
