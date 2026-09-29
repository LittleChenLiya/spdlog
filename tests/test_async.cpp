#include "includes.h"
#include "spdlog/async.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "test_sink.h"

#define TEST_FILENAME "test_logs/async_test.log"

// A latch-like gate used to deterministically stall a worker inside a sink,
// without any sleeps.
class gate {
public:
    void wait_to_enter(size_t n) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return entered_ >= n; });
    }

    void enter() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++entered_;
        cv_.notify_all();
    }

    void wait_inside() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !closed_; });
    }

    void open() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = false;
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t entered_{0};
    bool closed_{true};
};

// Sink implementing the raw sink interface so that log() and flush() can be
// blocked independently (base_sink serializes them with a single mutex).
class gated_sink : public spdlog::sinks::sink {
public:
    explicit gated_sink(std::shared_ptr<gate> log_gate,
                        std::shared_ptr<gate> flush_gate = nullptr)
        : log_gate_(std::move(log_gate)),
          flush_gate_(std::move(flush_gate)) {}

    // The next n calls to log() block inside the gate until it is opened.
    void set_block_logs(size_t n) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        block_logs_ = n;
    }

    void set_throw_on_log(bool on) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        throw_on_log_ = on;
    }

    void set_throw_on_flush(bool on) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        throw_on_flush_ = on;
    }

    size_t logged() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        return logged_;
    }

    size_t flushed() {
        std::lock_guard<std::mutex> lock(state_mutex_);
        return flushed_;
    }

    void log(const spdlog::details::log_msg &) override {
        bool block = false;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (block_logs_ > 0) {
                --block_logs_;
                block = true;
            }
        }
        if (block) {
            log_gate_->enter();
            log_gate_->wait_inside();
        }
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (throw_on_log_) {
            throw std::runtime_error("boom from log");
        }
        ++logged_;
    }

    void flush() override {
        bool gate_flush = static_cast<bool>(flush_gate_);
        if (gate_flush) {
            flush_gate_->enter();
            flush_gate_->wait_inside();
        }
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (throw_on_flush_) {
            throw std::runtime_error("boom from flush");
        }
        ++flushed_;
    }

    void set_pattern(const std::string &) override {}
    void set_formatter(std::unique_ptr<spdlog::formatter>) override {}

private:
    std::shared_ptr<gate> log_gate_;
    std::shared_ptr<gate> flush_gate_;
    std::mutex state_mutex_;
    size_t block_logs_{0};
    size_t logged_{0};
    size_t flushed_{0};
    bool throw_on_log_{false};
    bool throw_on_flush_{false};
};

TEST_CASE("async zero queue size", "[async]") {
    REQUIRE_THROWS_AS(spdlog::details::thread_pool(0, 1), spdlog::spdlog_ex);
}

TEST_CASE("basic async test ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t overrun_counter = 0;
    size_t queue_size = 128;
    size_t messages = 256;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                             spdlog::async_overflow_policy::block);
        for (size_t i = 0; i < messages; i++) {
            logger->info("Hello message #{}", i);
        }
        logger->flush();
        overrun_counter = tp->overrun_counter();
    }
    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
    REQUIRE(overrun_counter == 0);
}

TEST_CASE("discard policy ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(1));
    size_t queue_size = 4;
    size_t messages = 1024;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::overrun_oldest);
    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message");
    }
    REQUIRE(test_sink->msg_counter() < messages);
    REQUIRE(tp->overrun_counter() > 0);
}

TEST_CASE("discard policy discard_new ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(1));
    size_t queue_size = 4;
    size_t messages = 1024;

    auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::discard_new);
    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message");
    }
    REQUIRE(test_sink->msg_counter() < messages);
    REQUIRE(tp->discard_counter() > 0);
}

TEST_CASE("discard policy using factory ", "[async]") {
    size_t queue_size = 4;
    size_t messages = 1024;
    spdlog::init_thread_pool(queue_size, 1);

    auto logger = spdlog::create_async_nb<spdlog::sinks::test_sink_mt>("as2");
    auto test_sink = std::static_pointer_cast<spdlog::sinks::test_sink_mt>(logger->sinks()[0]);
    test_sink->set_delay(std::chrono::milliseconds(3));

    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message");
    }

    REQUIRE(test_sink->msg_counter() < messages);
    spdlog::drop_all();
}

TEST_CASE("flush", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t queue_size = 256;
    size_t messages = 256;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                             spdlog::async_overflow_policy::block);
        for (size_t i = 0; i < messages; i++) {
            logger->info("Hello message #{}", i);
        }

        logger->flush();
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(250));
    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
}

TEST_CASE("async periodic flush", "[async]") {
    auto logger = spdlog::create_async<spdlog::sinks::test_sink_mt>("as");
    auto test_sink = std::static_pointer_cast<spdlog::sinks::test_sink_mt>(logger->sinks()[0]);

    spdlog::flush_every(std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(1700));
    REQUIRE(test_sink->flush_counter() == 1);
    spdlog::flush_every(std::chrono::seconds(0));
    spdlog::drop_all();
}

TEST_CASE("tp->wait_empty() ", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    test_sink->set_delay(std::chrono::milliseconds(5));
    size_t messages = 100;

    auto tp = std::make_shared<spdlog::details::thread_pool>(messages, 2);
    auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                         spdlog::async_overflow_policy::block);
    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message #{}", i);
    }
    logger->flush();
    tp.reset();

    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
}

TEST_CASE("multi threads", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t queue_size = 128;
    size_t messages = 256;
    size_t n_threads = 10;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, tp,
                                                             spdlog::async_overflow_policy::block);

        std::vector<std::thread> threads;
        for (size_t i = 0; i < n_threads; i++) {
            threads.emplace_back([logger, messages] {
                for (size_t j = 0; j < messages; j++) {
                    logger->info("Hello message #{}", j);
                }
            });
            logger->flush();
        }

        for (auto &t : threads) {
            t.join();
        }
    }

    REQUIRE(test_sink->msg_counter() == messages * n_threads);
    REQUIRE(test_sink->flush_counter() == n_threads);
}

TEST_CASE("to_file", "[async]") {
    prepare_logdir();
    size_t messages = 1024;
    size_t tp_threads = 1;
    spdlog::filename_t filename = SPDLOG_FILENAME_T(TEST_FILENAME);
    {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        auto tp = std::make_shared<spdlog::details::thread_pool>(messages, tp_threads);
        auto logger =
            std::make_shared<spdlog::async_logger>("as", std::move(file_sink), std::move(tp));

        for (size_t j = 0; j < messages; j++) {
            logger->info("Hello message #{}", j);
        }
    }

    require_message_count(TEST_FILENAME, messages);
    auto contents = file_contents(TEST_FILENAME);
    using spdlog::details::os::default_eol;
    REQUIRE(ends_with(contents, spdlog::fmt_lib::format("Hello message #1023{}", default_eol)));
}

TEST_CASE("to_file multi-workers", "[async]") {
    prepare_logdir();
    size_t messages = 1024 * 10;
    size_t tp_threads = 10;
    spdlog::filename_t filename = SPDLOG_FILENAME_T(TEST_FILENAME);
    {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(filename, true);
        auto tp = std::make_shared<spdlog::details::thread_pool>(messages, tp_threads);
        auto logger =
            std::make_shared<spdlog::async_logger>("as", std::move(file_sink), std::move(tp));

        for (size_t j = 0; j < messages; j++) {
            logger->info("Hello message #{}", j);
        }
    }
    require_message_count(TEST_FILENAME, messages);
}

TEST_CASE("bad_tp", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    std::shared_ptr<spdlog::details::thread_pool> const empty_tp;
    auto logger = std::make_shared<spdlog::async_logger>("as", test_sink, empty_tp);
    logger->info("Please throw an exception");
    REQUIRE(test_sink->msg_counter() == 0);
}

TEST_CASE("flush_async basic", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t messages = 256;
    auto tp = std::make_shared<spdlog::details::thread_pool>(messages, 2);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::block);

    for (size_t i = 0; i < messages; i++) {
        logger->info("Hello message #{}", i);
    }

    auto result = logger->flush_async().get();
    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
    REQUIRE(result.lost_messages == 0);
}

// Two workers: the first log message is stuck inside the sink. The barrier
// must remain pending even once another worker dequeues and processes it, and
// must complete with zero losses after the stuck message is released.
TEST_CASE("flush_async waits for message stuck on another worker", "[async]") {
    auto log_gate = std::make_shared<gate>();
    auto flush_gate = std::make_shared<gate>();
    auto sink = std::make_shared<gated_sink>(log_gate, flush_gate);

    auto tp = std::make_shared<spdlog::details::thread_pool>(8, 2);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", sink, tp, spdlog::async_overflow_policy::block);

    sink->set_block_logs(1);
    logger->info("stuck message");
    log_gate->wait_to_enter(1);  // a worker is parked inside sink::log()

    auto future = logger->flush_async();

    // Give the second worker time to reach the barrier and stall on the sink
    // flush (fully deterministic: we wait for the flush gate to be entered).
    flush_gate->wait_to_enter(1);

    // The barrier must not have completed while the first message is stuck.
    REQUIRE(future.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);

    // Releasing the sink flush first: the barrier worker then waits for the
    // stuck message; the future must still be pending.
    flush_gate->open();
    REQUIRE(future.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);

    // Releasing the stuck log allows the barrier to finish successfully.
    log_gate->open();
    auto result = future.get();
    REQUIRE(sink->logged() == 1);
    REQUIRE(sink->flushed() == 1);
    REQUIRE(result.lost_messages == 0);
}

// Full queue with overrun_oldest: dropped messages must be reported exactly.
TEST_CASE("flush_async reports overrun_oldest losses", "[async]") {
    auto log_gate = std::make_shared<gate>();
    auto sink = std::make_shared<gated_sink>(log_gate);
    const size_t queue_size = 4;
    const size_t messages = 128;

    auto tp =
        std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", sink, tp, spdlog::async_overflow_policy::overrun_oldest);

    // Stall the very first log so the queue fills up and overruns occur.
    sink->set_block_logs(1);
    logger->info("stuck");
    log_gate->wait_to_enter(1);

    for (size_t i = 0; i < messages; i++) {
        logger->info("flood #{}", i);
    }

    log_gate->open();
    auto future = logger->flush_async();
    auto result = future.get();

    REQUIRE(sink->logged() == queue_size + 1);
    REQUIRE(result.lost_messages == messages - queue_size);
    REQUIRE(tp->overrun_counter() == result.lost_messages);
}

// Full queue with discard_new: rejected messages must be reported exactly.
TEST_CASE("flush_async reports discard_new losses", "[async]") {
    auto log_gate = std::make_shared<gate>();
    auto sink = std::make_shared<gated_sink>(log_gate);
    const size_t queue_size = 4;
    const size_t messages = 128;

    auto tp =
        std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", sink, tp, spdlog::async_overflow_policy::discard_new);

    sink->set_block_logs(1);
    logger->info("stuck");
    log_gate->wait_to_enter(1);

    for (size_t i = 0; i < messages; i++) {
        logger->info("flood #{}", i);
    }

    log_gate->open();
    auto future = logger->flush_async();
    auto result = future.get();

    // stuck message + the 4 slots; barrier always keeps its slot.
    REQUIRE(sink->logged() == queue_size + 1);
    REQUIRE(result.lost_messages == messages - queue_size);
    REQUIRE(tp->discard_counter() == result.lost_messages);
}

// Multiple loggers sharing the pool flush independently; a stuck logger does
// not prevent another logger's barrier from completing.
TEST_CASE("flush_async multiple loggers", "[async]") {
    auto gate_a = std::make_shared<gate>();
    auto gate_b = std::make_shared<gate>();
    auto sink_a = std::make_shared<gated_sink>(gate_a);
    auto sink_b = std::make_shared<gated_sink>(gate_b);

    auto tp = std::make_shared<spdlog::details::thread_pool>(16, 4);
    auto logger_a = std::make_shared<spdlog::async_logger>(
        "a", sink_a, tp, spdlog::async_overflow_policy::block);
    auto logger_b = std::make_shared<spdlog::async_logger>(
        "b", sink_b, tp, spdlog::async_overflow_policy::block);

    sink_a->set_block_logs(1);
    logger_a->info("a stuck");
    gate_a->wait_to_enter(1);

    logger_b->info("b one");
    logger_b->info("b two");
    auto future_b = logger_b->flush_async();

    // Logger B is independent and completes while A is still stuck.
    auto result_b = future_b.get();
    REQUIRE(sink_b->logged() == 2);
    REQUIRE(sink_b->flushed() == 1);
    REQUIRE(result_b.lost_messages == 0);

    auto future_a = logger_a->flush_async();
    gate_a->open();
    auto result_a = future_a.get();
    REQUIRE(sink_a->logged() == 1);
    REQUIRE(sink_a->flushed() == 1);
    REQUIRE(result_a.lost_messages == 0);
}

// A sink that throws from log() or flush() must not hang the barrier.
TEST_CASE("flush_async with throwing sink", "[async]") {
    std::string captured;

    SECTION("throw from log") {
        auto sink = std::make_shared<gated_sink>(nullptr);
        sink->set_throw_on_log(true);
        auto tp = std::make_shared<spdlog::details::thread_pool>(8, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", sink, tp);
        logger->set_error_handler([&](const std::string &msg) { captured = msg; });
        logger->info("will throw");
        auto result = logger->flush_async().get();
        REQUIRE_FALSE(captured.empty());
        REQUIRE(sink->flushed() == 1);
        REQUIRE(result.lost_messages == 0);
    }

    SECTION("throw from flush") {
        auto sink = std::make_shared<gated_sink>(nullptr);
        sink->set_throw_on_flush(true);
        auto tp = std::make_shared<spdlog::details::thread_pool>(8, 1);
        auto logger = std::make_shared<spdlog::async_logger>("as", sink, tp);
        logger->set_error_handler([&](const std::string &msg) { captured = msg; });
        logger->info("ok");
        auto result = logger->flush_async().get();
        REQUIRE_FALSE(captured.empty());
        REQUIRE(sink->logged() == 1);
        REQUIRE(result.lost_messages == 0);
    }
}

// Destroying the thread pool drains the queue and completes the flush handle.
TEST_CASE("flush_async completes when thread pool is destroyed", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    size_t messages = 128;
    std::future<spdlog::flush_result_t> future;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(messages, 2);
        auto logger = std::make_shared<spdlog::async_logger>(
            "as", test_sink, tp, spdlog::async_overflow_policy::block);
        for (size_t i = 0; i < messages; i++) {
            logger->info("Hello message #{}", i);
        }
        future = logger->flush_async();
        tp.reset();  // destructor drains and joins the workers
    }
    REQUIRE(future.valid());
    auto result = future.get();
    REQUIRE(test_sink->msg_counter() == messages);
    REQUIRE(test_sink->flush_counter() == 1);
    REQUIRE(result.lost_messages == 0);
}

// Repeated barriers build epochs that complete in order, each reporting its
// own losses independently.
TEST_CASE("flush_async repeated barriers are ordered", "[async]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    auto tp = std::make_shared<spdlog::details::thread_pool>(64, 2);
    auto logger = std::make_shared<spdlog::async_logger>(
        "as", test_sink, tp, spdlog::async_overflow_policy::block);

    std::vector<std::future<spdlog::flush_result_t>> futures;
    for (size_t round = 0; round < 10; round++) {
        for (size_t i = 0; i < 10; i++) {
            logger->info("round {} message {}", round, i);
        }
        futures.push_back(logger->flush_async());
    }

    for (auto &f : futures) {
        REQUIRE(f.get().lost_messages == 0);
    }
    REQUIRE(test_sink->msg_counter() == 100);
    REQUIRE(test_sink->flush_counter() == 10);
}
