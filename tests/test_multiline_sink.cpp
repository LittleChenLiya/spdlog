#include "includes.h"
#include "spdlog/async.h"
#include "spdlog/sinks/multiline_sink.h"
#include "test_sink.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using spdlog::sinks::sink;

// Sink that records the raw payload of each forwarded log call.
class recording_sink : public sink {
public:
    void log(const spdlog::details::log_msg &msg) final {
        std::lock_guard<std::mutex> lock(mutex_);
        payloads.emplace_back(msg.payload.data(), msg.payload.size());
        levels.push_back(msg.level);
        times.push_back(msg.time);
        names.emplace_back(msg.logger_name.data(), msg.logger_name.size());
        tids.push_back(msg.thread_id);
    }

    void flush() final {
        std::lock_guard<std::mutex> lock(mutex_);
        ++flush_calls;
    }

    void set_pattern(const std::string &) final {}
    void set_formatter(std::unique_ptr<spdlog::formatter>) final {}

    std::mutex mutex_;
    std::vector<std::string> payloads;
    std::vector<spdlog::level::level_enum> levels;
    std::vector<spdlog::log_clock::time_point> times;
    std::vector<std::string> names;
    std::vector<size_t> tids;
    size_t flush_calls = 0;
};

// Sink that throws on its nth log call (1 based).
class fail_on_nth_sink : public sink {
public:
    explicit fail_on_nth_sink(size_t fail_on) : fail_on_{fail_on} {}

    void log(const spdlog::details::log_msg &) final {
        ++calls_;
        if (calls_ == fail_on_) {
            throw std::runtime_error("injected sink failure");
        }
    }

    void flush() final { ++flush_calls_; }
    void set_pattern(const std::string &) final {}
    void set_formatter(std::unique_ptr<spdlog::formatter>) final {}

    size_t calls() const { return calls_; }
    size_t flush_calls() const { return flush_calls_; }

private:
    size_t fail_on_;
    size_t calls_ = 0;
    size_t flush_calls_ = 0;
};

// Slow sink: sleeps on every log call to make overlapping log() calls observable.
class slow_sink : public sink {
public:
    void log(const spdlog::details::log_msg &) final {
        ++in_flight;
        size_t observed = in_flight.load();
        max_in_flight.store(std::max(max_in_flight.load(), observed));
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        --in_flight;
        ++calls;
    }

    void flush() final {}
    void set_pattern(const std::string &) final {}
    void set_formatter(std::unique_ptr<spdlog::formatter>) final {}

    std::atomic<size_t> in_flight{0};
    std::atomic<size_t> max_in_flight{0};
    std::atomic<size_t> calls{0};
};

// Two party barrier so both threads submit at the same instant.
class barrier2 {
public:
    void arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++arrived_;
        if (arrived_ == 2) {
            cv_.notify_all();
        } else {
            cv_.wait(lock, [this] { return arrived_ == 2; });
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int arrived_ = 0;
};

spdlog::details::log_msg make_msg(spdlog::string_view_t logger_name,
                                  spdlog::level::level_enum lvl,
                                  spdlog::string_view_t payload) {
    return spdlog::details::log_msg{logger_name, lvl, payload};
}

}  // namespace

TEST_CASE("multiline basic split", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec};

    ml.log(make_msg("lg", spdlog::level::warn, "a\nb\nc"));

    REQUIRE(rec->payloads == std::vector<std::string>{"a", "b", "c"});
}

TEST_CASE("multiline empty payload", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec};

    ml.log(make_msg("lg", spdlog::level::info, ""));

    REQUIRE(rec->payloads == std::vector<std::string>{""});
}

TEST_CASE("multiline empty lines preserved", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec};

    SECTION("leading and middle") {
        ml.log(make_msg("lg", spdlog::level::info, "\na\n\nb"));
        REQUIRE(rec->payloads == std::vector<std::string>{"", "a", "", "b"});
    }
    SECTION("only separator") {
        ml.log(make_msg("lg", spdlog::level::info, "\n"));
        REQUIRE(rec->payloads == std::vector<std::string>{""});
    }
    SECTION("trailing newline adds no phantom line") {
        ml.log(make_msg("lg", spdlog::level::info, "a\n"));
        REQUIRE(rec->payloads == std::vector<std::string>{"a"});
    }
    SECTION("two trailing newlines keep one blank line") {
        ml.log(make_msg("lg", spdlog::level::info, "a\n\n"));
        REQUIRE(rec->payloads == std::vector<std::string>{"a", ""});
    }
}

TEST_CASE("multiline drop empty lines", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec, false};

    SECTION("middle blanks dropped") {
        ml.log(make_msg("lg", spdlog::level::info, "\na\n\nb\n"));
        REQUIRE(rec->payloads == std::vector<std::string>{"a", "b"});
    }
    SECTION("fully empty payload becomes nothing") {
        ml.log(make_msg("lg", spdlog::level::info, ""));
        REQUIRE(rec->payloads.empty());
    }
}

TEST_CASE("multiline CRLF is one separator", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec};

    ml.log(make_msg("lg", spdlog::level::info, "a\r\nb\r\n"));

    REQUIRE(rec->payloads == std::vector<std::string>{"a", "b"});
}

TEST_CASE("multiline keeps same metadata per line", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec};

    auto msg = make_msg("logger-x", spdlog::level::err, "p1\np2\np3");
    ml.log(msg);

    REQUIRE(rec->payloads.size() == 3);
    for (size_t i = 0; i < 3; ++i) {
        REQUIRE(rec->levels[i] == msg.level);
        REQUIRE(rec->times[i] == msg.time);
        REQUIRE(rec->names[i] == "logger-x");
        REQUIRE(rec->tids[i] == msg.thread_id);
    }
}

TEST_CASE("multiline forwards formatter to wrapped sink", "[multiline_sink]") {
    auto test_sink = std::make_shared<spdlog::sinks::test_sink_mt>();
    spdlog::sinks::multiline_sink_st ml{test_sink};
    ml.set_pattern("%v");

    ml.log(make_msg("lg", spdlog::level::info, "one\ntwo"));

    REQUIRE(test_sink->lines() == std::vector<std::string>{"one", "two"});
}

#ifndef SPDLOG_NO_EXCEPTIONS
TEST_CASE("multiline failure stops remaining lines", "[multiline_sink]") {
    auto failing = std::make_shared<fail_on_nth_sink>(2);
    spdlog::sinks::multiline_sink_st ml{failing};

    REQUIRE_THROWS_AS(ml.log(make_msg("lg", spdlog::level::info, "l1\nl2\nl3")),
                      std::runtime_error);

    // line 1 written, line 2 threw, line 3 was never attempted.
    REQUIRE(failing->calls() == 2);
}
#endif

TEST_CASE("multiline flush forwards", "[multiline_sink]") {
    auto failing = std::make_shared<fail_on_nth_sink>(100);
    spdlog::sinks::multiline_sink_mt ml{failing};
    ml.flush();
    REQUIRE(failing->flush_calls() == 1);
}

TEST_CASE("multiline concurrent expansion is atomic", "[multiline_sink]") {
    auto slow = std::make_shared<slow_sink>();
    auto ml = std::make_shared<spdlog::sinks::multiline_sink_mt>(slow);
    barrier2 barrier;

    auto submit = [&](const std::string &payload) {
        barrier.arrive_and_wait();
        ml->log(make_msg("lg", spdlog::level::info, payload));
    };

    std::thread t1(submit, "A1\nA2\nA3");
    std::thread t2(submit, "B1\nB2\nB3");
    t1.join();
    t2.join();

    // The wrapper mutex must serialize the wrapped sink: never two calls in flight.
    REQUIRE(slow->max_in_flight.load() == 1);
    REQUIRE(slow->calls.load() == 6);
}

TEST_CASE("multiline write count", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    spdlog::sinks::multiline_sink_st ml{rec};

    ml.log(make_msg("lg", spdlog::level::info, "x\ny\nz"));
    ml.log(make_msg("lg", spdlog::level::info, "one"));
    ml.log(make_msg("lg", spdlog::level::info, ""));

    REQUIRE(rec->payloads.size() == 5);
    ml.flush();
    REQUIRE(rec->flush_calls == 1);
}

TEST_CASE("multiline async copies payload before background", "[multiline_sink]") {
    auto rec = std::make_shared<recording_sink>();
    auto ml = std::make_shared<spdlog::sinks::multiline_sink_mt>(rec);
    size_t queue_size = 128;
    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(queue_size, 1);
        auto logger = std::make_shared<spdlog::async_logger>(
            "as", ml, tp, spdlog::async_overflow_policy::block);

        // The backing strings are destroyed right after the log call returns; the
        // background thread must work on its own copy, never on these views.
        {
            std::string p1 = "async1\nasync2\nasync3";
            logger->info(spdlog::string_view_t{p1.data(), p1.size()});
        }
        {
            std::string p2 = "second1\nsecond2";
            logger->info(spdlog::string_view_t{p2.data(), p2.size()});
        }
        logger->flush();
    }

    REQUIRE(rec->payloads ==
            std::vector<std::string>{"async1", "async2", "async3", "second1", "second2"});
    REQUIRE(rec->flush_calls == 1);
    for (const auto &name : rec->names) {
        REQUIRE(name == "as");
    }
}
