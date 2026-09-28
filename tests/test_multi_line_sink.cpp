#include "includes.h"
#include "spdlog/async.h"
#include "spdlog/sinks/base_sink-inl.h"
#include "spdlog/sinks/multi_line_sink.h"
#include "test_sink.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <thread>

using spdlog::details::log_msg;

namespace {

struct recorded_line {
    std::string payload;
    std::string logger_name;
    spdlog::level::level_enum level;
    spdlog::log_clock::time_point time;
    size_t thread_id;
    spdlog::source_loc source;
};

// sink that stores a deep copy of every forwarded log_msg and can inject a
// failure / delay on the wrapped (inner) sink level
class recording_sink : public spdlog::sinks::sink {
public:
    std::vector<recorded_line> records;
    std::chrono::milliseconds delay{std::chrono::milliseconds::zero()};
    size_t log_calls{0};
    size_t flush_calls{0};
    size_t throw_on_call{0};  // 0 = never throw, otherwise 1-based call index

    void log(const spdlog::details::log_msg &msg) override {
        size_t call = ++log_calls;
        if (delay.count() > 0) {
            std::this_thread::sleep_for(delay);
        }
        if (throw_on_call != 0 && call == throw_on_call) {
            throw std::runtime_error("recording_sink injected failure");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        records.push_back(recorded_line{
            std::string(msg.payload.data(), msg.payload.size()),
            std::string(msg.logger_name.data(), msg.logger_name.size()),
            msg.level, msg.time, msg.thread_id, msg.source});
    }

    void flush() override { ++flush_calls; }
    void set_pattern(const std::string &) override {}
    void set_formatter(std::unique_ptr<spdlog::formatter>) override {}

    std::vector<std::string> payloads() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> result;
        result.reserve(records.size());
        for (const auto &record : records) {
            result.push_back(record.payload);
        }
        return result;
    }

private:
    std::mutex mutex_;
};

// minimal reusable barrier for N parties
class barrier_n {
public:
    explicit barrier_n(size_t parties)
        : parties_{parties} {}

    void arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t generation = generation_;
        if (++arrived_ == parties_) {
            arrived_ = 0;
            ++generation_;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [&] { return generation_ != generation; });
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t parties_;
    size_t arrived_{0};
    size_t generation_{0};
};

// mutex that counts its own lock/unlock operations
struct counting_mutex {
    static std::atomic<size_t> lock_count;
    static std::atomic<size_t> unlock_count;

    std::mutex impl_;

    void lock() {
        impl_.lock();
        ++lock_count;
    }

    void unlock() {
        ++unlock_count;
        impl_.unlock();
    }

    static void reset() {
        lock_count = 0;
        unlock_count = 0;
    }
};

std::atomic<size_t> counting_mutex::lock_count{0};
std::atomic<size_t> counting_mutex::unlock_count{0};

}  // namespace

TEST_CASE("multi_line basic split with shared metadata", "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    spdlog::sinks::multi_line_sink_mt sink(inner);

    auto when = spdlog::log_clock::now();
    spdlog::source_loc loc{"my_file.cpp", 42, "my_func"};
    log_msg msg(when, loc, "my_logger", spdlog::level::warn, "line1\nline2\nline3");
    msg.thread_id = 1234;
    sink.log(msg);

    REQUIRE(inner->log_calls == 3);
    REQUIRE(inner->payloads() ==
            std::vector<std::string>{"line1", "line2", "line3"});

    for (const auto &record : inner->records) {
        REQUIRE(record.logger_name == "my_logger");
        REQUIRE(record.level == spdlog::level::warn);
        REQUIRE(record.time == when);
        REQUIRE(record.thread_id == 1234);
        REQUIRE(record.source.filename == std::string("my_file.cpp"));
        REQUIRE(record.source.line == 42);
        REQUIRE(record.source.funcname == std::string("my_func"));
    }
}

TEST_CASE("multi_line empty message and empty lines", "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    spdlog::sinks::multi_line_sink_mt sink(inner);

    SECTION("empty payload still forwards one empty line") {
        sink.log(log_msg("lg", spdlog::level::info, ""));
        REQUIRE(inner->log_calls == 1);
        REQUIRE(inner->payloads() == std::vector<std::string>{""});
    }

    SECTION("single newline is one empty line") {
        sink.log(log_msg("lg", spdlog::level::info, "\n"));
        REQUIRE(inner->payloads() == std::vector<std::string>{""});
    }

    SECTION("trailing newline adds no extra line") {
        sink.log(log_msg("lg", spdlog::level::info, "a\n"));
        REQUIRE(inner->payloads() == std::vector<std::string>{"a"});
    }

    SECTION("empty lines are preserved") {
        sink.log(log_msg("lg", spdlog::level::info, "a\n\nb\n"));
        REQUIRE(inner->payloads() ==
                std::vector<std::string>{"a", "", "b"});
    }

    SECTION("only newlines") {
        sink.log(log_msg("lg", spdlog::level::info, "\n\n"));
        REQUIRE(inner->payloads() ==
                std::vector<std::string>{"", ""});
    }
}

TEST_CASE("multi_line CRLF handling", "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    spdlog::sinks::multi_line_sink_mt sink(inner);

    SECTION("CRLF is a single separator") {
        sink.log(log_msg("lg", spdlog::level::info, "a\r\nb\r\nc"));
        REQUIRE(inner->payloads() ==
                std::vector<std::string>{"a", "b", "c"});
    }

    SECTION("CRLF with empty lines and trailing newline") {
        sink.log(log_msg("lg", spdlog::level::info, "a\r\n\r\nb\r\n"));
        REQUIRE(inner->payloads() ==
                std::vector<std::string>{"a", "", "b"});
    }

    SECTION("lone CR is kept as payload") {
        sink.log(log_msg("lg", spdlog::level::info, "a\rb\nc"));
        REQUIRE(inner->payloads() ==
                std::vector<std::string>{"a\rb", "c"});
    }
}

TEST_CASE("multi_line keeps each message contiguous across threads",
          "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    inner->delay = std::chrono::milliseconds(2);  // widen contention windows
    auto sink = std::make_shared<spdlog::sinks::multi_line_sink_mt>(inner);

    for (int iteration = 0; iteration < 20; ++iteration) {
        inner->records.clear();
        barrier_n barrier(2);

        auto submit = [&](char tag) {
            std::string payload = std::string(1, tag) + "1\n" +
                                  std::string(1, tag) + "2\n" +
                                  std::string(1, tag) + "3";
            barrier.arrive_and_wait();
            sink->log(log_msg("lg", spdlog::level::info, payload));
        };

        std::thread thread_a(submit, 'A');
        std::thread thread_b(submit, 'B');
        thread_a.join();
        thread_b.join();

        REQUIRE(inner->records.size() == 6);
        std::vector<std::string> payloads;
        payloads.reserve(6);
        for (const auto &record : inner->records) {
            payloads.push_back(record.payload);
        }

        // each message's 3 lines must appear as one contiguous block
        const std::vector<std::string> block_a{"A1", "A2", "A3"};
        const std::vector<std::string> block_b{"B1", "B2", "B3"};
        bool contiguous_a = false;
        bool contiguous_b = false;
        for (size_t i = 0; i + 3 <= payloads.size(); ++i) {
            std::vector<std::string> window{
                payloads.begin() + static_cast<std::ptrdiff_t>(i),
                payloads.begin() + static_cast<std::ptrdiff_t>(i + 3)};
            contiguous_a = contiguous_a || window == block_a;
            contiguous_b = contiguous_b || window == block_b;
        }
        REQUIRE(contiguous_a);
        REQUIRE(contiguous_b);
    }
}

TEST_CASE("multi_line failure on second line stops the rest",
          "[multi_line_sink]") {
    SECTION("exception propagates straight through the wrapper") {
        auto inner = std::make_shared<recording_sink>();
        inner->throw_on_call = 2;
        spdlog::sinks::multi_line_sink_mt sink(inner);

        REQUIRE_THROWS_AS(
            sink.log(log_msg("lg", spdlog::level::info, "one\ntwo\nthree")),
            std::runtime_error);

        // first line written, second attempted and failed, third never attempted
        REQUIRE(inner->log_calls == 2);
        REQUIRE(inner->payloads() == std::vector<std::string>{"one"});

        // the wrapper stays usable and its lock was released
        inner->throw_on_call = 0;
        sink.log(log_msg("lg", spdlog::level::info, "x\ny"));
        REQUIRE(inner->payloads() ==
                std::vector<std::string>{"one", "x", "y"});
    }

    SECTION("logger routes the failure through its error handler, no retry") {
        auto inner = std::make_shared<recording_sink>();
        inner->throw_on_call = 2;
        auto sink = std::make_shared<spdlog::sinks::multi_line_sink_mt>(inner);
        size_t error_count = 0;
        spdlog::logger logger("err_logger", sink);
        logger.set_error_handler([&](const std::string &) { ++error_count; });

        logger.log(spdlog::level::info, "one\ntwo\nthree");

        REQUIRE(error_count == 1);
        REQUIRE(inner->log_calls == 2);
        REQUIRE(inner->payloads() == std::vector<std::string>{"one"});
    }
}

TEST_CASE("multi_line flush is forwarded", "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    spdlog::sinks::multi_line_sink_mt sink(inner);

    sink.log(log_msg("lg", spdlog::level::info, "a\nb"));
    REQUIRE(inner->flush_calls == 0);
    sink.flush();
    REQUIRE(inner->flush_calls == 1);
}

TEST_CASE("multi_line takes the wrapper lock once for the whole message",
          "[multi_line_sink]") {
    using wrapper_sink = spdlog::sinks::multi_line_sink<counting_mutex>;
    counting_mutex::reset();

    auto inner = std::make_shared<recording_sink>();
    auto sink = std::make_shared<wrapper_sink>(inner);

    sink->log(log_msg("lg", spdlog::level::info, "a\nb\nc"));
    REQUIRE(inner->log_calls == 3);
    REQUIRE(counting_mutex::lock_count == 1);
    REQUIRE(counting_mutex::unlock_count == 1);

    sink->flush();
    REQUIRE(inner->flush_calls == 1);
    REQUIRE(counting_mutex::lock_count == 2);
    REQUIRE(counting_mutex::unlock_count == 2);

    // a failure still releases the lock
    inner->throw_on_call = 2;
    inner->log_calls = 0;
    REQUIRE_THROWS_AS(sink->log(log_msg("lg", spdlog::level::info, "x\ny\nz")),
                      std::runtime_error);
    REQUIRE(counting_mutex::lock_count == 3);
    REQUIRE(counting_mutex::unlock_count == 3);
}

TEST_CASE("multi_line forwards pattern to the wrapped sink",
          "[multi_line_sink]") {
    auto inner = std::make_shared<spdlog::sinks::test_sink_mt>();
    spdlog::sinks::multi_line_sink_mt sink(inner);
    sink.set_pattern("[%n] %v");

    sink.log(log_msg("lg", spdlog::level::info, "first\nsecond"));

    auto lines = inner->lines();
    REQUIRE(lines.size() == 2);
    REQUIRE(lines[0] == "[lg] first");
    REQUIRE(lines[1] == "[lg] second");
}

TEST_CASE("multi_line honors the sink level from the logger",
          "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    auto sink = std::make_shared<spdlog::sinks::multi_line_sink_mt>(inner);
    sink->set_level(spdlog::level::warn);
    spdlog::logger logger("lg", sink);

    logger.log(spdlog::level::info, "a\nb\nc");
    REQUIRE(inner->log_calls == 0);

    logger.log(spdlog::level::err, "d\ne");
    REQUIRE(inner->log_calls == 2);
    REQUIRE(inner->payloads() == std::vector<std::string>{"d", "e"});
}

TEST_CASE("multi_line async logger copies payload before queuing",
          "[multi_line_sink]") {
    auto inner = std::make_shared<recording_sink>();
    // keep the worker busy so the queued entry outlives the caller's buffer
    inner->delay = std::chrono::milliseconds(20);
    auto sink = std::make_shared<spdlog::sinks::multi_line_sink_mt>(inner);

    {
        auto tp = std::make_shared<spdlog::details::thread_pool>(128, 1);
        auto logger =
            std::make_shared<spdlog::async_logger>("as", sink, tp);

        {
            // buffer built on the calling thread's stack and destroyed below
            std::string local = "async1";
            local += "\nasync2\nasync3";
            logger->log(spdlog::level::info, spdlog::string_view_t{local});
        }
        // local is gone here; flush waits for the worker to finish processing
        logger->flush();
    }

    REQUIRE(inner->log_calls == 3);
    REQUIRE(inner->flush_calls == 1);
    REQUIRE(inner->payloads() ==
            std::vector<std::string>{"async1", "async2", "async3"});

    for (const auto &record : inner->records) {
        REQUIRE(record.logger_name == "as");
        REQUIRE(record.level == spdlog::level::info);
    }
}
