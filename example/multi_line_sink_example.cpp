// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

// Demonstrates the multi_line_sink wrapper: a message containing newlines is
// split line by line with the same timestamp/level/logger info, and the whole
// expansion is serialized so other threads cannot interleave it.

#include "spdlog/sinks/multi_line_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#include <thread>

int main() {
    auto console = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto multi_line =
        std::make_shared<spdlog::sinks::multi_line_sink_mt>(console);
    spdlog::logger logger("multi", multi_line);
    logger.set_pattern("[%H:%M:%S.%e] [%n] [%^%l%$] %v");

    // one log call, three physical lines, identical header on each line
    logger.info("request received\nuser: spdlog\nresult: ok");

    // CRLF is one separator; empty lines are kept; trailing newline adds none
    logger.info("a\r\nb\n\nc\n");

    // two threads logging 3-line messages: each block stays contiguous
    std::thread t1([&] { logger.info("T1 line1\nT1 line2\nT1 line3"); });
    std::thread t2([&] { logger.warn("T2 line1\nT2 line2\nT2 line3"); });
    t1.join();
    t2.join();

    logger.flush();
    return 0;
}
