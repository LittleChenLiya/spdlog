// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

// multiline_sink wraps another sink so one multi line log message is expanded
// into per-line log calls under one lock, keeping each line together even with
// concurrent threads.

#include "spdlog/sinks/multiline_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

int main() {
    auto console = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto ml_sink = std::make_shared<spdlog::sinks::multiline_sink_mt>(console);
    spdlog::logger logger("multi", ml_sink);

    logger.info("one log call produces: \nsecond line\nthird line");
    logger.info("empty line below:\n\nback again");
    logger.info("crlf is one separator:\nend");
    logger.flush();
    return 0;
}
