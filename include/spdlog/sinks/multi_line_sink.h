// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/details/log_msg.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>

#include <memory>
#include <mutex>

// Multi-line sink wrapper.
// Splits the payload of a single log_msg into lines and forwards each line
// to the wrapped sink as its own log_msg, keeping the original timestamp,
// level, logger name, thread id and source location.
//
// The wrapper mutex is taken once per log/flush call and held until every
// line of the message has been written, so the lines of one message are never
// interleaved with messages from other threads that go through this wrapper.
// This guarantee only covers messages written through the wrapper.
//
// Line splitting rules:
// * "\r\n" is treated as a single line separator.
// * Empty lines between separators are preserved.
// * A trailing newline does not produce an extra empty line.
// * An empty payload is forwarded as a single empty line.
//
// If the wrapped sink throws while writing a line, the remaining lines of the
// current message are not written (or retried); the exception propagates to
// the caller, going through the regular logger error handling.
//
// Example:
//
//     #include <spdlog/sinks/multi_line_sink.h>
//     #include <spdlog/sinks/stdout_color_sinks.h>
//
//     auto console = std::make_shared<stdout_color_sink_mt>();
//     auto multi_line = std::make_shared<multi_line_sink_mt>(console);
//     spdlog::logger logger("logger", multi_line);
//     logger.info("line1\nline2\r\nline3");

SPDLOG_NAMESPACE_BEGIN
namespace sinks {

template <typename Mutex>
class multi_line_sink : public base_sink<Mutex> {
public:
    explicit multi_line_sink(std::shared_ptr<sink> wrapped_sink)
        : wrapped_sink_{std::move(wrapped_sink)} {}

    multi_line_sink(const multi_line_sink &) = delete;
    multi_line_sink(multi_line_sink &&) = delete;
    multi_line_sink &operator=(const multi_line_sink &) = delete;
    multi_line_sink &operator=(multi_line_sink &&) = delete;

protected:
    void sink_it_(const details::log_msg &msg) override {
        const string_view_t payload = msg.payload;
        size_t line_start = 0;
        for (size_t pos = 0; pos < payload.size(); ++pos) {
            if (payload[pos] == '\n') {
                size_t line_end = pos;
                // treat a preceding '\r' as part of the CRLF separator
                if (line_end > line_start && payload[line_end - 1] == '\r') {
                    --line_end;
                }
                log_line_(msg, payload.data() + line_start, line_end - line_start);
                line_start = pos + 1;
            }
        }

        // emit the last line only when the message does not end with a newline;
        // an empty payload is still forwarded as a single empty line
        if (payload.size() == 0 || line_start != payload.size()) {
            log_line_(msg, payload.data() + line_start, payload.size() - line_start);
        }
    }

    void flush_() override { wrapped_sink_->flush(); }

    void set_pattern_(const std::string &pattern) override {
        wrapped_sink_->set_pattern(pattern);
    }

    void set_formatter_(std::unique_ptr<formatter> sink_formatter) override {
        wrapped_sink_->set_formatter(std::move(sink_formatter));
    }

private:
    void log_line_(const details::log_msg &msg,
                   const char *line_data,
                   size_t line_size) {
        details::log_msg line_msg = msg;
        line_msg.payload = string_view_t{line_data, line_size};
        wrapped_sink_->log(line_msg);
    }

    std::shared_ptr<sink> wrapped_sink_;
};

using multi_line_sink_mt = multi_line_sink<std::mutex>;
using multi_line_sink_st = multi_line_sink<details::null_mutex>;

}  // namespace sinks
SPDLOG_NAMESPACE_END
