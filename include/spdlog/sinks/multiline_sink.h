// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/details/log_msg.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/pattern_formatter.h>

#include <cstddef>
#include <memory>
#include <mutex>

// Multi line sink.
// Wraps another sink and splits a single log_msg payload into lines. Each line
// is forwarded to the wrapped sink as a separate log_msg while sharing the
// original timestamp, level, logger name, thread id and source location, so a
// multi line log message keeps one consistent prefix per line.
//
// "\n" and "\r\n" are both treated as a single line separator. A trailing
// separator does not produce an extra empty line. With keep_empty_lines (the
// default), empty lines in the middle of the payload are preserved. A completely
// empty payload is still forwarded as a single empty line.
//
// All log() and flush() calls of this sink are serialized with one mutex for
// the whole expansion of a message, so the lines of one message are never
// interleaved with lines from another thread. This guarantee only covers
// messages written through this wrapper.
//
// If the wrapped sink throws while writing a line, the remaining lines of the
// current message are not written and the exception propagates through the
// normal error handling path; they are not retried as new messages.
//
// Example:
//
//     #include <spdlog/sinks/multiline_sink.h>
//
//     auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs.txt");
//     auto ml_sink = std::make_shared<spdlog::sinks::multiline_sink_mt>(file_sink);
//     spdlog::logger l("logger", ml_sink);
//     l.info("line1\nline2\nline3");

SPDLOG_NAMESPACE_BEGIN
namespace sinks {

template <typename Mutex>
class multiline_sink : public base_sink<Mutex> {
public:
    explicit multiline_sink(std::shared_ptr<sink> wrapped, bool keep_empty_lines = true)
        : wrapped_sink_{std::move(wrapped)}, keep_empty_lines_{keep_empty_lines} {}

    multiline_sink(const multiline_sink &) = delete;
    multiline_sink &operator=(const multiline_sink &) = delete;

protected:
    void sink_it_(const details::log_msg &msg) override {
        const string_view_t payload = msg.payload;

        size_t line_start = 0;
        for (size_t i = 0; i < payload.size(); ++i) {
            if (payload[i] != '\n') {
                continue;
            }

            size_t line_end = i;
            // "\r\n" is a single separator, so a trailing '\r' is not part of the line.
            if (line_end > line_start && payload[line_end - 1] == '\r') {
                --line_end;
            }
            forward_line_(msg, string_view_t{payload.data() + line_start, line_end - line_start});
            line_start = i + 1;
        }

        // Forward the segment after the last separator. When the payload ends with
        // '\n', line_start == size and there is no trailing segment, so a trailing
        // newline does not produce a phantom extra line. An empty payload still
        // reaches forward_line_() once as a single empty line.
        if (line_start < payload.size() || payload.size() == 0) {
            forward_line_(msg, string_view_t{payload.data() + line_start,
                                              payload.size() - line_start});
        }
    }

    void flush_() override { wrapped_sink_->flush(); }

    void set_pattern_(const std::string &pattern) override {
        set_formatter_(details::make_unique<pattern_formatter>(pattern));
    }

    void set_formatter_(std::unique_ptr<formatter> sink_formatter) override {
        base_sink<Mutex>::formatter_ = std::move(sink_formatter);
        wrapped_sink_->set_formatter(base_sink<Mutex>::formatter_->clone());
    }

private:
    void forward_line_(const details::log_msg &msg, string_view_t line) {
        if (!keep_empty_lines_ && line.size() == 0) {
            return;
        }
        details::log_msg line_msg = msg;
        line_msg.payload = line;
        wrapped_sink_->log(line_msg);
    }

    std::shared_ptr<sink> wrapped_sink_;
    bool keep_empty_lines_;
};

using multiline_sink_mt = multiline_sink<std::mutex>;
using multiline_sink_st = multiline_sink<details::null_mutex>;

}  // namespace sinks
SPDLOG_NAMESPACE_END
