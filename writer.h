#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <filesystem>
#include <type_traits>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "schemas.h"
#include "../utils/spsc.h"

static constexpr uint64_t HUGE_PAGE_SIZE = 2ull * 1024 * 1024;

struct WriterOpt {
    std::string base_dir;
    std::string product;
    static constexpr uint64_t rows_per_hr = 1ull << 24;
    uint32_t fsync_every_rows{0};

    WriterOpt(std::string base, std::string prod) : base_dir(std::move(base)), product(std::move(prod)) {
    }
};

template <class Schema>
class WriterT {
public:
    using Row = typename Schema::Row;
    using Header = ColFileHeaderT<Schema>;

    explicit WriterT(const WriterOpt& opt) : opt_(opt) {
    }

    ~WriterT() {
        stop();
        join();
        close_file();
    }

    void start() {
        running_.store(true, std::memory_order_release);
        stop_.store(false, std::memory_order_release);
        thread_ = std::make_unique<std::thread>(&WriterT::run, this);
        std::cout << opt_.base_dir << "/" << opt_.product << std::endl;
    }

    void stop() { stop_.store(true, std::memory_order_release); }
    void join() { if (thread_ && thread_->joinable()) thread_->join(); }

    bool enqueue(const Row& r) noexcept { return queue_.enqueue(r); }
    uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
    uint64_t rows() const noexcept { return rows_.load(std::memory_order_acquire); }
    uint64_t hour_s() const noexcept { return day_start_; } // kept name for ABI; now returns day start

private:
    int fd_{-1};
    uint8_t* base_{nullptr};
    size_t map_bytes_{0};
    Header hdr_{};
    uint64_t col_off_[Schema::COLS]{};
    uint64_t col_sz_[Schema::COLS]{};
    void* col_ptrs_[Schema::COLS]{};
    std::atomic<uint64_t> rows_{0};
    std::atomic<uint64_t> dropped_{0};
    uint64_t capacity_{WriterOpt::rows_per_hr};
    uint64_t day_start_{~0ull};
    WriterOpt opt_;
    static constexpr size_t kQueueCapacity = (1ull << 26);
    LockFreeQueue<Row, kQueueCapacity> queue_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};

    static bool mkdir_p(const std::string& dir) {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        return !ec;
    }

    static std::string date_string(uint64_t epoch_s) {
        auto tt = static_cast<time_t>(epoch_s);
        std::tm tm{};
        localtime_r(&tt, &tm);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%04d%02d%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
        return buf;
    }

    static bool preallocate(int fd, size_t bytes) {
        return ::posix_fallocate(fd, 0, bytes) == 0;
    }

    static inline uint64_t day_from_hour(uint64_t hour_s) noexcept {
        return hour_s - (hour_s % 86400ull);
    }

    void run() {
        uint32_t since_fsync = 0;

        while (running_.load(std::memory_order_acquire)) {
            if (stop_.load(std::memory_order_acquire) && queue_.empty()) break;

            auto r = queue_.dequeue();
            if (!r) {
                std::this_thread::yield();
                continue;
            }

            const Row row = *r;

            const uint64_t h = Schema::hour_from_row(row);
            const uint64_t d = day_from_hour(h);
            if (d != day_start_) {
                if (!rotate_to_day(d)) {
                    dropped_.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

            }

            const uint64_t idx = rows_.fetch_add(1, std::memory_order_acq_rel);
            if (idx >= capacity_) {
                if (!grow_file()) {
                    rows_.store(capacity_, std::memory_order_release);
                    dropped_.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
            }

            Schema::write_row_to_cols(row, col_ptrs_, idx);

            if (opt_.fsync_every_rows) {
                if (++since_fsync >= opt_.fsync_every_rows) {
                    update_rows_in_header();
                    since_fsync = 0;
                }
            }
        }
        update_rows_in_header();
        running_.store(false, std::memory_order_release);
    }

    bool rotate_to_day(uint64_t day_s) {
        if (day_start_ == day_s) {
            return true;
        }
        std::cout << rows_ << std::endl;
        update_rows_in_header();
        close_file();
        day_start_ = day_s;
        return open_day_file(day_s);
    }

    void close_file() {
        if (!base_) {
            return;
        }
        ::msync(base_, HEADER_SZ, MS_SYNC);
        ::munmap(base_, map_bytes_);
        base_ = nullptr;
        map_bytes_ = 0;
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        rows_.store(0, std::memory_order_release);
        std::memset(&hdr_, 0, sizeof(hdr_));
        std::memset(col_off_, 0, sizeof(col_off_));
        std::memset(col_sz_, 0, sizeof(col_sz_));
        std::memset(col_ptrs_, 0, sizeof(col_ptrs_));
    }

    static constexpr size_t HEADER_SZ = 256;

    bool open_day_file(uint64_t day_s) {
        capacity_ = WriterOpt::rows_per_hr * 2ull;

        size_t cols_bytes = 0;
        for (uint32_t i = 0; i < Schema::COLS; ++i) {
            col_sz_[i] = capacity_ * Schema::col_size(i);
            cols_bytes += col_sz_[i];
        }
        const size_t file_bytes = HEADER_SZ + cols_bytes;

        const std::string dir = opt_.base_dir + "/" + opt_.product;
        if (!mkdir_p(dir)) {
            return false;
        }
        const std::string path = dir + "/" + date_string(day_s) + ".bin";
        //const std::string path = dir + "/" + "20240815" + ".bin";

        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            return false;
        }

        if (!preallocate(fd_, file_bytes)) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }

        map_bytes_ = file_bytes;
        base_ = static_cast<uint8_t*>(::mmap(nullptr, map_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (base_ == MAP_FAILED) {
            base_ = nullptr;
            ::close(fd_);
            fd_ = -1;
            return false;
        }

        std::memset(&hdr_, 0, sizeof(hdr_));
        std::memcpy(hdr_.magic, Schema::MAGIC, 6);
        hdr_.header_size = static_cast<uint16_t>(HEADER_SZ);
        hdr_.version = Schema::VERSION;
        std::snprintf(hdr_.product, sizeof(hdr_.product), "%s", opt_.product.c_str());
        hdr_.hour_epoch_start = day_start_;
        hdr_.rows = 0;
        hdr_.capacity = capacity_;

        uint64_t off = HEADER_SZ;
        for (uint32_t i = 0; i < Schema::COLS; ++i) {
            hdr_.col_off[i] = off;
            col_off_[i] = off;
            hdr_.col_sz[i] = Schema::col_size(i);
            off += col_sz_[i];
        }
        std::memcpy(base_, &hdr_, sizeof(hdr_));
        ::msync(base_, HEADER_SZ, MS_SYNC);

        for (uint32_t i = 0; i < Schema::COLS; ++i) {
            col_ptrs_[i] = base_ + col_off_[i];
        }
        rows_.store(0, std::memory_order_release);
        return true;
    }

    bool grow_file() {
        const uint64_t new_capacity = capacity_ * 2ull;
        std::cout << "Growing file capacity from " << capacity_ << " to " << new_capacity << std::endl;

        size_t new_cols_bytes = 0;
        for (uint32_t i = 0; i < Schema::COLS; ++i) {
            col_sz_[i] = new_capacity * Schema::col_size(i);
            new_cols_bytes += col_sz_[i];
        }
        const size_t new_file_bytes = HEADER_SZ + new_cols_bytes;

        ::munmap(base_, map_bytes_);
        if (!preallocate(fd_, new_file_bytes)) {
            std::cerr << "Failed to grow file" << std::endl;
            return false;
        }

        map_bytes_ = new_file_bytes;
        base_ = static_cast<uint8_t*>(::mmap(nullptr, map_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (base_ == MAP_FAILED) {
            base_ = nullptr;
            return false;
        }

        capacity_ = new_capacity;
        hdr_.capacity = capacity_;

        uint64_t off = HEADER_SZ;
        for (uint32_t i = 0; i < Schema::COLS; ++i) {
            hdr_.col_off[i] = off;
            col_off_[i] = off;
            hdr_.col_sz[i] = Schema::col_size(i);
            col_ptrs_[i] = base_ + col_off_[i];
            off += col_sz_[i];
        }

        std::memcpy(base_, &hdr_, sizeof(hdr_));
        ::msync(base_, HEADER_SZ, MS_SYNC);
        return true;
    }

    bool update_rows_in_header() {
        if (!base_) {
            return true;
        }
        hdr_.rows = rows_.load(std::memory_order_acquire);
        std::memcpy(base_, &hdr_, sizeof(hdr_));
        return ::msync(base_, HEADER_SZ, MS_SYNC) == 0;
    }
};
