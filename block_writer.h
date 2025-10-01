#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>
#include <cstdio>
#include <ctime>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <stdexcept>
#include <sys/mman.h>
#include "schemas.h"
#include "block_codec.h"
#ifndef MREMAP_MAYMOVE
#define MREMAP_MAYMOVE 1
#endif

struct BlockWriterOpt {
    std::string base_dir;
    std::string product;
    uint32_t fsync_every_blocks{0};
    uint32_t block_rows{8192};

    BlockWriterOpt(std::string base, std::string prod)
        : base_dir(std::move(base)), product(std::move(prod)) {
    }
};

#pragma pack(push,1)
struct DayFileHeader {
    uint64_t rows_total;
    uint64_t bytes_total;
    uint32_t yyyymmdd;
    uint32_t blocks_total;
};
#pragma pack(pop)

template <class Schema, class Codec>
class BlockWriterT {
public:
    using Row = typename Schema::Row;

    explicit BlockWriterT(const BlockWriterOpt& opt) : opt_(opt) {
    }

    ~BlockWriterT() { close(); }

    void begin_day(uint32_t yyyymmdd) {
        if (curr_day_ == yyyymmdd) {
            return;
        }
        flush_block();
        close();
        open_day_file(yyyymmdd);
        curr_day_ = yyyymmdd;
    }

    void write_row(const Row& r) {
        buf_.push_back(r);
        if (buf_.size() >= opt_.block_rows) {
            flush_block();
        }
    }

    void write_block(const Row* rows, uint32_t n) {
        if (n == 0) {
            return;
        }
        if (!is_open()) {
            throw std::runtime_error("[blockwriter]: write_block called w/o opening file");
        }
        if (!buf_.empty()) {
            flush_block();
        }
        append_rows_as_block(rows, n);
    }

    void close() {
        if (!is_open()) {
            return;
        }

        flush_block();
        close_map();

        header_.rows_total = rows_total_;
        header_.bytes_total = bytes_total_;

        ::ftruncate(fd_, static_cast<off_t>(file_off_));
        allocated_bytes_ = 0;

        ssize_t hs = ::pwrite(fd_, &header_, sizeof(header_), 0);
        if (hs != (ssize_t)sizeof(header_)) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("[blockwriter]: pwrite header failed");
        }
        ::fdatasync(fd_);

        ::close(fd_);
        fd_ = -1;

        path_.clear();
        rows_total_ = 0;
        bytes_total_ = 0;
        bytes_since_sync_ = 0;
        curr_day_ = 0;
        buf_.clear();
        map_base_ = nullptr;
        map_len_ = 0;
        file_off_ = 0;
    }

    bool is_open() const { return fd_ >= 0; }

private:
    static constexpr size_t SYNC_INTERVAL = 64ull << 20;
    static constexpr size_t MAP_WINDOW = 256ull << 20;
    static constexpr size_t FALLOCATE_CHUNK = 1ull << 30;

    BlockWriterOpt opt_;
    int fd_{-1};
    std::string path_;
    uint64_t allocated_bytes_{0};
    uint32_t curr_day_{0};
    DayFileHeader header_{};
    uint64_t rows_total_{0};
    uint64_t bytes_total_{0};
    uint32_t blocks_since_fsync_{0};
    size_t bytes_since_sync_{0};
    uint8_t* map_base_{nullptr};
    size_t map_len_{0};

    uint64_t file_off_{0};
    std::vector<Row> buf_;
    std::vector<uint8_t> block_buf_;

    static bool mkdir_p(const std::string& dir) {
        std::error_code ex;
        std::filesystem::create_directory(dir, ex);
        return !ex;
    }

    static std::string date_string(uint32_t yyyymmdd) {
        char buf[10];
        std::snprintf(buf, sizeof(buf), "%08u", yyyymmdd);
        return buf;
    }

    static inline size_t worst_case_block_bytes(uint32_t n_rows) {
        return sizeof(typename Codec::BlockHeader) + 18 * n_rows + 16;
    }

    static inline uint64_t align_up(uint64_t x, uint64_t a) {
        return (x + (a - 1)) / a * a;
    }

    void open_day_file(uint32_t yyyymmdd) {
        const std::string dir = opt_.base_dir + "/" + opt_.product + "-BLOCKS";
        if (!mkdir_p(dir)) {
            throw std::runtime_error("[blockwriter]: mkdir failed");
        }

        path_ = dir + "/" + date_string(yyyymmdd) + ".blocks";
        fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("[blockwriter]: open file failed");
        }

        const uint64_t first_target = sizeof(DayFileHeader) + MAP_WINDOW;
        const uint64_t first_round = align_up(first_target, FALLOCATE_CHUNK);
        int rc = ::posix_fallocate(fd_, 0, static_cast<off_t>(first_round));
        if (rc != 0) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("[blockwriter]: posix_fallocate failed");
        }

        allocated_bytes_ = first_round;

        const long page = ::sysconf(_SC_PAGESIZE);
        size_t init_map_len = std::max<uint64_t>(
            align_up(sizeof(DayFileHeader) + MAP_WINDOW, page), MAP_WINDOW);
        void* p = ::mmap(nullptr, init_map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);

        if (p == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("[blockwriter]: mmap failed");
        }

        map_base_ = static_cast<uint8_t*>(p);
        map_len_ = init_map_len;
        std::memset(&header_, 0, sizeof(header_));
        header_.yyyymmdd = yyyymmdd;
        header_.rows_total = 0;
        header_.bytes_total = 0;
        std::memcpy(map_base_, &header_, sizeof(header_));
        ::msync(map_base_, sizeof(DayFileHeader), MS_SYNC);
        file_off_ = sizeof(DayFileHeader);
        ::posix_fadvise(fd_, 0, static_cast<off_t>(map_len_), POSIX_FADV_SEQUENTIAL);
        ::madvise(map_base_, map_len_, MADV_SEQUENTIAL);

        std::fprintf(stdout, "[blockwriter:%u] opened %s\n", yyyymmdd, path_.c_str());
    }

    void close_map() {
        if (map_base_) {
            ::munmap(map_base_, map_len_);
            map_base_ = nullptr;
            map_len_ = 0;
        }
    }

    void ensure_chunk(size_t need) {
        if (map_base_ && map_len_ >= file_off_ + need) {
            return;
        }

        const uint64_t min_len = file_off_ + need;
        ensure_allocated(min_len);

        size_t new_len = map_len_;
        while (new_len < min_len) {
            new_len += MAP_WINDOW;
        }

        void* p = ::mremap(map_base_, map_len_, new_len, MREMAP_MAYMOVE);
        if (p == MAP_FAILED) {
            throw std::runtime_error("[blockwriter]: mremap failed");
        }
        map_base_ = static_cast<uint8_t*>(p);
        map_len_ = new_len;


        ::posix_fadvise(fd_, 0, static_cast<off_t>(map_len_), POSIX_FADV_SEQUENTIAL);
        ::madvise(map_base_, map_len_, MADV_SEQUENTIAL);
    }

    void ensure_allocated(uint64_t required_len) {
        if (required_len <= allocated_bytes_) {
            return;
        }

        const uint64_t rounded = align_up(required_len, FALLOCATE_CHUNK);
        int rc = ::posix_fallocate(fd_, 0, static_cast<off_t>(rounded));
        if (rc != 0) {
            throw std::runtime_error("[blockwriter]: posix_fallocate failed");
        }
        allocated_bytes_ = rounded;
    }

    void append_rows_as_block(const Row* rows, uint32_t n) {
        block_buf_.clear();
        Codec::encode_block(rows, n, block_buf_);
        ensure_chunk(block_buf_.size());
        //std::cout << "encoded sz: " <<  block_buf_.size() << std::endl;
        std::memcpy(map_base_ + file_off_, block_buf_.data(), block_buf_.size());

        file_off_ += block_buf_.size();
        rows_total_ += n;
        bytes_total_ += block_buf_.size();
        bytes_since_sync_ += block_buf_.size();

        if (bytes_since_sync_ >= SYNC_INTERVAL) {
            ::fdatasync(fd_);
            bytes_since_sync_ = 0;
        }
        header_.blocks_total++;
    }

    void flush_block() {
        if (!is_open() || buf_.empty()) {
            return;
        }
        append_rows_as_block(buf_.data(), static_cast<uint32_t>(buf_.size()));
        buf_.clear();
        ::fdatasync(fd_);
    }
};
