#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>
#include <functional>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "schemas.h"
#include "block_codec.h"

struct BlockReaderOpt {
    std::string base_dir;
    std::string product;
    uint32_t date_from = 00000000;
    uint32_t date_to = 99999999;
};

template <class Schema, class Codec>
class BlockReaderT {
public:

    BlockReaderT(BlockReaderOpt& opt)
        : opt_(std::move(opt)) {
        build_day_file_list();
    }

    using Row = typename Schema::Row;

    struct RowsView {
        const Row* data;
        uint32_t n_rows;
        size_t file_offset;
        uint32_t yyyymmdd;
    };

    template <class Fn>
    void visit_day_files(Fn&& fn) {
        for (int i = 0; i < files_.size(); i++) {
            map(files_[i].path);

            const size_t file_begin = sizeof(DayFileHeader);
            const size_t file_limit = std::min<size_t>(file_begin + hdr_.bytes_total,
                                                       mapped_bytes_);

            size_t off = file_begin;
            size_t count = 0;
            const uint32_t max_blocks = hdr_.blocks_total;

            rows_.clear();

            while (off < file_limit && count < max_blocks) {
                uint8_t* blk = base_ + off;
                size_t len = file_limit - off;
                size_t consumed = Codec::decode_block(blk, len, rows_);
                if (consumed == 0) {
                    break;
                }
                if (off + consumed > file_limit) {
                    break;
                }

                RowsView view{rows_.data(), static_cast<uint32_t>(rows_.size()), off, hdr_.yyyymmdd};
                fn(view);

                off += consumed;
                count += 1;
            }
            unmap_();
        }
    }

private:

    struct DayFile {
        uint32_t yyyymmdd;
        fs::path path;
    };

    bool parse_yyyymmdd(std::string_view name, uint32_t& out) {
        if (name.size() < 12) {
            return false;
        }
        std::string_view stem = name.substr(name.size() - 12, 8);
        uint32_t val{};
        auto [p, ec] = std::from_chars(stem.data(), stem.data()+8, val);
        if (ec != std::errc{} || p != stem.data()+8) {
            return false;
        }
        out = val;
        return true;
    }

    void build_day_file_list() {
        const fs::path dir = fs::path(opt_.base_dir) / opt_.product;
        if (!fs::exists(dir)) {
            return;
        }

        for (const auto& e : fs::directory_iterator(dir)) {
            if (!e.is_regular_file()) {
                continue;
            }
            if (e.path().extension() != ".bin") {
                continue;
            }

            uint32_t d{};
            const auto name = e.path().filename().string();
            if (!parse_yyyymmdd(name, d)) {
                continue;
            }
            if (d < opt_.date_from || d > opt_.date_to) {
                continue;
            }
            files_.push_back(DayFile{d, e.path()});
        }
        std::sort(files_.begin(), files_.end(),
                  [](const DayFile& a, const DayFile& b) { return a.yyyymmdd < b.yyyymmdd; });
        for (auto& f : files_) {
            days_.push_back(f.yyyymmdd);
            paths_only_.push_back(f.path);
        }
        //std::cout << days_.size() << std::endl;
    }



    void map(const fs::path& path) {
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0) {
            throw std::runtime_error("[blockreader] open failed");
        }

        struct stat st{};
        if (::fstat(fd_, &st) != 0 || st.st_size < static_cast<off_t>(sizeof(DayFileHeader))) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("[blockreader] fstat/header too small");
        }

        mapped_bytes_ = static_cast<size_t>(st.st_size);
        base_ = static_cast<uint8_t*>(::mmap(nullptr, mapped_bytes_, PROT_READ, MAP_SHARED, fd_, 0));
        if (base_ == MAP_FAILED) {
            base_ = nullptr;
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("[blockreader] mmap failed");
        }

        std::memcpy(&hdr_, base_, sizeof(hdr_));
    }

    void unmap_() {
        if (base_) {
            ::munmap(base_, mapped_bytes_);
            base_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        mapped_bytes_ = 0;
        hdr_ = {};
    }

    int fd_{-1};
    uint8_t* base_{nullptr};
    size_t mapped_bytes_{0};
    DayFileHeader hdr_{};
    std::vector<Row> rows_;
    BlockReaderOpt opt_;
    std::vector<DayFile> files_;
    std::vector<uint32_t> days_;
    std::vector<fs::path> paths_only_;
    size_t file_idx_{0};
};
