#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <charconv>
#include <type_traits>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "schemas.h"

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000 // 2mb
#endif

#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

struct HugeBuff {
    void* ptr{nullptr};
    size_t len{0};
    bool huge_tlb{false};

    static HugeBuff alloc(size_t bytes) {
        HugeBuff buff;
        const size_t two_mb = 2ull * 1024 * 1024;
        // bytes rounded up to the next multiple of huge page size (2mb)
        size_t want = (bytes + (two_mb - 1)) & ~(two_mb - 1);

        int flags = MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE|MAP_HUGETLB| MAP_HUGE_2MB;
        if (void* p = mmap(nullptr, want, PROT_READ|PROT_WRITE, flags, -1, 0); p != MAP_FAILED) {
            buff.ptr = p;
            buff.len = want;
            buff.huge_tlb = true;
            return buff;
        }

        if (void* p = ::mmap(nullptr, bytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
            p != MAP_FAILED) {
            ::madvise(p, bytes, MADV_HUGEPAGE);
            buff.ptr=p;
            buff.len=bytes;
            }
        return buff;
    }

    void free() {
        if (ptr) {
            munmap(ptr, len);
            ptr = nullptr;
            len = 0;
            huge_tlb = false;
        }
    }
};



namespace fs = std::filesystem;

struct ReaderOpt {
    std::string base_dir;
    std::string product;
    uint32_t date_from = 0;
    uint32_t date_to = 99999999;
};

template <class Schema>
class ReaderT {
public:
    using Header = ColFileHeaderT<Schema>;

    struct Segment {
        const void* col_ptrs[Schema::COLS];
        uint64_t rows{0};

        template <class T>
        const T* col(uint32_t i) const noexcept {
            return reinterpret_cast<const T*>(col_ptrs[i]);
        }
    };

    struct Stage {
        HugeBuff slab{};
        std::byte* cols[Schema::COLS]{};
        size_t capacity_rows{0};

        void ensure(size_t rows) {
            size_t need = 0;
            for (uint32_t i = 0; i < Schema::COLS; ++i) {
                need += rows * Schema::col_size(i);
            }
            if (!slab.ptr || slab.len < need) {
                if (slab.ptr) {
                    slab.free();
                }

                slab = HugeBuff::alloc(need);
                auto* p = static_cast<std::byte*>(slab.ptr);
                // initialize ptr for each column
                for (uint32_t i = 0; i < Schema::COLS; ++i) {
                    cols[i] = p;
                    p += rows * Schema::col_size(i);
                }
                capacity_rows = rows;
            }
        }
    };

    Stage stage_;

    template <class Fn>
    size_t visit_single_segment(const std::filesystem::path& file, Fn&& fn) {
        if (mapped_) {
            unmap();
        }

        if (!map_file(file)) {
            return 0;
        }

        Segment seg{};
        for (uint32_t c = 0; c < Schema::COLS; ++c) {
            seg.col_ptrs[c] = col_ptrs_[c];
        }

        seg.rows = rows_;
        std::forward<Fn>(fn)(seg);
        unmap();
        return seg.rows;
    }


    bool first_stage_file(Segment& out) {
        if (files_.empty()) {
            return false;
        }
        file_idx_ = 0;
        if (!map_file(files_[file_idx_].path)) {
            return false;
        }
        return stage_curr_file(out);
    }

    bool next_stage_file(Segment& out) {
        if (!advance()) {
            return false;
        }

        return stage_curr_file(out);
    }

    template <class Fn>
    void visit_stage_files(Fn&& fn) {
        Segment s{};
        if (!first_stage_file(s)) {
            return;
        }
        do {
            if (!fn(s)) {
                break;
            }
        }
        while (next_stage_file(s));
    }


    explicit ReaderT(const ReaderOpt& opt) : opt_(opt) { build_day_file_list(); }
    ~ReaderT() { unmap(); }

    inline const std::vector<uint32_t>& days() const noexcept { return days_; }
    inline const std::vector<fs::path>& paths() const noexcept { return paths_only_; }

private:
    struct DayFile {
        uint32_t yyyymmdd;
        fs::path path;
    };

    bool stage_curr_file(Segment& out) {
        if (!mapped_ || rows_ == 0) {
            return false;
        }

        stage_.ensure(static_cast<size_t>(rows_));
        for (uint32_t c = 0; c < Schema::COLS; ++c) {
            const size_t sz = static_cast<size_t>(Schema::col_size(c));
            const size_t bytes = rows_ * sz;
            // copy column to stage
            std::memcpy(stage_.cols[c], col_ptrs_[c], bytes);
            out.col_ptrs[c] = stage_.cols[c];
        }
        out.rows = rows_;
        return true;
    }

    void fill_segment(Segment& out) {
        for (uint32_t c = 0; c < Schema::COLS; ++c) {
            out.col_ptrs[c] = col_ptrs_[c];
            out.rows = rows_;
        }
    }

    fs::path product_dir() const {
        if (opt_.product.empty()) {
            return fs::path(opt_.base_dir);
        }
        return fs::path(opt_.base_dir) / opt_.product;
    }

    static bool parse_yyyymmdd(const std::string& fname, uint32_t& out) {
        if (fname.size() != 12) {
            return false;
        }
        if (fname.substr(8) != ".bin") {
            return false;
        }
        for (int i = 0; i < 8; ++i)
            if (fname[i] < '0' || fname[i] > '9') {
                return false;
            }
        uint32_t v = 0;
        auto res = std::from_chars(fname.data(), fname.data() + 8, v);
        if (res.ec != std::errc()) {
            return false;
        }
        out = v;
        return true;
    }

    void build_day_file_list() {
        files_.clear();
        days_.clear();
        paths_only_.clear();
        const fs::path root = product_dir();
        if (!fs::exists(root)) {
            return;
        }

        for (const auto& e : fs::directory_iterator(root)) {
            if (!e.is_regular_file()) {
                continue;
            }
            const std::string name = e.path().filename().string();
            uint32_t d = 0;
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
    }

    bool map_file(const fs::path& p) {
        unmap();

        fd_ = ::open(p.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0) {
            return false;
        }

        struct stat st{};
        if (::fstat(fd_, &st) != 0 || st.st_size <= 0) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        map_bytes_ = static_cast<size_t>(st.st_size);

        map_ = ::mmap(nullptr, map_bytes_, PROT_READ, MAP_SHARED, fd_, 0);
        if (map_ == MAP_FAILED) {
            map_ = nullptr;
            ::close(fd_);
            fd_ = -1;
            return false;
        }

        posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
        madvise(map_, map_bytes_, MADV_SEQUENTIAL);
        madvise(map_, map_bytes_, MADV_WILLNEED);

        const auto* base = static_cast<const std::byte*>(map_);
        std::memcpy(&hdr_, base, sizeof(Header));

        if (std::memcmp(hdr_.magic, Schema::MAGIC, sizeof(hdr_.magic)) != 0) {
            unmap();
            return false;
        }

        rows_ = hdr_.rows;

        for (uint32_t c = 0; c < Schema::COLS; ++c) {
            // location of first entry of column
            col_ptrs_[c] = base + hdr_.col_off[c];
            // size of element each col hols
            col_sz_[c] = hdr_.col_sz[c];
        }
        idx_ = 0;
        mapped_ = true;
        return true;
    }

    bool advance() {
        if (!mapped_) {
            return false;
        }
        unmap();
        if (++file_idx_ >= files_.size()) {
            return false;
        }
        return map_file(files_[file_idx_].path);
    }

    void unmap() {
        if (map_) {
            ::munmap(map_, map_bytes_);
            map_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        map_bytes_ = 0;
        rows_ = 0;
        idx_ = 0;
        mapped_ = false;
    }

private:
    ReaderOpt opt_;
    std::vector<DayFile> files_;
    std::vector<uint32_t> days_;
    std::vector<fs::path> paths_only_;
    size_t file_idx_{0};
    int fd_{-1};
    void* map_{nullptr};
    size_t map_bytes_{0};
    Header hdr_{};
    const std::byte* col_ptrs_[Schema::COLS]{};
    uint64_t col_sz_[Schema::COLS]{};
    uint64_t rows_{0};
    uint64_t idx_{0};
    bool mapped_{false};
};

using L2Reader = ReaderT<L2Schema>;
using L3Reader = ReaderT<L3Schema>;
using ImbalanceReader = ReaderT<ImbalanceSchema>;
using VwapReader = ReaderT<VwapSchema>;
using VoiReader = ReaderT<VoiSchema>;