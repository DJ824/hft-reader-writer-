#pragma once
#include <cstdint>
#include <vector>
#include <cstring>
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>

template <class Schema>
struct L2TBlockCodec {
#pragma pack(push, 1)
    struct BlockHeader {
        char magic[8];
        uint16_t version;
        uint16_t flags;
        uint32_t n_rows;
        uint64_t base_ts;
        uint32_t base_px;
        uint32_t ts_scale_ns = 1'000'000;
        uint8_t ts_bw;
        uint8_t px_bw;
        uint16_t reserved0;
        uint32_t off_ts;
        uint32_t len_ts;
        uint32_t off_px;
        uint32_t len_px;
        uint32_t off_sz;
        uint32_t len_sz;
        uint32_t off_side;
        uint32_t len_side;
        uint32_t off_type;
        uint32_t len_type;
    };
#pragma pack(pop)

    static inline uint64_t ceil_log2_u64(uint64_t x) {
        if (x <= 1) {
            return 1;
        }
        return 64u - static_cast<uint32_t>(__builtin_clzll(x - 1));
    }

    static inline uint32_t zigzag_enc32(int32_t v) {
        return (static_cast<uint32_t>(v) << 1) ^ static_cast<uint32_t>(v >> 31);
    }

    static inline int32_t zigzag_dec32(uint32_t v) {
        return int32_t((v >> 1) ^ -(v & 1));
    }

    static inline void bitpack_u32(const uint32_t* vals, size_t n, uint32_t bw, std::vector<uint8_t>& out) {
        if (bw == 0 || n == 0) {
            return;
        }

        const uint64_t mask = bw == 32 ? 0xffff'ffffull : (1ull << bw) - 1ull;
        uint64_t acc = 0;
        uint32_t bits = 0;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t v = vals[i] & mask;
            acc |= (v << bits);
            bits += bw;
            while (bits >= 8) {
                out.push_back(static_cast<uint8_t>(acc & 0xffu));
                acc >>= 8;
                bits -= 8;
            }
        }

        if (bits > 0) {
            out.push_back(static_cast<uint8_t>(acc & 0xffu));
        }
    }


    static inline void bitpack_u64(const uint64_t* vals, size_t n, uint32_t bw, std::vector<uint8_t>& out) {
        if (bw == 0 || n == 0) {
            return;
        }

        const uint64_t mask = (bw == 64) ? ~0ull : (1ull << bw) - 1ull;
        uint64_t acc = 0;
        uint64_t bits = 0;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t v = vals[i] & mask;
            acc |= (v << bits);
            bits += bw;
            while (bits >= 8) {
                out.push_back(static_cast<uint8_t>(acc & 0xffu));
                acc >>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) {
            out.push_back(static_cast<uint8_t>(acc & 0xffu));
        }
    }

    static void bitunpack_u64(const uint8_t* src, size_t n, uint32_t bw, uint64_t* out) {
        if (bw == 0 || n == 0) {
            for (size_t i = 0; i < n; ++i) {
                out[i] = 0;
            }
            return;
        }

        const uint64_t mask = (bw == 64) ? ~0ull : (1ull << bw) - 1ull;
        size_t idx = 0;
        uint64_t acc = 0;
        uint32_t bits = 0;
        for (size_t i = 0; i < n; ++i) {
            while (bits < bw) {
                acc |= (static_cast<uint64_t>(src[idx++]) << bits);
                bits += 8;
            }
            out[i] = acc & mask;
            acc >>= bw;
            bits -= bw;
        }
    }

    static void bitunpack_u32(const uint8_t* src, size_t n, uint32_t bw, uint32_t* out) {
        if (bw == 0 || n == 0) {
            for (size_t i = 0; i < n; ++i) {
                out[i] = 0;
            }
        }

        const uint64_t mask = (bw == 32) ? 0xffff'ffffull : (1ull << bw) - 1ull;
        size_t idx = 0;
        uint64_t acc = 0;
        uint32_t bits = 0;
        for (size_t i = 0; i < n; ++i) {
            while (bits < bw) {
                acc |= (static_cast<uint64_t>(src[idx++]) << bits);
                bits += 8;
            }
            out[i] = static_cast<uint32_t>(acc & mask);
            acc >>= bw;
            bits -= bw;
        }
    }

    static void bitpack_u8(const uint8_t* src, size_t n, std::vector<uint8_t>& out) {
        size_t i = 0;
        for (; i + 8 <= n; i += 8) {
            uint8_t b =
                (src[i + 0] & 1) << 0 |
                (src[i + 1] & 1) << 1 |
                (src[i + 2] & 1) << 2 |
                (src[i + 3] & 1) << 3 |
                (src[i + 4] & 1) << 4 |
                (src[i + 5] & 1) << 5 |
                (src[i + 6] & 1) << 6 |
                (src[i + 7] & 1) << 7;
            out.push_back(b);
        }
        if (i < n) {
            uint8_t b = 0;
            uint32_t bit = 0;
            for (; i < n; ++i, ++bit) {
                b |= (src[i] & 1) << bit;
            }
            out.push_back(b);
        }
    }

    static void bitunpack_u8(const uint8_t* src, size_t n, uint8_t* out) {
        size_t i = 0;
        for (; i + 8 <= n; i += 8) {
            uint8_t b = *src++;
            out[i + 0] = b >> 0 & 1;
            out[i + 1] = b >> 1 & 1;
            out[i + 2] = b >> 2 & 1;
            out[i + 3] = b >> 3 & 1;
            out[i + 4] = b >> 4 & 1;
            out[i + 5] = b >> 5 & 1;
            out[i + 6] = b >> 6 & 1;
            out[i + 7] = b >> 7 & 1;
        }

        if (i < n) {
            const uint8_t b = *src;
            uint32_t bit = 0;
            for (; i < n; ++i, ++bit) {
                out[i] = b >> bit & 1;
            }
        }
    }

    using Row = typename Schema::Row;

    static void encode_block(const Row* rows, uint32_t n, std::vector<uint8_t>& out) {
        if (n == 0) {
            return;
        }

        BlockHeader hdr{};
        hdr.version = 1;
        hdr.flags = 0;
        hdr.n_rows = n;
        hdr.base_ts = rows[0].ts_ns;
        hdr.base_px = rows[0].price;

        std::vector<uint64_t> ts_delta(n);
        std::vector<uint32_t> px_delta_zigzag(n);
        std::vector<uint8_t> side(n);
        std::vector<uint8_t> type(n);

        uint64_t max_dt = 0;
        uint64_t max_dxz = 0;


        for (uint32_t i = 0; i < n; ++i) {
            const uint64_t dt = rows[i].ts_ns - hdr.base_ts;
            // apply scale
            const uint32_t u = dt / hdr.ts_scale_ns;
            ts_delta[i] = u;
            if (u > max_dt) {
                max_dt = u;
            }

            const int32_t dx = static_cast<int32_t>(static_cast<int64_t>(rows[i].price - static_cast<int64_t>(hdr.
                base_px)));
            const uint32_t dz = zigzag_enc32(dx);
            px_delta_zigzag[i] = dz;
            if (dz > max_dxz) {
                max_dxz = dz;
            }

            side[i] = rows[i].side;
            // encode chars as L = 0, T = 1
            type[i] = rows[i].type == 'T' ? 1 : 0;
        }

        hdr.ts_bw = static_cast<uint8_t>(ceil_log2_u64(max_dt + 1));
        hdr.px_bw = static_cast<uint8_t>(ceil_log2_u64(max_dxz + 1));
        hdr.reserved0 = 0;

        const uint32_t start = static_cast<uint32_t>(out.size());
        const uint32_t hdr_size = (sizeof(BlockHeader));
        out.resize(start + hdr_size);

        hdr.off_ts = hdr_size;
        {
            // original size of vector
            const size_t before = out.size();
            // store packed data
            bitpack_u64(ts_delta.data(), n, hdr.ts_bw, out);
            // len of packed data is new size - before
            hdr.len_ts = (out.size() - before);
        }

        hdr.off_px = hdr.off_ts + hdr.len_ts;
        {
            const size_t before = out.size();
            bitpack_u32(px_delta_zigzag.data(), n, hdr.px_bw, out);
            hdr.len_px = (out.size() - before);
        }

        hdr.off_sz = hdr.off_px + hdr.len_px;
        hdr.len_sz = n * static_cast<uint32_t>(sizeof(float));
        {
            const size_t before = out.size();
            out.resize(before + hdr.len_sz);
            uint8_t* p = out.data() + before;
            for (uint32_t i = 0; i < n; ++i) {
                std::memcpy(p + i * sizeof(float), &rows[i].size, sizeof(float));
            }
        }

        hdr.off_side = hdr.off_sz + hdr.len_sz;
        {
            size_t before = out.size();
            bitpack_u8(side.data(), n, out);
            hdr.len_side = out.size() - before;
        }


        hdr.off_type = hdr.off_side + hdr.len_side;
        {
            size_t before = out.size();
            bitpack_u8(type.data(), n, out);
            hdr.len_type = out.size() - before;
        }

        std::memcpy(out.data() + start, &hdr, sizeof(BlockHeader));
    }

    static size_t decode_block(const uint8_t* src, size_t src_len, std::vector<Row>& rows_out) {
        if (src_len < sizeof(BlockHeader)) {
            throw std::runtime_error("block too small");
        }

        BlockHeader hdr{};
        std::memcpy(&hdr, src, sizeof(BlockHeader));
        if (!check_magic(hdr)) {
            throw std::runtime_error("block magic incorrect");
        }

        if (hdr.n_rows == 0) {
            return sizeof(BlockHeader);
        }

        rows_out.resize(hdr.n_rows);

        std::vector<uint64_t> ts_delta(hdr.n_rows);
        if (hdr.ts_bw == 0) {
            fill(ts_delta.begin(), ts_delta.end(), 0);
        }
        else {
            bitunpack_u64(src + hdr.off_ts, hdr.n_rows, hdr.ts_bw, ts_delta.data());
        }

        std::vector<uint32_t> px_dz(hdr.n_rows);
        if (hdr.px_bw == 0) {
            fill(px_dz.begin(), px_dz.end(), 0);
        }
        else {
            bitunpack_u32(src + hdr.len_px, hdr.n_rows, hdr.px_bw, px_dz.data());
        }

        std::vector<uint8_t> side(hdr.n_rows);
        bitunpack_u8(src + hdr.off_side, hdr.n_rows, side.data());

        std::vector<uint8_t> type(hdr.n_rows);
        bitunpack_u8(src + hdr.off_type, hdr.n_rows, type.data());

        uint8_t* p_sz = src + hdr.off_sz;

        for (uint32_t i = 0; i < hdr.n_rows; ++i) {
            Row& r = rows_out[i];
            uint64_t u = ts_delta[i];
            r.ts_ts = hdr.base_ts + u * hdr.ts_scale_ns;

            int32_t dx = zigzag_dec32(px_dz[i]);
            uint64_t px = hdr.base_px + dx;

            if (px < 0 || px > std::numeric_limits<uint32_t>::max()) {
                throw std::runtime_error("price overflow");
            }

            float fsz;
            std::memcpy(&fsz, p_sz + i * sizeof(float), sizeof(float));
            r.size = fsz;
            r.side = side[i];
            if (type[i] == 1) {
                r.type = 'T';
            } else {
                r.type = 'L';
            }
        }

        uint32_t end_off = std::max({
            hdr.off_ts + hdr.len_ts,
            hdr.off_px + hdr.len_px,
            hdr.off_sz + hdr.len_sz,
            hdr.off_side + hdr.len_side,
            hdr.off_type + hdr.len_type
        });
        return std::max<uint32_t>(end_off, sizeof(BlockHeader));
    }
};
