#pragma once
#include <atomic>
#include <cstdint>

struct L2Row {
    uint64_t ts_ns;
    uint32_t price;
    float qty;
    uint8_t side;
};

struct L2Schema {
    enum : uint32_t { COL_TS = 0, COL_PX = 1, COL_QTY = 2, COL_SIDE = 3, COL_COUNT = 4 };

    static constexpr uint32_t COLS = 4;
    static constexpr const char* MAGIC = "L2COL\n";
    static constexpr uint16_t VERSION = 1;
    using Row = L2Row;

    static constexpr size_t col_size(uint32_t i) {
        return (i == COL_TS)
                   ? sizeof(uint64_t)
                   : (i == COL_PX)
                   ? sizeof(uint32_t)
                   : (i == COL_QTY)
                   ? sizeof(float)
                   : sizeof(uint8_t);
    }

    static inline uint64_t hour_from_row(const Row& r) {
        const uint64_t s = r.ts_ns / 1'000'000'000ull;
        return (s / 3600ull) * 3600ull;
    }

    static inline void write_row_to_cols(const Row& r, void** c, uint64_t i) {
        reinterpret_cast<uint64_t*>(c[COL_TS])[i] = r.ts_ns;
        reinterpret_cast<uint32_t*>(c[COL_PX])[i] = r.price;
        reinterpret_cast<float*>(c[COL_QTY])[i] = r.qty;
        reinterpret_cast<uint8_t*>(c[COL_SIDE])[i] = r.side;
    }

    static inline void read_row_from_cols(Row& r, const void* const* c, uint64_t i) {
        r.ts_ns = reinterpret_cast<const uint64_t*>(c[COL_TS])[i];
        r.price = reinterpret_cast<const uint32_t*>(c[COL_PX])[i];
        r.qty = reinterpret_cast<const float*>(c[COL_QTY])[i];
        r.side = reinterpret_cast<const uint8_t*>(c[COL_SIDE])[i];
    }
};

struct L3Row {
    uint64_t id;
    uint64_t ts_ns;
    uint32_t price;
    uint32_t size;
    uint8_t action;
    uint8_t side;
};

struct L3Schema {
    enum : uint32_t { COL_ID = 0, COL_TS = 1, COL_PX = 2, COL_SZ = 3, COL_ACT = 4, COL_SIDE = 5 };

    static constexpr uint32_t COLS = 6;
    static constexpr const char* MAGIC = "L3COL\n";
    static constexpr uint16_t VERSION = 1;
    using Row = L3Row;

    static constexpr size_t col_size(uint32_t i) {
        return (i <= COL_TS) ? sizeof(uint64_t) : (i <= COL_SZ) ? sizeof(uint32_t) : sizeof(uint8_t);
    }

    static inline uint64_t hour_from_row(const Row& r) {
        const uint64_t s = r.ts_ns / 1'000'000'000ull;
        return (s / 3600ull) * 3600ull;
    }

    static inline void write_row_to_cols(const Row& r, void** c, uint64_t i) {
        reinterpret_cast<uint64_t*>(c[COL_ID])[i] = r.id;
        reinterpret_cast<uint64_t*>(c[COL_TS])[i] = r.ts_ns;
        reinterpret_cast<uint32_t*>(c[COL_PX])[i] = r.price;
        reinterpret_cast<uint32_t*>(c[COL_SZ])[i] = r.size;
        reinterpret_cast<uint8_t*>(c[COL_ACT])[i] = r.action;
        reinterpret_cast<uint8_t*>(c[COL_SIDE])[i] = r.side;
    }

    static inline void read_row_from_cols(Row& r, const void* const* c, uint64_t i) {
        r.id = reinterpret_cast<const uint64_t*>(c[COL_ID])[i];
        r.ts_ns = reinterpret_cast<const uint64_t*>(c[COL_TS])[i];
        r.price = reinterpret_cast<const uint32_t*>(c[COL_PX])[i];
        r.size = reinterpret_cast<const uint32_t*>(c[COL_SZ])[i];
        r.action = reinterpret_cast<const uint8_t*>(c[COL_ACT])[i];
        r.side = reinterpret_cast<const uint8_t*>(c[COL_SIDE])[i];
    }
};

struct ImbalanceRow {
    float imbalance;
    uint64_t ts_ns;
};

struct ImbalanceSchema {
    static constexpr uint32_t COLS = 2;
    static constexpr const char* MAGIC = "IMBAL\n"; // 6 bytes
    static constexpr uint16_t VERSION = 1;
    using Row = ImbalanceRow;

    static constexpr size_t col_size(uint32_t i) {
        return (i == 0) ? sizeof(float) : sizeof(uint64_t);
    }

    static inline uint64_t hour_from_row(const Row& r) {
        const uint64_t s = r.ts_ns / 1'000'000'000ull;
        return (s / 3600ull) * 3600ull;
    }

    static inline void write_row_to_cols(const Row& r, void** c, uint64_t i) {
        reinterpret_cast<float*>(c[0])[i] = r.imbalance;
        reinterpret_cast<uint64_t*>(c[1])[i] = r.ts_ns;
    }

    static inline void read_row_from_cols(Row& r, const void* const* c, uint64_t i) {
        r.imbalance = reinterpret_cast<const float*>(c[0])[i];
        r.ts_ns = reinterpret_cast<const uint64_t*>(c[1])[i];
    }
};

struct VwapRow {
    float vwap;
    uint64_t ts_ns;
};

struct VwapSchema {
    static constexpr uint32_t COLS = 2;
    static constexpr const char* MAGIC = "VWAP\n"; // 5 + NUL = 6 bytes copied
    static constexpr uint16_t VERSION = 1;
    using Row = VwapRow;

    static constexpr size_t col_size(uint32_t i) {
        return (i == 0) ? sizeof(float) : sizeof(uint64_t);
    }

    static inline uint64_t hour_from_row(const Row& r) {
        const uint64_t s = r.ts_ns / 1'000'000'000ull;
        return (s / 3600ull) * 3600ull;
    }

    static inline void write_row_to_cols(const Row& r, void** c, uint64_t i) {
        reinterpret_cast<float*>(c[0])[i] = r.vwap;
        reinterpret_cast<uint64_t*>(c[1])[i] = r.ts_ns;
    }

    static inline void read_row_from_cols(Row& r, const void* const* c, uint64_t i) {
        r.vwap = reinterpret_cast<const float*>(c[0])[i];
        r.ts_ns = reinterpret_cast<const uint64_t*>(c[1])[i];
    }
};

struct VoiRow {
    uint32_t mid_price;
    uint32_t voi;
    uint64_t ts_ns;
};

struct VoiSchema {
    enum : uint32_t { COL_MID = 0, COL_VOI = 1, COL_TS = 2, COL_COUNT = 3 };

    static constexpr uint32_t COLS = COL_COUNT;
    static constexpr const char* MAGIC = "VOIEVT\n";
    static constexpr uint16_t VERSION = 1;

    using Row = VoiRow;

    static constexpr size_t col_size(uint32_t i) {
        switch (i) {
        case COL_TS: return sizeof(uint64_t);
        case COL_MID: return sizeof(uint32_t);
        case COL_VOI: return sizeof(uint32_t);
        default: return 0;
        }
    }

    static inline uint64_t hour_from_row(const Row& r) {
        const uint64_t s = r.ts_ns / 1'000'000'000ull;
        return (s / 3600ull) * 3600ull;
    }

    static inline void write_row_to_cols(const Row& r, void** c, uint64_t i) {
        reinterpret_cast<uint32_t*>(c[COL_MID])[i] = r.mid_price;
        reinterpret_cast<uint32_t*>(c[COL_VOI])[i] = r.voi;
        reinterpret_cast<uint64_t*>(c[COL_TS])[i] = r.ts_ns;
    }

    static inline void read_row_from_cols(Row& r, const void* const* c, uint64_t i) {
        r.mid_price = reinterpret_cast<const uint32_t*>(c[COL_MID])[i];
        r.voi = reinterpret_cast<const uint32_t*>(c[COL_VOI])[i];
        r.ts_ns = reinterpret_cast<const uint64_t*>(c[COL_TS])[i];
    }
};

template <class Schema>
struct alignas(64) ColFileHeaderT {
    char magic[6];
    uint16_t header_size;
    uint16_t version;
    uint16_t pad16{0};
    uint32_t _pad32{0};
    char product[16];
    uint64_t hour_epoch_start;
    uint64_t rows;
    uint64_t capacity;
    uint64_t col_off[Schema::COLS];
    uint64_t col_sz[Schema::COLS];
    uint8_t pad[256 - 6 - 2 - 2 - 2 - 4 - 16 - 8 - 8 - 8
        - (8 * Schema::COLS) - (8 * Schema::COLS)];
};

static_assert(sizeof(ColFileHeaderT<L2Schema>) == 256, "L2 header must be 256B");
static_assert(sizeof(ColFileHeaderT<L3Schema>) == 256, "L3 header must be 256B");
static_assert(sizeof(ColFileHeaderT<ImbalanceSchema>) == 256, "Imbalance header must be 256B");
static_assert(sizeof(ColFileHeaderT<VwapSchema>) == 256, "VWAP header must be 256B");
static_assert(sizeof(ColFileHeaderT<VoiSchema>) == 256, "VWAP header must be 256B");
