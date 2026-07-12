/**
 * sqltypes_td.h - Comprehensive Teradata SQL Types Header
 * 
 * This is a mock header for local compilation verification.
 * The actual header is provided by Teradata TTU installation.
 * 
 * Reference: Teradata SQL External Routine Programming
 * 
 * Includes all major Teradata data types:
 * - Numeric: INTEGER, BIGINT, SMALLINT, BYTEINT, DECIMAL, NUMBER, FLOAT
 * - Character: CHAR, VARCHAR, CLOB
 * - Binary: BYTE, VARBYTE, BLOB
 * - DateTime: DATE, TIME, TIMESTAMP, PERIOD, INTERVAL
 * - Complex: JSON, XML, ARRAY, ST_GEOMETRY
 */

#ifndef SQLTYPES_TD_H
#define SQLTYPES_TD_H

#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* ============================================================
 * Basic SQL types - Teradata to C type mappings
 * ============================================================ */
typedef int32_t INTEGER;
typedef int64_t BIGINT;
typedef int16_t SMALLINT;
typedef int8_t BYTEINT;
typedef double FLOAT;
typedef double REAL;
typedef unsigned char BYTE;
typedef char SQL_TEXT;

/* For 128-bit decimal support */
#ifdef __SIZEOF_INT128__
typedef __int128 INT128;
#else
typedef struct { int64_t high; uint64_t low; } INT128;
#endif

/* ============================================================
 * Data type enumeration - Complete Teradata type list
 * ============================================================ */
typedef enum {
    /* Real FNC datatype codes (must match FNC_TblOpGetColDef on the DB).
     * See docs/TECHNICAL_GUIDE_TDEXPORT.md. */
    CHAR_DT = 1,
    VARCHAR_DT = 2,
    LONG_VARCHAR_DT = 3,
    VARGRAPHIC_DT = 4,
    BYTEINT_DT = 7,
    SMALLINT_DT = 8,
    INTEGER_DT = 9,
    FLOAT_DT = 10,
    REAL_DT = 10,
    DOUBLE_PRECISION_DT = 10,
    BIGINT_DT = 36,

    /* Decimal / Number family (bytesize distinguishes short vs long) */
    DECIMAL4_DT = 13,
    DECIMAL_DT = 14,
    DECIMAL8_DT = 14,
    NUMBER_DT = 14,
    DECIMAL1_DT = 14,
    DECIMAL2_DT = 14,
    DECIMAL16_DT = 14,

    GRAPHIC_DT = 23,

    /* Binary Types */
    BYTE_DT = 30,
    VARBYTE_DT = 31,

    /* Large Object Types */
    BLOB_REFERENCE_DT = 40,
    CLOB_REFERENCE_DT = 41,

    /* Date/Time Types */
    DATE_DT = 15,
    TIME_DT = 16,
    TIMESTAMP_DT = 17,
    TIME_WTZ_DT = 52,
    TIMESTAMP_WTZ_DT = 54,
    
    /* Period Types */
    PERIOD_DATE_DT = 60,
    PERIOD_TIME_DT = 61,
    PERIOD_TIME_WTZ_DT = 62,
    PERIOD_TIMESTAMP_DT = 63,
    PERIOD_TIMESTAMP_WTZ_DT = 64,
    
    /* Interval Types */
    INTERVAL_YEAR_DT = 70,
    INTERVAL_YTM_DT = 71,    /* Year to Month */
    INTERVAL_MONTH_DT = 72,
    INTERVAL_DAY_DT = 73,
    INTERVAL_DTH_DT = 74,    /* Day to Hour */
    INTERVAL_DTM_DT = 75,    /* Day to Minute */
    INTERVAL_DTS_DT = 76,    /* Day to Second */
    INTERVAL_HOUR_DT = 77,
    INTERVAL_HTM_DT = 78,    /* Hour to Minute */
    INTERVAL_HTS_DT = 79,    /* Hour to Second */
    INTERVAL_MINUTE_DT = 80,
    INTERVAL_MTS_DT = 81,    /* Minute to Second */
    INTERVAL_SECOND_DT = 82,
    
    /* Complex Types */
    JSON_DT = 90,
    XML_DT = 91,
    ST_GEOMETRY_DT = 92,
    ARRAY_DT = 93,
    UDT_DT = 94              /* User-Defined Type */
} DataType_t;

/* ============================================================
 * Charset type enumeration
 * ============================================================ */
typedef enum {
    LATIN_CT = 1,
    UNICODE_CT = 2,
    KANJISJIS_CT = 3,
    GRAPHIC_CT = 4,
    UTF8_CT = 5,
    UTF16_CT = 6
} CharsetType_t;

/* ============================================================
 * Stream format enumeration
 * ============================================================ */
typedef enum {
    INDICFMT1 = 1,      /* Indicator format 1 (recommended) */
    INDICFMT2 = 2       /* Indicator format 2 */
} Stream_Fmt_en;

/* ============================================================
 * Table Operator result codes
 * ============================================================ */
#define TBLOP_SUCCESS 0
#define TBLOP_EOF 1
#define TBLOP_ERROR -1

/* Input/Output indicators */
#define ISINPUT 1
#define ISOUTPUT 0

/* JSON storage format */
#define JSON_TEXT_EN 1
#define JSON_BSON_EN 2
#define JSON_UBJSON_EN 3

/* Max name length */
#define FNC_MAXNAMELEN_EON 128

/* ============================================================
 * Column type definition structures
 * ============================================================ */

/* Union for column size (different interpretations based on type) */
typedef union {
    int length;          /* For CHAR, VARCHAR, BYTE, VARBYTE */
    int precision;       /* For FLOAT, REAL */
    struct {
        short totaldigit; /* Total precision */
        short fracdigit;  /* Scale (fractional digits) */
    } range;             /* For DECIMAL, NUMBER */
    int intervalrange;   /* For INTERVAL types */
} ColumnSize_t;

/* Table Operator Column Definition */
typedef struct {
    int num_columns;
    int length;
    struct {
        DataType_t datatype;
        ColumnSize_t size;
        CharsetType_t charset;
        int bytesize;
        int period_et;           /* Period element type */
        int udt_indicator;       /* UDT indicator */
        char udt_type[FNC_MAXNAMELEN_EON];
        int JSONStorageFormat;
        int struct_num_attributes;
        char column_name[FNC_MAXNAMELEN_EON]; /* Optional column name */
    } column_types[256]; /* Max 256 columns per stream */
} FNC_TblOpColumnDef_t;

/* UDT Base Info */
typedef struct {
    int base_type;
    int base_size;
} UDT_BaseInfo_t;

/* ============================================================
 * Row data structure
 * ============================================================ */
typedef struct {
    void *columnptr[256];           /* Pointers to column values */
    int lengths[256];               /* Actual lengths of each column */
    unsigned char indicators[32];   /* Bit array for null indicators (256/8) */
} RowData_t;

/* Table Operator Handle */
typedef struct {
    int stream_id;
    char mode;           /* 'r' for read, 'w' for write */
    RowData_t *row;
} FNC_TblOpHandle_t;

/* ============================================================
 * Phase enumeration (legacy Table Function)
 * ============================================================ */
typedef enum {
    TBL_PRE_EXE = 0,
    TBL_MODE_CONST = 1
} FNC_Phase;

/* Legacy return codes */
#define TBL_row 0
#define TBL_NO_MORE_ROWS 1

/* ============================================================
 * LOB types
 * ============================================================ */
typedef int LOB_CONTEXT_ID;
typedef int LOB_RESULT_LOCATOR;
typedef int FNC_LobLength_t;

/* ============================================================
 * Utility Macros
 * ============================================================ */

/* Calculate size of column definition structure */
#define TblOpSIZECOLDEF(n) (sizeof(FNC_TblOpColumnDef_t))

/* Initialize column definition */
#define TblOpINITCOLDEF(cols, n) do { (cols)->num_columns = (n); } while(0)

/* Null indicator macros */
#define TBLOPISNULL(indicators, idx) ((indicators)[(idx)/8] & (1 << ((idx)%8)))
#define TBLOPSETNULL(indicators, idx) ((indicators)[(idx)/8] |= (1 << ((idx)%8)))
#define TBLOPSETNULLCLEAR(indicators, idx) ((indicators)[(idx)/8] &= ~(1 << ((idx)%8)))

/* ============================================================
 * Table Operator Functions (FNC_TblOp*)
 * 
 * These are mock implementations for local syntax checking.
 * Actual implementations are provided by Teradata runtime.
 * ============================================================ */

/* Get number of input and output streams */
static inline int FNC_TblOpGetStreamCount(int *incount, int *outcount) {
    *incount = 1;   /* Mock: 1 input stream */
    *outcount = 1;  /* Mock: 1 output stream */
    return 0;
}

/* Get column count for a stream */
static inline int FNC_TblOpGetColCount(int stream_id, int is_input) {
    (void)stream_id;
    (void)is_input;
    return 4; /* Mock: 4 columns */
}

/* Get column definitions for a stream */
static inline int FNC_TblOpGetColDef(int stream_id, int is_input, FNC_TblOpColumnDef_t *cols) {
    (void)stream_id;
    (void)is_input;
    /* Mock: Set up some example column definitions */
    if (cols && cols->num_columns >= 3) {
        cols->column_types[0].datatype = INTEGER_DT;
        cols->column_types[0].bytesize = 4;
        cols->column_types[1].datatype = VARCHAR_DT;
        cols->column_types[1].size.length = 50;
        cols->column_types[1].bytesize = 52;
        cols->column_types[2].datatype = BIGINT_DT;
        cols->column_types[2].bytesize = 8;
    }
    return 0;
}

/* Set output column definitions (contract phase) */
static inline int FNC_TblOpSetOutputColDef(int stream_id, FNC_TblOpColumnDef_t *cols) {
    (void)stream_id;
    (void)cols;
    return 0;
}

/* Set contract definition string */
static inline int FNC_TblOpSetContractDef(const char *contract, int length) {
    (void)contract;
    (void)length;
    return 0;
}

/* Set stream format */
static inline int FNC_TblOpSetFormat(const char *name, int stream_id, int is_input, 
                                      void *value, int size) {
    (void)name;
    (void)stream_id;
    (void)is_input;
    (void)value;
    (void)size;
    return 0;
}

/* Get UDT base info */
static inline int FNC_TblOpGetBaseInfo(FNC_TblOpColumnDef_t *cols, UDT_BaseInfo_t *baseInfos) {
    (void)cols;
    (void)baseInfos;
    return 0;
}

/* Check if stream is dimension stream */
static inline int FNC_TblOpIsDimension(int stream_id, int is_input) {
    (void)stream_id;
    (void)is_input;
    return 0;
}

/* Open a stream handle */
static inline FNC_TblOpHandle_t* FNC_TblOpOpen(int stream_id, char mode, int flags) {
    static FNC_TblOpHandle_t handle;
    static RowData_t row;
    (void)flags;
    handle.stream_id = stream_id;
    handle.mode = mode;
    handle.row = &row;
    memset(&row, 0, sizeof(row));
    return &handle;
}

/* Read next row from input stream */
static inline int FNC_TblOpRead(FNC_TblOpHandle_t *handle) {
    static int call_count = 0;
    (void)handle;
    if (++call_count > 10) return TBLOP_EOF;
    return TBLOP_SUCCESS;
}

/* Write row to output stream */
static inline int FNC_TblOpWrite(FNC_TblOpHandle_t *handle) {
    (void)handle;
    return 0;
}

/* Close stream handle */
static inline int FNC_TblOpClose(FNC_TblOpHandle_t *handle) {
    (void)handle;
    return 0;
}

/* ============================================================
 * Legacy Table Function APIs (for reference)
 * ============================================================ */

/* Get execution phase */
static inline int FNC_GetPhase(FNC_Phase *phase) {
    *phase = TBL_MODE_CONST;
    return TBL_MODE_CONST;
}

/* Get AMP ID */
static inline int FNC_GetAmpId(void) {
    return 0;
}

/* Get AMP vproc ID (Table Operator API) - actual AMP number for worker distribution */
static inline int FNC_TblOpGetAmpId(INTEGER *amp_id) {
    static int mock_amp_counter = 0;
    *amp_id = mock_amp_counter++; /* Mock: return sequential AMP IDs for testing */
    return 0;
}

/* Get unique AMP ID (Table Operator API) */
static inline int FNC_TblOpGetUniqID(INTEGER *amp_id) {
    *amp_id = 0; /* Mock: return AMP 0 */
    return 0;
}

/* Get next row (legacy) */
static inline int FNC_GetNextRow(void) {
    return TBL_NO_MORE_ROWS;
}

/* ============================================================
 * Memory Functions
 * ============================================================ */

static inline void* FNC_malloc(size_t size) {
    return malloc(size);
}

static inline void FNC_free(void* ptr) {
    free(ptr);
}

/* ============================================================
 * LOB Functions (stubs)
 * ============================================================ */

static inline LOB_RESULT_LOCATOR FNC_LobCol2Loc(int stream, int col) {
    (void)stream;
    (void)col;
    return 0;
}

static inline int FNC_LobOpen_CL(void *ref, LOB_CONTEXT_ID *id, int offset, int flags) {
    (void)ref;
    (void)id;
    (void)offset;
    (void)flags;
    return 0;
}

static inline int FNC_LobRead(LOB_CONTEXT_ID id, void *buffer, int size, FNC_LobLength_t *actlen) {
    (void)id;
    (void)buffer;
    (void)size;
    *actlen = 0;
    return -1; /* EOF */
}

static inline int FNC_LobAppend(LOB_RESULT_LOCATOR loc, void *data, int len, FNC_LobLength_t *actlen) {
    (void)loc;
    (void)data;
    *actlen = len;
    return 0;
}

static inline int FNC_LobClose(LOB_CONTEXT_ID id) {
    (void)id;
    return 0;
}

/* ============================================================
 * USING Clause Parameter Access
 * ============================================================ */

/**
 * FNC_TblOpGetUsingParam - Get a parameter value from USING clause
 * 
 * @param name   Parameter name (e.g., "TargetIP", "QueryID")
 * @param value  Buffer to receive the parameter value
 * @param length Pointer to receive the actual length
 * @return 0 on success, -1 if parameter not found
 */
static inline int FNC_TblOpGetUsingParam(const char *name, void *value, int *length) {
    (void)name;
    (void)value;
    (void)length;
    /* In mock header, this returns -1 (not found).
     * Actual Teradata implementation returns the USING clause value.
     * For now, we rely on environment variables as fallback.
     */
    return -1;
}

#endif /* SQLTYPES_TD_H */
