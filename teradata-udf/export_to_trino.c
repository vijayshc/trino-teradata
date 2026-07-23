/**
 * ExportToTrino - Teradata Table Operator with Socket-based Data Transfer
 * 
 * High-Performance Massively Parallel Data Export from Teradata to Trino
 */

#define SQL_TEXT Latin_Text
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <zlib.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/time.h>
#include "lz4.h"
#include "sqltypes_td.h"

/* Real Teradata Internal Data Type Codes confirmed by binary diagnostics */
/* Real Teradata Internal Data Type Codes confirmed by binary diagnostics */
/* Using standard SQLTYPES_TD.H enums instead of manual defines to prevent duplicate case errors */
/* #define TD_CHAR 1 */
/* #define TD_VARCHAR 2 */ 
/* ... Relying on system headers ... */

#define BATCH_SIZE 1000
#define BUFFER_SIZE 16777216  /* 16MB buffer for throughput - safe for FNC_malloc */

/* Fault Tolerance Configuration */
#define CONNECT_TIMEOUT_SECONDS 30
#define SOCKET_RECV_TIMEOUT_SECONDS 60
#define SOCKET_SEND_TIMEOUT_SECONDS 60
#define MAX_CONNECT_RETRIES 3
#define MAX_WORKER_IPS 512

typedef struct {
    char bridge_host[256];
    int bridge_port;
    char query_id[256];
    char security_token[256];
    int batch_size;
    int compression_type;
    /* Fault Tolerance: Store all worker IPs for failover */
    char all_worker_ips[MAX_WORKER_IPS][256];
    int all_worker_ports[MAX_WORKER_IPS];
    int worker_count;
    int assigned_worker_idx;  /* Initially assigned worker index */
} ExportParams_t;

typedef struct {
    INTEGER amp_id;
    BIGINT rows_processed;
    BIGINT bytes_sent;
    BIGINT null_count;
    BIGINT batches_sent;
    int error_code;
    char error_message[250];
} ExportStats_t;

/* Prototypes to avoid warnings */
void ExportToTrino(void);
void ExportToTrino_contract(INTEGER *Result, int *indicator_Result, char sqlstate[6], SQL_TEXT extname[129], SQL_TEXT specific_name[129], SQL_TEXT error_message[257]);

/* Network Helpers - Big Endian Swapping */
static int write_uint32(unsigned char *buf, unsigned int val) {
    buf[0] = (val >> 24) & 0xFF; buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;  buf[3] = val & 0xFF;
    return 4;
}

static int send_all(int sock_fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t r = send(sock_fd, p, len, 0);
        if (r < 0) return -1;
        if (r == 0) return -1;
        p += r;
        len -= r;
    }
    return 0;
}
static int write_uint16(unsigned char *buf, unsigned short val) {
    buf[0] = (val >> 8) & 0xFF; buf[1] = val & 0xFF;
    return 2;
}
static int write_int32(unsigned char *buf, int val) {
    buf[0] = (val >> 24) & 0xFF; buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;  buf[3] = val & 0xFF;
    return 4;
}
static int write_int64(unsigned char *buf, long long val) {
    buf[0] = (val >> 56) & 0xFF; buf[1] = (val >> 48) & 0xFF;
    buf[2] = (val >> 40) & 0xFF; buf[3] = (val >> 32) & 0xFF;
    buf[4] = (val >> 24) & 0xFF; buf[5] = (val >> 16) & 0xFF;
    buf[6] = (val >> 8) & 0xFF;  buf[7] = val & 0xFF;
    return 8;
}

/* Date/Time Helpers */
static int ymd_to_epoch_days(int y, int m, int d) {
    if (m <= 2) { y -= 1; m += 12; }
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m - 3) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

static long long time_to_picos(void *val) {
    unsigned int s_scaled; memcpy(&s_scaled, val, 4);
    unsigned char hour = ((unsigned char*)val)[4], min = ((unsigned char*)val)[5];
    /* Trino TIME expects picos since midnight */
    return ((long long)(hour % 24) * 3600 + (long long)(min % 60) * 60) * 1000000000000LL + (long long)s_scaled * 1000000LL;
}

static long long timestamp_to_micros(void *val) {
    unsigned int s_scaled; memcpy(&s_scaled, val, 4);
    unsigned short year; memcpy(&year, (char*)val + 4, 2);
    unsigned char mon = ((unsigned char*)val)[6], day = ((unsigned char*)val)[7], 
                  hour = ((unsigned char*)val)[8], min = ((unsigned char*)val)[9];
    int days = ymd_to_epoch_days(year, mon, day);
    /* Trino TIMESTAMP expects micros since epoch */
    return (long long)days * 86400000000LL + (long long)(hour % 24) * 3600000000LL + (long long)(min % 60) * 60000000LL + (long long)s_scaled;
}



/**
 * Connect to a host with a timeout using non-blocking sockets.
 * Returns socket fd on success, -1 on failure.
 */
static int connect_with_timeout(const char *host, int port, int timeout_seconds) {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) return -1;
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(sock_fd);
        return -1;
    }
    
    /* Set socket to non-blocking mode for connect timeout */
    int flags = fcntl(sock_fd, F_GETFL, 0);
    if (flags < 0 || fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(sock_fd);
        return -1;
    }
    
    /* Start non-blocking connect */
    int result = connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr));
    if (result == 0) {
        /* Connected immediately (rare for TCP) */
        fcntl(sock_fd, F_SETFL, flags); /* Restore blocking mode */
        return sock_fd;
    }
    
    if (errno != EINPROGRESS) {
        close(sock_fd);
        return -1;
    }
    
    /* Wait for connect with timeout using select() */
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(sock_fd, &write_fds);
    
    struct timeval tv;
    tv.tv_sec = timeout_seconds;
    tv.tv_usec = 0;
    
    result = select(sock_fd + 1, NULL, &write_fds, NULL, &tv);
    if (result <= 0) {
        /* Timeout or error */
        close(sock_fd);
        return -1;
    }
    
    /* Check if connect succeeded */
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
        close(sock_fd);
        return -1;
    }
    
    /* Restore blocking mode */
    fcntl(sock_fd, F_SETFL, flags);
    
    /* Set SO_RCVTIMEO and SO_SNDTIMEO for send/recv timeouts */
    struct timeval recv_timeout, send_timeout;
    recv_timeout.tv_sec = SOCKET_RECV_TIMEOUT_SECONDS;
    recv_timeout.tv_usec = 0;
    send_timeout.tv_sec = SOCKET_SEND_TIMEOUT_SECONDS;
    send_timeout.tv_usec = 0;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &recv_timeout, sizeof(recv_timeout));
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &send_timeout, sizeof(send_timeout));
    
    /* Enable TCP keepalive for network partition detection */
    int keepalive = 1;
    setsockopt(sock_fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));
    
    return sock_fd;
}

static void send_control_message(ExportParams_t *params, int command) {
    if (params->worker_count == 0) return;
    
    /* Connect to the primary worker to send the control message */
    int sock_fd = connect_with_timeout(params->bridge_host, params->bridge_port, 5);
    if (sock_fd < 0) return;
    
    unsigned char buf[1024];
    int off = 0;
    
    /* 1. Token Length and Token */
    int tl = strlen(params->security_token);
    off += write_int32(buf + off, tl);
    memcpy(buf + off, params->security_token, tl);
    off += tl;
    
    /* 2. Magic Number (0xFEEDFACE) */
    off += write_uint32(buf + off, 0xFEEDFACE);
    
    /* 3. Query ID Length and Query ID */
    int ql = strlen(params->query_id);
    off += write_int32(buf + off, ql);
    memcpy(buf + off, params->query_id, ql);
    off += ql;
    
    /* 4. Command ID */
    off += write_int32(buf + off, command);
    
    send_all(sock_fd, buf, off);
    
    /* Wait for acknowledgment to ensure it's processed */
    char ack[4];
    recv(sock_fd, ack, sizeof(ack), 0);
    
    close(sock_fd);
}

/**
 * Connect with retry and failover to alternate workers.
 * Returns socket fd on success, -1 on failure.
 * On success, updates params->bridge_host and params->bridge_port to the connected worker.
 */
static int connect_with_retry_failover(ExportParams_t *params, char *error_msg, int error_msg_size) {
    int retry, worker_idx;
    int sock_fd = -1;
    int start_idx = params->assigned_worker_idx;
    
    for (retry = 0; retry < MAX_CONNECT_RETRIES; retry++) {
        /* Try each worker starting from assigned, with round-robin */
        for (worker_idx = 0; worker_idx < params->worker_count; worker_idx++) {
            int try_idx = (start_idx + worker_idx) % params->worker_count;
            const char *host = params->all_worker_ips[try_idx];
            int port = params->all_worker_ports[try_idx];
            
            sock_fd = connect_with_timeout(host, port, CONNECT_TIMEOUT_SECONDS);
            if (sock_fd >= 0) {
                /* Success - update current connection info */
                strncpy(params->bridge_host, host, 255);
                params->bridge_host[255] = '\0';
                params->bridge_port = port;
                return sock_fd;
            }
        }
        
        /* All workers failed this round, wait before retry (exponential backoff) */
        if (retry < MAX_CONNECT_RETRIES - 1) {
            int wait_secs = 1 << retry; /* 1, 2, 4 seconds */
            if (wait_secs > 10) wait_secs = 10;
            sleep(wait_secs);
        }
    }
    
    /* All retries exhausted */
    snprintf(error_msg, error_msg_size, 
             "Failed to connect to any worker after %d retries. Last tried: %s:%d",
             MAX_CONNECT_RETRIES, params->bridge_host, params->bridge_port);
    return -1;
}

static int send_batch_to_bridge(int sock_fd, unsigned char *bb, int batch_offset, int rows, int compression_type, unsigned char **dest_ptr, unsigned long *dest_cap) {
    write_uint32(bb, rows);
    if (compression_type == 0) {  /* 0 = None */
        unsigned char lb[4]; write_uint32(lb, batch_offset);
        if (send_all(sock_fd, lb, 4) < 0 || send_all(sock_fd, bb, batch_offset) < 0) return -1;
        return 0;
    }
    
    /* Ensure compression buffer is large enough */
    unsigned long bound = (compression_type == 2) ? LZ4_compressBound(batch_offset) : compressBound(batch_offset);
    if (!*dest_ptr || *dest_cap < bound) {
        if (*dest_ptr) FNC_free(*dest_ptr);
        *dest_ptr = (unsigned char *)FNC_malloc(bound);
        if (!*dest_ptr) return -1;
        *dest_cap = bound;
    }

    unsigned long actual_len;
    if (compression_type == 2) { /* 2 = LZ4 */
        actual_len = LZ4_compress_default((const char*)bb, (char*)*dest_ptr, batch_offset, *dest_cap);
        if (actual_len <= 0) return -1;
    } else { /* 1 = ZLIB */
        actual_len = *dest_cap;
        if (compress(*dest_ptr, &actual_len, bb, batch_offset) != Z_OK) return -1;
    }
    
    unsigned char lb[4]; write_uint32(lb, (unsigned int)actual_len);
    if (send_all(sock_fd, lb, 4) < 0 || send_all(sock_fd, *dest_ptr, actual_len) < 0) return -1;
    return 0;
}

static int write_unicode_to_utf8(unsigned char *buf, const unsigned char *val, int bytes) {
    int i = 0, j = 0;
    unsigned char *out = buf + 2;
    while (i + 1 < bytes) {
        unsigned int cp;
        unsigned short w1 = val[i] | (val[i+1] << 8);
        i += 2;
        if (w1 >= 0xD800 && w1 <= 0xDBFF && i + 1 < bytes) {
            unsigned short w2 = val[i] | (val[i+1] << 8);
            i += 2;
            cp = (((w1 & 0x3FF) << 10) | (w2 & 0x3FF)) + 0x10000;
        } else {
            cp = w1;
        }
        if (cp < 0x80) out[j++] = cp;
        else if (cp < 0x800) { out[j++] = (cp >> 6)|0xC0; out[j++] = (cp&0x3F)|0x80; }
        else if (cp < 0x10000) { out[j++] = (cp >> 12)|0xE0; out[j++] = ((cp >> 6)&0x3F)|0x80; out[j++] = (cp&0x3F)|0x80; }
        else { out[j++] = (cp >> 18)|0xF0; out[j++] = ((cp >> 12)&0x3F)|0x80; out[j++] = ((cp >> 6)&0x3F)|0x80; out[j++] = (cp&0x3F)|0x80; }
    }
    write_uint16(buf, (unsigned short)j);
    return 2 + j;
}

static void parse_params_from_stream(ExportParams_t *params, FNC_TblOpHandle_t *param_stream) {
    char *target_ips = (char *)FNC_malloc(32768);
    if (target_ips) target_ips[0] = '\0';
    params->query_id[0] = '\0';
    params->batch_size = BATCH_SIZE;

    if (param_stream && FNC_TblOpRead(param_stream) == TBLOP_SUCCESS && target_ips) {
        int c;
        for (c = 0; c < 5; c++) {
            if (c >= FNC_TblOpGetColCount(1, ISINPUT)) break;
            void *val = param_stream->row->columnptr[c];
            if (!val || TBLOPISNULL(param_stream->row->indicators, c)) continue;
            
            int actual_len = param_stream->row->lengths[c];
            if (actual_len < 0) continue;
            
            if (c == 3) {
                int bs = 0;
                memcpy(&bs, val, 4);
                if (bs > 0) params->batch_size = bs;
                continue;
            }

            char tmp[1024] = "";
            char *src = (char*)val;
            int src_len = actual_len;

            /* Check for VARCHAR prefix (2 bytes length) */
            if (actual_len >= 2) {
                unsigned short vlen = *(unsigned short*)val;
                if (vlen == (unsigned short)(actual_len - 2)) {
                    src += 2; src_len = vlen;
                }
            }

            if (src_len > 0) {
                /* Detect UTF-16: if second byte is 0 */
                if (src_len >= 2 && src[1] == '\0') {
                    int i, j = 0;
                    for (i = 0; i < src_len && j < 1023; i += 2) {
                        tmp[j++] = src[i];
                    }
                    tmp[j] = '\0';
                } else {
                    int copy_len = (src_len > 1023) ? 1023 : src_len;
                    memcpy(tmp, src, copy_len);
                    tmp[copy_len] = '\0';
                }
            }

            /* Trim trailing spaces */
            int len = strlen(tmp);
            if (len > 0) {
                char *end = tmp + len - 1;
                while(end >= tmp && (*end == ' ' || *end == '\n' || *end == '\r' || *end == '\0')) { *end = '\0'; end--; }
            }

            if (c == 0) { strncpy(params->bridge_host, tmp, 255); params->bridge_host[255] = '\0'; strcpy(target_ips, tmp); }
            else if (c == 1) { strncpy(params->query_id, tmp, 255); params->query_id[255] = '\0'; }
            else if (c == 2) { strncpy(params->security_token, tmp, 255); params->security_token[255] = '\0'; }
            else if (c == 4) {
                if (strstr(tmp, "LZ4")) params->compression_type = 2;
                else if (strstr(tmp, "ZLIB")) params->compression_type = 1;
                else params->compression_type = 0;
            }
        }
    }

    /* Fallback for Security Token */
    if (params->security_token[0] == '\0') {
        char *env = getenv("EXPORT_SECURITY_TOKEN");
        if (env) strcpy(params->security_token, env);
    }

    /* Fallback for Target IPs */
    if (target_ips[0] == '\0') {
        char *env = getenv("EXPORT_BRIDGE_HOSTS");
        if (env) strcpy(target_ips, env);
    }
    /* Fallback for Query ID */
    if (params->query_id[0] == '\0') {
        char *env = getenv("EXPORT_QUERY_ID");
        strcpy(params->query_id, env ? env : "default-query");
    }

    /* Parse all worker IPs and store them for failover capability.
     * Each AMP selects a primary worker based on PID for distribution.
     * XOR high bits to low bits to break parity patterns (all odd/even PIDs). */
    INTEGER raw_pid = getpid();
    INTEGER amp_id = raw_pid ^ (raw_pid >> 8); /* Mix high bits into low bits for better distribution */
    char *ips[MAX_WORKER_IPS]; 
    int ip_count = 0;
    char *saveptr;
    char *token = strtok_r(target_ips, ",", &saveptr);
    while (token && ip_count < MAX_WORKER_IPS) {
        while (*token == ' ') token++; /* skip leading spaces */
        ips[ip_count++] = token;
        token = strtok_r(NULL, ",", &saveptr);
    }

    /* Store all worker IPs for failover */
    params->worker_count = 0;
    int i;
    for (i = 0; i < ip_count && i < MAX_WORKER_IPS; i++) {
        char temp_ip[256];
        strncpy(temp_ip, ips[i], 255);
        temp_ip[255] = '\0';
        
        char *colon = strchr(temp_ip, ':');
        if (colon) {
            *colon = '\0';
            strncpy(params->all_worker_ips[i], temp_ip, 255);
            params->all_worker_ips[i][255] = '\0';
            params->all_worker_ports[i] = atoi(colon + 1);
        } else {
            strncpy(params->all_worker_ips[i], temp_ip, 255);
            params->all_worker_ips[i][255] = '\0';
            params->all_worker_ports[i] = 9999;
        }
        params->worker_count++;
    }
    
    /* Select primary worker based on PID for even load balancing.
     * PID provides unique value per process enabling even distribution. */
    if (params->worker_count > 0) {
        params->assigned_worker_idx = amp_id % params->worker_count;
        strncpy(params->bridge_host, params->all_worker_ips[params->assigned_worker_idx], 255);
        params->bridge_host[255] = '\0';
        params->bridge_port = params->all_worker_ports[params->assigned_worker_idx];
    } else {
        /* No workers available - parameters were empty */
        params->worker_count = 0;
        params->bridge_host[0] = '\0';
        params->bridge_port = 0;
    }
    
    if (target_ips) FNC_free(target_ips);
}

static int write_hex_string(unsigned char *buf, void *value, int bytesize) {
    char hex[] = "0123456789ABCDEF";
    unsigned char *p = (unsigned char*)value;
    /* Limit hex string to avoid internal buffer overflows in batch */
    int len = bytesize * 2;
    if (len > 32767) len = 32767; 
    write_uint16(buf, (unsigned short)len);
    int i;
    for (i = 0; i < len/2; i++) {
        buf[2 + i*2] = hex[(p[i] >> 4) & 0xF];
        buf[2 + i*2 + 1] = hex[p[i] & 0xF];
    }
    return 2 + len;
}

static int write_decimal_binary(unsigned char *buf, void *value, int bytesize) {
    if (bytesize <= 8) {
        long long v = 0;
        if (bytesize == 1) v = *(__int8_t*)value;
        else if (bytesize == 2) v = *(__int16_t*)value;
        else if (bytesize == 4) v = *(__int32_t*)value;
        else if (bytesize == 8) v = *(long long*)value;
        return write_int64(buf, v);
    } else {
        /* 16-byte decimal, Trino expects Big Endian. Teradata is Little Endian. */
        unsigned char *p = (unsigned char *)value;
        int i;
        for (i = 0; i < 16; i++) {
            buf[i] = p[15 - i];
        }
        return 16;
    }
}

void ExportToTrino_contract(INTEGER *Result, int *indicator_Result, char sqlstate[6], SQL_TEXT extname[129], SQL_TEXT specific_name[129], SQL_TEXT error_message[257]) {
    FNC_TblOpColumnDef_t *oCols;
    int incount, outcount, i;
    Stream_Fmt_en format = INDICFMT1;
    char mycontract[] = "ExportToTrino v4.18";
    FNC_TblOpGetStreamCount(&incount, &outcount);
    oCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(7));
    TblOpINITCOLDEF(oCols, 7);
    oCols->num_columns = 7;
    oCols->column_types[0].datatype = INTEGER_DT; oCols->column_types[0].bytesize = 4;
    oCols->column_types[1].datatype = BIGINT_DT;  oCols->column_types[1].bytesize = 8;
    oCols->column_types[2].datatype = BIGINT_DT;  oCols->column_types[2].bytesize = 8;
    oCols->column_types[3].datatype = BIGINT_DT;  oCols->column_types[3].bytesize = 8;
    oCols->column_types[4].datatype = BIGINT_DT;  oCols->column_types[4].bytesize = 8;
    oCols->column_types[5].datatype = INTEGER_DT; oCols->column_types[5].bytesize = 4;
    oCols->column_types[6].datatype = VARCHAR_DT; oCols->column_types[6].bytesize = 258; oCols->column_types[6].size.length = 256; oCols->column_types[6].charset = LATIN_CT;
    FNC_TblOpSetContractDef(mycontract, strlen(mycontract) + 1);
    FNC_TblOpSetOutputColDef(0, oCols);
    /* Set format for primary data stream and output stream */
    FNC_TblOpSetFormat("RECFMT", 0, ISINPUT, &format, sizeof(format));
    FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));
    FNC_free(oCols); *Result = 1; *indicator_Result = 0;
}

void ExportToTrino(void) {
    FNC_TblOpHandle_t *in = NULL, *out = NULL, *param_in = NULL;
    int col, sock_fd = -1, batch_offset = 4, rows_in_batch = 0, tic = 0;
    FNC_TblOpColumnDef_t *iCols = NULL;
    ExportParams_t *params = NULL;
    ExportStats_t stats;
    unsigned char *bb = NULL;
    unsigned char *dest = NULL;
    unsigned long dest_cap = 0;
    int incount, outcount;

    memset(&stats, 0, sizeof(stats));
    FNC_TblOpGetStreamCount(&incount, &outcount);
    
    in = FNC_TblOpOpen(0, 'r', 0);
    out = FNC_TblOpOpen(0, 'w', 0);
    if (incount > 1) param_in = FNC_TblOpOpen(1, 'r', 0);

    params = (ExportParams_t *)FNC_malloc(sizeof(ExportParams_t));
    if (!params) {
        stats.error_code = 1007; strcpy(stats.error_message, "Params heap malloc failed"); goto send_status;
    }
    memset(params, 0, sizeof(ExportParams_t));
    parse_params_from_stream(params, param_in);

    if (!in || !out) {
        stats.error_code = 1001; strcpy(stats.error_message, "Stream open failed"); goto send_status;
    }

    tic = FNC_TblOpGetColCount(0, ISINPUT);
    iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(tic));
    TblOpINITCOLDEF(iCols, tic);
    FNC_TblOpGetColDef(0, ISINPUT, iCols);

    bb = (unsigned char *)FNC_malloc(BUFFER_SIZE);
    if (!bb) {
        stats.error_code = 1005; strcpy(stats.error_message, "Batch buffer malloc failed"); goto send_status;
    }
    
    /* Enterprise Fault Tolerance: Connect with timeout, retry, and failover */
    sock_fd = connect_with_retry_failover(params, stats.error_message, sizeof(stats.error_message));
    if (sock_fd < 0) {
        stats.error_code = errno ? errno : 1006;
        goto send_status;
    }

    /* Socket optimization: TCP_NODELAY and large send buffer */
    int flag = 1;
    setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
    int sndbuf = 4194304; /* 4MB */
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, (char *)&sndbuf, sizeof(int));

    unsigned char ph[4096]; int ho = 0; 
    
    /* 1. Security Token (if configured) */
    if (params->security_token[0] != '\0') {
        int tl = strlen(params->security_token);
        ho += write_uint32(ph + ho, tl);
        memcpy(ph + ho, params->security_token, tl);
        ho += tl;
    }

    /* 2. Query ID */
    int ql = strlen(params->query_id);
    ho += write_uint32(ph + ho, ql); memcpy(ph+ho, params->query_id, ql); ho += ql;

    /* 3. Compression Type Flag */
    ho += write_uint32(ph + ho, params->compression_type);
    
    /* Allocate enough space for potentially large column metadata JSON */
    int sj_size = tic * 256 + 128;
    char *sj = (char *)FNC_malloc(sj_size);
    if (!sj) {
        stats.error_code = 1002; strcpy(stats.error_message, "Metadata malloc failed"); goto send_status;
    }
    strcpy(sj, "{\"columns\":[");
    for (col = 0; col < tic; col++) {
        char cd[512]; const char *tn; int dt = iCols->column_types[col].datatype;
        int precision = -1;
        int scale = -1;
        /* Real FNC codes (numeric): CHAR=1 VARCHAR=2 BYTEINT=7 SMALLINT=8 INTEGER=9
         * FLOAT=10 DECIMAL=14 DATE=15 TIME=16 TIMESTAMP=17 BIGINT=36
         * Use literals so this compiles against both mock and system headers. */
        switch(dt) {
            case 1: case 2: case 3: case 4:
            case 20: case 21: case 22: case 23: case 24:
            case 41:
            case 52: case 54:
            case 60: case 61: case 62: case 63: case 64:
            case 70: case 71: case 72: case 73: case 74: case 75: case 76:
            case 77: case 78: case 79: case 80: case 81: case 82:
            case 90: case 91: case 92: case 93: case 94:
                tn = "VARCHAR";
                break;
            case 30: case 31: case 40:
                tn = "VARBINARY";
                break;
            case 7: case 8: case 9:
                tn = "INTEGER";
                break;
            case 36:
                tn = "BIGINT";
                break;
            case 10: case 11: case 12:
                tn = "DOUBLE";
                break;
            case 15:
                tn = "DATE";
                break;
            case 16:
                tn = "TIME";
                break;
            case 17:
                tn = "TIMESTAMP";
                break;
            case 13: case 14:
                tn = (iCols->column_types[col].bytesize <= 8) ? "DECIMAL_SHORT" : "DECIMAL_LONG";
                precision = iCols->column_types[col].size.range.totaldigit;
                scale = iCols->column_types[col].size.range.fracdigit;
                break;
            default:
                if (iCols->column_types[col].bytesize == 1) tn = "INTEGER";
                else if (iCols->column_types[col].bytesize == 2) tn = "INTEGER";
                else if (iCols->column_types[col].bytesize == 4) tn = "INTEGER";
                else if (iCols->column_types[col].bytesize == 8) tn = "BIGINT";
                else tn = "VARCHAR";
                break;
        }
        if (precision > 0 && scale >= 0) {
            snprintf(cd, 512, "%s{\"name\":\"col_%d\",\"type\":\"%s\",\"precision\":%d,\"scale\":%d}",
                     col > 0 ? "," : "", col, tn, precision, scale);
        } else {
            snprintf(cd, 512, "%s{\"name\":\"col_%d\",\"type\":\"%s\"}", col > 0 ? "," : "", col, tn);
        }
        strcat(sj, cd);
    }
    strcat(sj, "]}"); int sj_len = strlen(sj);
    ho += write_uint32(ph + ho, sj_len);
    if (send_all(sock_fd, ph, ho) < 0 || send_all(sock_fd, sj, sj_len) < 0) {
        stats.error_code = 1003; strcpy(stats.error_message, "Handshake send failed"); 
        FNC_free(sj); sj = NULL; goto send_status;
    }
    FNC_free(sj); sj = NULL;

    while (FNC_TblOpRead(in) == TBLOP_SUCCESS) {
        stats.rows_processed++; rows_in_batch++;
        for (col = 0; col < tic; col++) {
            bb[batch_offset++] = TBLOPISNULL(in->row->indicators, col) ? 1 : 0;
            if (TBLOPISNULL(in->row->indicators, col)) stats.null_count++;
            else {
                int dt = iCols->column_types[col].datatype;
                int cs = iCols->column_types[col].charset;
                void *val = in->row->columnptr[col];
                int bsz = iCols->column_types[col].bytesize;

                /* Real FNC codes: CHAR=1 VARCHAR=2 BYTEINT=7 SMALLINT=8 INTEGER=9
                 * FLOAT=10 DECIMAL=14 DATE=15 TIME=16 TIMESTAMP=17 BIGINT=36 */
                if (dt == 2 || dt == 3 || dt == 4 || dt == 21 || dt == 22 || dt == 24) {
                    short blen = *(short*)val;
                    if (cs == 2 || cs == 6) batch_offset += write_unicode_to_utf8(bb + batch_offset, (unsigned char*)val + 2, blen);
                    else {
                        write_uint16(bb + batch_offset, blen); memcpy(bb + batch_offset + 2, (char*)val + 2, blen);
                        batch_offset += 2 + blen;
                    }
                } else if (dt == 1 || dt == 20 || dt == 23) {
                    if (cs == 2 || cs == 6) batch_offset += write_unicode_to_utf8(bb + batch_offset, (unsigned char*)val, bsz);
                    else {
                        write_uint16(bb + batch_offset, (unsigned short)bsz); memcpy(bb + batch_offset + 2, (char*)val, bsz);
                        batch_offset += 2 + bsz;
                    }
                } else if (dt == 30) {
                    write_uint16(bb + batch_offset, (unsigned short)bsz);
                    memcpy(bb + batch_offset + 2, (unsigned char*)val, bsz);
                    batch_offset += 2 + bsz;
                } else if (dt == 31) {
                    unsigned short blen = *(unsigned short*)val;
                    write_uint16(bb + batch_offset, blen);
                    memcpy(bb + batch_offset + 2, (unsigned char*)val + 2, blen);
                    batch_offset += 2 + blen;
                } else if (dt == 9) {
                    batch_offset += write_int32(bb + batch_offset, *(int*)val);
                } else if (dt == 36) {
                    batch_offset += write_int64(bb + batch_offset, *(long long*)val);
                } else if (dt == 8) {
                    batch_offset += write_int32(bb + batch_offset, (int)*(short*)val);
                } else if (dt == 7) {
                    batch_offset += write_int32(bb + batch_offset, (int)*(__int8_t*)val);
                } else if (dt == 10 || dt == 11 || dt == 12) {
                    long long lv; memcpy(&lv, val, 8);
                    batch_offset += write_int64(bb + batch_offset, lv);
                } else if (dt == 15) {
                    int d = *(int*)val;
                    int y_off = d / 10000;
                    int md = d % 10000;
                    if (md < 0) { y_off--; md += 10000; }
                    int year = y_off + 1900;
                    int month = md / 100;
                    int day = md % 100;
                    batch_offset += write_int32(bb + batch_offset, ymd_to_epoch_days(year, month, day));
                } else if (dt == 16) {
                    batch_offset += write_int64(bb + batch_offset, time_to_picos(val));
                } else if (dt == 17) {
                    batch_offset += write_int64(bb + batch_offset, timestamp_to_micros(val));
                } else if (dt == 13 || dt == 14) {
                    batch_offset += write_decimal_binary(bb + batch_offset, val, bsz);
                } else {
                    if (bsz == 1) batch_offset += write_int32(bb + batch_offset, (int)*(__int8_t*)val);
                    else if (bsz == 2) batch_offset += write_int32(bb + batch_offset, (int)*(short*)val);
                    else if (bsz == 4) batch_offset += write_int32(bb + batch_offset, *(int*)val);
                    else if (bsz == 8) batch_offset += write_int64(bb + batch_offset, *(long long*)val);
                    else batch_offset += write_hex_string(bb + batch_offset, val, bsz);
                }
            }
        }
        /* Safety check: ensure we don't overflow bb even with wide rows. 
           Max Teradata row is 1MB, so we check for 1MB safety margin. */
        if (rows_in_batch >= params->batch_size || batch_offset > BUFFER_SIZE - 1048576) {
            if (send_batch_to_bridge(sock_fd, bb, batch_offset, rows_in_batch, params->compression_type, &dest, &dest_cap) < 0) {
                stats.error_code = 1004; strcpy(stats.error_message, "Batch send failed"); break;
            }
            stats.batches_sent++; stats.bytes_sent += batch_offset;
            batch_offset = 4; rows_in_batch = 0;
        }
    }

    if (rows_in_batch > 0 && stats.error_code == 0) {
        send_batch_to_bridge(sock_fd, bb, batch_offset, rows_in_batch, params->compression_type, &dest, &dest_cap);
        stats.batches_sent++; stats.bytes_sent += batch_offset;
    }
    
    unsigned char emsg[4] = {0,0,0,0}; send_all(sock_fd, emsg, 4); 
    
    /* Close data socket after all pages are pushed. Bridge treats data-socket close
     * as the authoritative AMP completion signal (push-before-decrement EOS), so the
     * extra TERADATA_FINISHED TCP hop is no longer required for correctness and is
     * skipped to cut per-AMP latency under high concurrency. */
    close(sock_fd);
    sock_fd = -1;

send_status:
    if (sock_fd >= 0) close(sock_fd);
    static INTEGER ra; static BIGINT rr, rb, rn, rba; static INTEGER rc; static char rs[300];
    INTEGER pid_for_dist = getpid() ^ (getpid() >> 8); /* Same formula as distribution */
    ra = pid_for_dist; /* Return mixed PID so Java can predict worker routing correctly */
    rr = stats.rows_processed; rb = stats.bytes_sent; rn = stats.null_count; rba = stats.batches_sent; rc = tic;
    int char_len;
    if (stats.error_code == 0) {
        char_len = snprintf(rs + 2, 256, "[%s:%d] AMP:%d PID:%d SUCCESS (Query: %s)", params->bridge_host, params->bridge_port, ra, (int)getpid(), params->query_id);
    } else char_len = snprintf(rs + 2, 256, "ERROR %d: %s", stats.error_code, stats.error_message);
    if (char_len > 256) char_len = 256;
    unsigned short slen = (unsigned short)char_len;
    memcpy(rs, &slen, 2);
    if (out) {
        out->row->columnptr[0] = (void *)&ra; out->row->columnptr[1] = (void *)&rr; out->row->columnptr[2] = (void *)&rb;
        out->row->columnptr[3] = (void *)&rn; out->row->columnptr[4] = (void *)&rba; out->row->columnptr[5] = (void *)&rc;
        out->row->columnptr[6] = (void *)rs; out->row->lengths[0] = 4; out->row->lengths[1] = 8; out->row->lengths[2] = 8;
        out->row->lengths[3] = 8; out->row->lengths[4] = 8; out->row->lengths[5] = 4; out->row->lengths[6] = 2 + slen;
        memset(out->row->indicators, 0, 7); FNC_TblOpWrite(out); FNC_TblOpClose(out);
    }
    if (iCols) FNC_free(iCols);
    if (bb) FNC_free(bb);
    if (dest) FNC_free(dest);
    if (params) FNC_free(params);
    if (in) FNC_TblOpClose(in);
    if (param_in) FNC_TblOpClose(param_in);
}