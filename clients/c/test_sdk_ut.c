/*
 * VEMB V16 Client SDK Unit Tests
 *
 * Build:
 *   gcc -I../../src test_sdk_ut.c vemb_v16_client_sdk.c ../../src/vemb_v16_net.c -o test_sdk_ut
 * Run:
 *   ./test_sdk_ut
 *
 * Pure unit tests — no running server required.
 */

#include "vemb_v16_client_sdk.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

static int g_passed = 0;
static int g_failed = 0;

#define FAIL_AT(msg) do { \
    printf("    [ASSERT] %s:%d: %s\n", __FILE__, __LINE__, msg); \
    return -1; \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) FAIL_AT(#cond); \
} while (0)

#define ASSERT_EQ_UINT(actual, expected) do { \
    unsigned int _a = (actual), _e = (expected); \
    if (_a != _e) { \
        printf("    [ASSERT] %s:%d: expected=%u got=%u\n", \
               __FILE__, __LINE__, _e, _a); \
        return -1; \
    } \
} while (0)

#define ASSERT_EQ_SIZE(actual, expected) do { \
    size_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        printf("    [ASSERT] %s:%d: expected=%zu got=%zu\n", \
               __FILE__, __LINE__, _e, _a); \
        return -1; \
    } \
} while (0)

#define ASSERT_EQ_SSIZE(actual, expected) do { \
    ssize_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        printf("    [ASSERT] %s:%d: expected=%zd got=%zd\n", \
               __FILE__, __LINE__, _e, _a); \
        return -1; \
    } \
} while (0)

#define ASSERT_EQ_U64(actual, expected) do { \
    uint64_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        printf("    [ASSERT] %s:%d: expected=%lu got=%lu\n", \
               __FILE__, __LINE__, (unsigned long)_e, (unsigned long)_a); \
        return -1; \
    } \
} while (0)

#define ASSERT_MEMEQ(a, b, len) do { \
    if (memcmp((a), (b), (len)) != 0) FAIL_AT("memory mismatch"); \
} while (0)

#define RUN(name) do { \
    int rc = test_##name(); \
    if (rc == 0) { g_passed++; printf("  [PASS] %s\n", #name); } \
    else        { g_failed++; printf("  [FAIL] %s\n", #name); } \
} while (0)

/* ===================================================================== */
/* vemb_v16_build_combined_key                                           */
/* ===================================================================== */

static int test_combined_key_ascii(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), "setA", "elem1", &len) == 0);
    ASSERT_EQ_UINT(len, 10u);  /* 4 + 1 + 5 */
    ASSERT_TRUE(out[4] == '\0');
    ASSERT_MEMEQ(out, "setA", 4);
    ASSERT_MEMEQ(out + 5, "elem1", 5);
    return 0;
}

static int test_combined_key_with_separator_in_name(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    /* build_combined_key uses strlen which stops at \0 in the source strings.
     * "a\0b" literal has strlen=1 (stops at first \0), "c\0d" also 1.
     * Result is "a\0c" with len = 1 + 1 + 1 = 3. */
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), "a\0b", "c\0d", &len) == 0);
    ASSERT_EQ_UINT(len, 3u);  /* 1 + 1 + 1 */
    ASSERT_TRUE(out[0] == 'a');
    ASSERT_TRUE(out[1] == '\0');
    ASSERT_TRUE(out[2] == 'c');
    return 0;
}

static int test_combined_key_empty_set(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), "", "elem", &len) == 0);
    ASSERT_EQ_UINT(len, 5u);  /* 0 + 1 + 4 */
    ASSERT_TRUE(out[0] == '\0');
    ASSERT_MEMEQ(out + 1, "elem", 4);
    return 0;
}

static int test_combined_key_empty_elem(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), "set", "", &len) == 0);
    ASSERT_EQ_UINT(len, 4u);  /* 3 + 1 + 0 */
    ASSERT_MEMEQ(out, "set", 3);
    ASSERT_TRUE(out[3] == '\0');
    return 0;
}

static int test_combined_key_both_empty(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), "", "", &len) == 0);
    ASSERT_EQ_UINT(len, 1u);  /* 0 + 1 + 0 */
    ASSERT_TRUE(out[0] == '\0');
    return 0;
}

static int test_combined_key_max_length(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    char set[64], elem[64];
    memset(set, 's', sizeof(set) - 1);
    set[sizeof(set) - 1] = '\0';
    memset(elem, 'e', sizeof(elem) - 1);
    elem[sizeof(elem) - 1] = '\0';
    /* 63 + 1 + 63 = 127 = VEMB_V16_MAX_KEY_LEN - 1, should fit */
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), set, elem, &len) == 0);
    ASSERT_EQ_UINT(len, 127u);
    return 0;
}

static int test_combined_key_exact_overflow(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    char set[64], elem[65];
    memset(set, 's', sizeof(set) - 1);
    set[sizeof(set) - 1] = '\0';
    memset(elem, 'e', sizeof(elem) - 1);
    elem[sizeof(elem) - 1] = '\0';
    /* 63 + 1 + 64 = 128 = VEMB_V16_MAX_KEY_LEN, should fail (>= out_cap) */
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), set, elem, &len) == -1);
    return 0;
}

static int test_combined_key_way_too_long(void) {
    char out[VEMB_V16_MAX_KEY_LEN];
    uint32_t len;
    char set[VEMB_V16_MAX_KEY_LEN];
    char elem[VEMB_V16_MAX_KEY_LEN];
    memset(set, 's', sizeof(set) - 1); set[sizeof(set) - 1] = '\0';
    memset(elem, 'e', sizeof(elem) - 1); elem[sizeof(elem) - 1] = '\0';
    ASSERT_TRUE(vemb_v16_build_combined_key(out, sizeof(out), set, elem, &len) == -1);
    return 0;
}

/* ===================================================================== */
/* vemb_v16_serialize_hello                                              */
/* ===================================================================== */

static int test_serialize_hello_normal(void) {
    char buf[64];
    ssize_t n = vemb_v16_serialize_hello(buf, sizeof(buf), 300, 0x01);
    ASSERT_EQ_SSIZE(n, (ssize_t)(sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_alloc_req_t)));

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    ASSERT_EQ_UINT(hdr->magic, VEMB_V16_MAGIC);
    ASSERT_EQ_UINT(hdr->version, VEMB_V16_VERSION);
    ASSERT_EQ_UINT(hdr->type, (uint16_t)VEMB_V16_NET_HELLO);
    ASSERT_EQ_UINT(hdr->payload_len, (uint32_t)sizeof(vemb_v16_alloc_req_t));
    ASSERT_EQ_U64(hdr->channel_id, 0u);
    ASSERT_EQ_UINT(hdr->req_id, 0u);

    vemb_v16_alloc_req_t *req = (vemb_v16_alloc_req_t *)(buf + sizeof(*hdr));
    ASSERT_EQ_UINT(req->vector_dim, 300u);
    ASSERT_EQ_UINT(req->flags, 0x01u);
    return 0;
}

static int test_serialize_hello_minimal(void) {
    char buf[64];
    ssize_t n = vemb_v16_serialize_hello(buf, sizeof(buf), 1, 0);
    ASSERT_TRUE(n > 0);
    vemb_v16_alloc_req_t *req = (vemb_v16_alloc_req_t *)(buf + sizeof(vemb_v16_net_hdr_t));
    ASSERT_EQ_UINT(req->vector_dim, 1u);
    ASSERT_EQ_UINT(req->flags, 0u);
    return 0;
}

static int test_serialize_hello_buf_exact(void) {
    char buf[sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_alloc_req_t)];
    ssize_t n = vemb_v16_serialize_hello(buf, sizeof(buf), 300, 0);
    ASSERT_EQ_SSIZE(n, (ssize_t)sizeof(buf));
    return 0;
}

static int test_serialize_hello_buf_too_small(void) {
    char buf[sizeof(vemb_v16_net_hdr_t) + sizeof(vemb_v16_alloc_req_t) - 1];
    ssize_t n = vemb_v16_serialize_hello(buf, sizeof(buf), 300, 0);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_hello_buf_zero(void) {
    ssize_t n = vemb_v16_serialize_hello(NULL, 0, 300, 0);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

/* ===================================================================== */
/* vemb_v16_serialize_vadd                                               */
/* ===================================================================== */

static int test_serialize_vadd_normal(void) {
    char buf[4096];
    float vec[4] = {1.0f, 2.0f, 3.0f, 4.0f};
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf),
                                        0x1234, 42,
                                        "set\0elem", 8,
                                        vec, 4);
    ASSERT_TRUE(n > 0);

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    ASSERT_EQ_UINT(hdr->magic, VEMB_V16_MAGIC);
    ASSERT_EQ_UINT(hdr->type, (uint16_t)VEMB_V16_NET_REQUEST);
    ASSERT_EQ_U64(hdr->channel_id, 0x1234u);
    ASSERT_EQ_UINT(hdr->req_id, 42u);

    vemb_v16_req_t *req = (vemb_v16_req_t *)(buf + sizeof(*hdr));
    ASSERT_EQ_UINT(req->op, (uint8_t)VEMB_V16_OP_VADD_INLINE);
    ASSERT_EQ_UINT(req->key_len, 8u);
    ASSERT_MEMEQ(req->key, "set\0elem", 8);
    ASSERT_EQ_UINT(req->dim, 4u);
    ASSERT_EQ_UINT(req->vector_bytes, 4u * sizeof(float));
    ASSERT_MEMEQ(req->vector, vec, req->vector_bytes);

    uint32_t expected_payload = (uint32_t)vemb_v16_req_inline_len(req->vector_bytes);
    ASSERT_EQ_UINT(hdr->payload_len, expected_payload);
    return 0;
}

static int test_serialize_vadd_max_dim(void) {
    char buf[65536];
    float vec[VEMB_V16_MAX_DIM];
    for (uint32_t i = 0; i < VEMB_V16_MAX_DIM; i++) vec[i] = (float)i;
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf),
                                        0, 0,
                                        "k", 1,
                                        vec, VEMB_V16_MAX_DIM);
    ASSERT_TRUE(n > 0);
    vemb_v16_req_t *req = (vemb_v16_req_t *)(buf + sizeof(vemb_v16_net_hdr_t));
    ASSERT_EQ_UINT(req->dim, (uint32_t)VEMB_V16_MAX_DIM);
    ASSERT_EQ_UINT(req->vector_bytes, (uint32_t)(VEMB_V16_MAX_DIM * sizeof(float)));
    return 0;
}

static int test_serialize_vadd_null_key(void) {
    char buf[4096];
    float vec[4] = {1.0f};
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, NULL, 4, vec, 4);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vadd_zero_key_len(void) {
    char buf[4096];
    float vec[4] = {1.0f};
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, "", 0, vec, 4);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vadd_key_too_long(void) {
    char buf[4096];
    float vec[4] = {1.0f};
    char key[VEMB_V16_MAX_KEY_LEN];
    memset(key, 'k', sizeof(key));
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, key, (uint32_t)sizeof(key), vec, 4);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vadd_null_vector(void) {
    char buf[4096];
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, "k", 1, NULL, 4);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vadd_zero_dim(void) {
    char buf[4096];
    float vec[4] = {1.0f};
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, "k", 1, vec, 0);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vadd_dim_too_large(void) {
    char buf[4096];
    float vec[4] = {1.0f};
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, "k", 1, vec, VEMB_V16_MAX_DIM + 1);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vadd_buf_exact(void) {
    float vec[4] = {1.0f, 2.0f, 3.0f, 4.0f};
    size_t need = sizeof(vemb_v16_net_hdr_t) + vemb_v16_req_inline_len(4 * sizeof(float));
    char *buf = malloc(need);
    ssize_t n = vemb_v16_serialize_vadd(buf, need, 0, 0, "k", 1, vec, 4);
    ASSERT_EQ_SSIZE(n, (ssize_t)need);
    free(buf);
    return 0;
}

static int test_serialize_vadd_buf_too_small(void) {
    char buf[sizeof(vemb_v16_net_hdr_t)];  /* not enough for payload */
    float vec[300];
    memset(vec, 0, sizeof(vec));
    ssize_t n = vemb_v16_serialize_vadd(buf, sizeof(buf), 0, 0, "k", 1, vec, 300);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

/* ===================================================================== */
/* vemb_v16_serialize_vemb                                               */
/* ===================================================================== */

static int test_serialize_vemb_normal(void) {
    char buf[2048];
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf),
                                        0xABCD, 99,
                                        "myset\0myelem", 12,
                                        300);
    ASSERT_TRUE(n > 0);

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)buf;
    ASSERT_EQ_UINT(hdr->type, (uint16_t)VEMB_V16_NET_REQUEST);
    ASSERT_EQ_U64(hdr->channel_id, 0xABCDu);
    ASSERT_EQ_UINT(hdr->req_id, 99u);

    vemb_v16_req_t *req = (vemb_v16_req_t *)(buf + sizeof(*hdr));
    ASSERT_EQ_UINT(req->op, (uint8_t)VEMB_V16_OP_VEMB_HANDLE);
    ASSERT_EQ_UINT(req->key_len, 12u);
    ASSERT_MEMEQ(req->key, "myset\0myelem", 12);
    ASSERT_EQ_UINT(req->dim, 300u);

    uint32_t expected_payload = (uint32_t)vemb_v16_req_handle_len();
    ASSERT_EQ_UINT(hdr->payload_len, expected_payload);
    return 0;
}

static int test_serialize_vemb_null_key(void) {
    char buf[2048];
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf), 0, 0, NULL, 4, 300);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vemb_zero_key_len(void) {
    char buf[2048];
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf), 0, 0, "", 0, 300);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vemb_key_too_long(void) {
    char buf[2048];
    char key[VEMB_V16_MAX_KEY_LEN];
    memset(key, 'k', sizeof(key));
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf), 0, 0, key, (uint32_t)sizeof(key), 300);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vemb_zero_dim(void) {
    char buf[2048];
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf), 0, 0, "k", 1, 0);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vemb_dim_too_large(void) {
    char buf[2048];
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf), 0, 0, "k", 1, VEMB_V16_MAX_DIM + 1);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

static int test_serialize_vemb_buf_exact(void) {
    size_t need = sizeof(vemb_v16_net_hdr_t) + vemb_v16_req_handle_len();
    char *buf = malloc(need);
    ssize_t n = vemb_v16_serialize_vemb(buf, need, 0, 0, "k", 1, 300);
    ASSERT_EQ_SSIZE(n, (ssize_t)need);
    free(buf);
    return 0;
}

static int test_serialize_vemb_buf_too_small(void) {
    char buf[sizeof(vemb_v16_net_hdr_t)];
    ssize_t n = vemb_v16_serialize_vemb(buf, sizeof(buf), 0, 0, "k", 1, 300);
    ASSERT_EQ_SSIZE(n, -1);
    return 0;
}

/* ===================================================================== */
/* vemb_v16_murmur3                                                      */
/* ===================================================================== */

static int test_murmur3_empty(void) {
    uint32_t h = vemb_v16_murmur3("", 0);
    ASSERT_EQ_UINT(h, 0xD9AAF7D3u);
    return 0;
}

static int test_murmur3_single_char(void) {
    uint32_t h = vemb_v16_murmur3("a", 1);
    ASSERT_EQ_UINT(h, 0xC795411Cu);
    return 0;
}

static int test_murmur3_deterministic(void) {
    uint32_t h1 = vemb_v16_murmur3("hello", 5);
    uint32_t h2 = vemb_v16_murmur3("hello", 5);
    ASSERT_EQ_UINT(h1, h2);
    return 0;
}

static int test_murmur3_avalanche(void) {
    /* tiny change should produce very different hash */
    uint32_t h1 = vemb_v16_murmur3("hello", 5);
    uint32_t h2 = vemb_v16_murmur3("hellp", 5);
    ASSERT_TRUE(h1 != h2);
    uint32_t diff = h1 ^ h2;
    ASSERT_TRUE(diff > 0xFFFFu);  /* at least half bits flipped */
    return 0;
}

static int test_murmur3_embedded_null(void) {
    uint32_t h1 = vemb_v16_murmur3("a\0b", 3);
    uint32_t h2 = vemb_v16_murmur3("a\0c", 3);
    ASSERT_TRUE(h1 != h2);
    return 0;
}

static int test_murmur3_long_string(void) {
    char buf[256];
    memset(buf, 'x', sizeof(buf));
    uint32_t h = vemb_v16_murmur3(buf, sizeof(buf));
    ASSERT_TRUE(h != 0);
    return 0;
}

/* ===================================================================== */
/* vemb_v16_req_handle_len / vemb_v16_req_inline_len                     */
/* ===================================================================== */

static int test_req_handle_len(void) {
    size_t len = vemb_v16_req_handle_len();
    ASSERT_EQ_SIZE(len, offsetof(vemb_v16_req_t, vector));
    return 0;
}

static int test_req_inline_len_zero(void) {
    size_t len = vemb_v16_req_inline_len(0);
    ASSERT_EQ_SIZE(len, vemb_v16_req_handle_len());
    return 0;
}

static int test_req_inline_len_typical(void) {
    size_t len = vemb_v16_req_inline_len(1200);
    ASSERT_EQ_SIZE(len, vemb_v16_req_handle_len() + 1200);
    return 0;
}

static int test_req_inline_len_max(void) {
    size_t len = vemb_v16_req_inline_len(VEMB_V16_MAX_DIM * sizeof(float));
    ASSERT_EQ_SIZE(len, vemb_v16_req_handle_len() + VEMB_V16_MAX_DIM * sizeof(float));
    return 0;
}

/* ===================================================================== */
/* main                                                                  */
/* ===================================================================== */

int main(void)
{
    printf("VEMB V16 SDK Unit Tests\n");
    printf("=======================\n\n");

    RUN(combined_key_ascii);
    RUN(combined_key_with_separator_in_name);
    RUN(combined_key_empty_set);
    RUN(combined_key_empty_elem);
    RUN(combined_key_both_empty);
    RUN(combined_key_max_length);
    RUN(combined_key_exact_overflow);
    RUN(combined_key_way_too_long);

    RUN(serialize_hello_normal);
    RUN(serialize_hello_minimal);
    RUN(serialize_hello_buf_exact);
    RUN(serialize_hello_buf_too_small);
    RUN(serialize_hello_buf_zero);

    RUN(serialize_vadd_normal);
    RUN(serialize_vadd_max_dim);
    RUN(serialize_vadd_null_key);
    RUN(serialize_vadd_zero_key_len);
    RUN(serialize_vadd_key_too_long);
    RUN(serialize_vadd_null_vector);
    RUN(serialize_vadd_zero_dim);
    RUN(serialize_vadd_dim_too_large);
    RUN(serialize_vadd_buf_exact);
    RUN(serialize_vadd_buf_too_small);

    RUN(serialize_vemb_normal);
    RUN(serialize_vemb_null_key);
    RUN(serialize_vemb_zero_key_len);
    RUN(serialize_vemb_key_too_long);
    RUN(serialize_vemb_zero_dim);
    RUN(serialize_vemb_dim_too_large);
    RUN(serialize_vemb_buf_exact);
    RUN(serialize_vemb_buf_too_small);

    RUN(murmur3_empty);
    RUN(murmur3_single_char);
    RUN(murmur3_deterministic);
    RUN(murmur3_avalanche);
    RUN(murmur3_embedded_null);
    RUN(murmur3_long_string);

    RUN(req_handle_len);
    RUN(req_inline_len_zero);
    RUN(req_inline_len_typical);
    RUN(req_inline_len_max);

    printf("\n-----------------------\n");
    printf("Results: %d passed, %d failed\n", g_passed, g_failed);
    return g_failed ? 1 : 0;
}
