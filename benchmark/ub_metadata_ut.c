#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../src/macro.h"
#include "../src/monotonic.h"
#include "../src/ub_metadata.h"
#include "../src/zmalloc.h"

#ifndef C_OK
#define C_OK 0
#endif
#ifndef C_ERR
#define C_ERR -1
#endif

void *zmalloc(size_t size) {
    return malloc(size ? size : 1);
}

void *zcalloc(size_t size) {
    return calloc(1, size ? size : 1);
}

void *ztrycalloc(size_t size) {
    return calloc(1, size ? size : 1);
}

void *ztrymalloc(size_t size) {
    return malloc(size ? size : 1);
}

void *zrealloc(void *ptr, size_t size) {
    return realloc(ptr, size ? size : 1);
}

void zfree(void *ptr) {
    free(ptr);
}

void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "assertion failed at %s:%d: %s\n", file, line, estr);
    abort();
}

static monotime test_get_monotonic_us(void) {
    return 0;
}

monotime (*getMonotonicUs)(void) = test_get_monotonic_us;

char *zstrdup(const char *s) {
    size_t len = strlen(s) + 1;
    char *out = zmalloc(len);
    if (!out) return NULL;
    memcpy(out, s, len);
    return out;
}

void *zmalloc_usable(size_t size, size_t *usable) {
    void *ptr = zmalloc(size);
    if (usable) *usable = size;
    return ptr;
}

void *ztrymalloc_usable(size_t size, size_t *usable) {
    void *ptr = zmalloc(size);
    if (usable) *usable = ptr ? size : 0;
    return ptr;
}

void *zrealloc_usable(void *ptr, size_t size, size_t *usable, size_t *old_usable) {
    if (old_usable) *old_usable = 0;
    void *out = zrealloc(ptr, size);
    if (usable) *usable = out ? size : 0;
    return out;
}

void zfree_usable(void *ptr, size_t *usable) {
    if (usable) *usable = 0;
    zfree(ptr);
}

int ll2string(char *s, size_t len, long long value) {
    return snprintf(s, len, "%lld", value);
}

int ull2string(char *s, size_t len, unsigned long long value) {
    return snprintf(s, len, "%llu", value);
}

static int lookup_row_cstr(ub_vector_set_meta_t *set, const char *element, uint64_t *row) {
    sds tmp = sdsnew(element);
    assert(tmp != NULL);
    int rc = ub_metadata_lookup_row(set, tmp, row);
    sdsfree(tmp);
    return rc;
}

static int remove_row_cstr(ub_vector_set_meta_t *set, const char *element, uint64_t *row) {
    sds tmp = sdsnew(element);
    assert(tmp != NULL);
    int rc = ub_metadata_remove_row(set, tmp, row);
    sdsfree(tmp);
    return rc;
}

static int alloc_row_cstr(ub_vector_set_meta_t *set, const char *element, uint64_t *row) {
    sds tmp = sdsnew(element);
    assert(tmp != NULL);
    int rc = ub_metadata_alloc_row(set, tmp, row);
    sdsfree(tmp);
    return rc;
}

static void test_dense_suffix_lookup(void) {
    assert(ub_metadata_init() == C_OK);
    ub_vector_set_meta_t *set = ub_metadata_get_or_create_set("myvectors", 300);
    assert(set != NULL);

    uint64_t row = UINT64_MAX;
    assert(ub_metadata_alloc_row(set, "item:0", &row) == C_OK && row == 0);
    assert(ub_metadata_alloc_row(set, "item:1", &row) == C_OK && row == 1);
    assert(ub_metadata_lookup_dense_suffix_row(set, "item:1", strlen("item:1"), &row) == C_OK);
    assert(row == 1);
    assert(ub_metadata_lookup_dense_suffix_row(set, "other:1", strlen("other:1"), &row) == C_ERR);
    assert(ub_metadata_lookup_dense_suffix_row(set, "item:2", strlen("item:2"), &row) == C_ERR);

    ub_metadata_cleanup();
}

static void test_dense_suffix_disabled_on_gap(void) {
    assert(ub_metadata_init() == C_OK);
    ub_vector_set_meta_t *set = ub_metadata_get_or_create_set("myvectors", 300);
    assert(set != NULL);

    uint64_t row = UINT64_MAX;
    assert(ub_metadata_alloc_row(set, "item:0", &row) == C_OK && row == 0);
    assert(ub_metadata_alloc_row(set, "item:2", &row) == C_OK && row == 1);
    assert(ub_metadata_lookup_dense_suffix_row(set, "item:1", strlen("item:1"), &row) == C_ERR);
    assert(lookup_row_cstr(set, "item:2", &row) == C_OK && row == 1);

    ub_metadata_cleanup();
}

static void test_dense_suffix_disabled_on_remove(void) {
    assert(ub_metadata_init() == C_OK);
    ub_vector_set_meta_t *set = ub_metadata_get_or_create_set("myvectors", 300);
    assert(set != NULL);

    uint64_t row = UINT64_MAX;
    assert(ub_metadata_alloc_row(set, "item:0", &row) == C_OK && row == 0);
    assert(ub_metadata_alloc_row(set, "item:1", &row) == C_OK && row == 1);
    assert(remove_row_cstr(set, "item:0", &row) == C_OK && row == 0);
    assert(ub_metadata_lookup_dense_suffix_row(set, "item:1", strlen("item:1"), &row) == C_ERR);
    assert(lookup_row_cstr(set, "item:1", &row) == C_OK && row == 1);

    ub_metadata_cleanup();
}

static void test_dense_suffix_stays_disabled_after_mutation(void) {
    assert(ub_metadata_init() == C_OK);
    ub_vector_set_meta_t *set = ub_metadata_get_or_create_set("myvectors", 300);
    assert(set != NULL);

    uint64_t row = UINT64_MAX;
    assert(alloc_row_cstr(set, "item:0", &row) == C_OK && row == 0);
    assert(alloc_row_cstr(set, "item:2", &row) == C_OK && row == 1);
    assert(alloc_row_cstr(set, "item:2", &row) == C_OK && row == 1);
    assert(alloc_row_cstr(set, "item:3", &row) == C_OK && row == 2);
    assert(ub_metadata_lookup_dense_suffix_row(set, "item:2", strlen("item:2"), &row) == C_ERR);
    assert(lookup_row_cstr(set, "item:3", &row) == C_OK && row == 2);

    ub_metadata_cleanup();
}

int main(void) {
    test_dense_suffix_lookup();
    test_dense_suffix_disabled_on_gap();
    test_dense_suffix_disabled_on_remove();
    test_dense_suffix_stays_disabled_after_mutation();
    puts("ub_metadata_ut: all tests passed");
    return 0;
}
