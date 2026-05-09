/*
 * placement_strategy.h — Compile-time HOT placement/insertion dispatch
 *
 * Orthogonal dimensions:
 *   - hash_strategy.h      : primary / probe sequence generation
 *   - eviction_strategy.h  : victim selection once local neighborhood is full
 *   - placement_strategy.h : how a key is placed into HOT
 */
#ifndef __PLACEMENT_STRATEGY_H
#define __PLACEMENT_STRATEGY_H

#include <stdint.h>
#include "three_layer_cache_ub.h"

#define PLACEMENT_BASELINE    0
#define PLACEMENT_ROBIN_HOOD  1
#define PLACEMENT_TWO_CHOICE  2
#define PLACEMENT_HOPSCOTCH   3

#ifndef PLACEMENT_STRATEGY
#define PLACEMENT_STRATEGY PLACEMENT_BASELINE
#endif

static inline const char *placement_strategy_name(void) {
#if PLACEMENT_STRATEGY == PLACEMENT_BASELINE
    return "BASELINE";
#elif PLACEMENT_STRATEGY == PLACEMENT_ROBIN_HOOD
    return "ROBIN_HOOD";
#elif PLACEMENT_STRATEGY == PLACEMENT_TWO_CHOICE
    return "TWO_CHOICE";
#elif PLACEMENT_STRATEGY == PLACEMENT_HOPSCOTCH
    return "HOPSCOTCH";
#else
    return "UNKNOWN";
#endif
}

static inline void placement_fill_primary_slots(uint64_t key, uint32_t mask, uint32_t *slots) {
    for (uint32_t i = 0; i < HASH_MAX_PROBES; i++)
        slots[i] = hash_probe(key, mask, (int)i);
}

static inline void placement_fill_alt_slots(uint64_t key, uint32_t mask, uint32_t *slots) {
    for (uint32_t i = 0; i < HASH_MAX_PROBES; i++)
        slots[i] = hash_probe_alt(key, mask, (int)i);
}

static inline int placement_probe_distance_primary(uint64_t key, uint32_t mask, uint32_t slot) {
    for (int i = 0; i < HASH_MAX_PROBES; i++) {
        if (hash_probe(key, mask, i) == slot) return i;
    }
    return HASH_MAX_PROBES;
}

static inline int placement_find_in_slots(hot_index_t *table, uint64_t key, const uint32_t *slots, int *out_idx) {
    for (int i = 0; i < HASH_MAX_PROBES; i++) {
        hot_index_t cur = table[slots[i]];
        if (cur.warm_idx < 0) break;
        if (cur.key == key) {
            if (out_idx) *out_idx = i;
            return 1;
        }
    }
    return 0;
}

static inline int placement_first_free_in_slots(hot_index_t *table, const uint32_t *slots) {
    for (int i = 0; i < HASH_MAX_PROBES; i++) {
        if (table[slots[i]].warm_idx < 0) return i;
    }
    return HASH_MAX_PROBES;
}

static inline int placement_insert_sequence(hot_index_t *table, uint64_t key, int32_t warm_idx,
                                            const uint32_t *slots) {
    for (int i = 0; i < HASH_MAX_PROBES; i++) {
        hot_index_t cur = table[slots[i]];
        if (cur.warm_idx < 0 || cur.key == key) {
            if (cur.key == key) eviction_on_put_hit(slots[i]);
            table[slots[i]] = (hot_index_t){key, warm_idx, 0};
            return i;
        }
    }
    uint32_t victim = eviction_select_victim_slots(slots, HASH_MAX_PROBES);
    table[victim] = (hot_index_t){key, warm_idx, 0};
    return HASH_MAX_PROBES;
}

static inline int placement_insert_robinhood(hot_index_t *table, uint64_t key, int32_t warm_idx, uint32_t mask) {
    uint64_t cur_key = key;
    int32_t cur_widx = warm_idx;
    int cur_dist = 0;

    for (;;) {
        if (cur_dist >= HASH_MAX_PROBES) break;
        uint32_t slot = hash_probe(cur_key, mask, cur_dist);
        hot_index_t occ = table[slot];
        if (occ.warm_idx < 0 || occ.key == cur_key) {
            if (occ.key == cur_key) eviction_on_put_hit(slot);
            table[slot] = (hot_index_t){cur_key, cur_widx, 0};
            return cur_dist;
        }

        int occ_dist = placement_probe_distance_primary(occ.key, mask, slot);
        if (occ_dist >= HASH_MAX_PROBES) break;

        if (cur_dist > occ_dist) {
            table[slot] = (hot_index_t){cur_key, cur_widx, 0};
            cur_key = occ.key;
            cur_widx = occ.warm_idx;
            cur_dist = occ_dist + 1;
            continue;
        }
        cur_dist++;
    }

    {
        uint32_t slots[HASH_MAX_PROBES];
        placement_fill_primary_slots(cur_key, mask, slots);
        uint32_t victim = eviction_select_victim_slots(slots, HASH_MAX_PROBES);
        table[victim] = (hot_index_t){cur_key, cur_widx, 0};
    }
    return HASH_MAX_PROBES;
}

static inline int placement_insert_hot(hot_index_t *table, uint64_t key, int32_t warm_idx, uint32_t mask) {
#if PLACEMENT_STRATEGY == PLACEMENT_BASELINE
    uint32_t slots[HASH_MAX_PROBES];
    placement_fill_primary_slots(key, mask, slots);
    return placement_insert_sequence(table, key, warm_idx, slots);
#elif PLACEMENT_STRATEGY == PLACEMENT_ROBIN_HOOD
    return placement_insert_robinhood(table, key, warm_idx, mask);
#elif PLACEMENT_STRATEGY == PLACEMENT_TWO_CHOICE
    uint32_t a[HASH_MAX_PROBES], b[HASH_MAX_PROBES];
    int hit_idx;
    placement_fill_primary_slots(key, mask, a);
    placement_fill_alt_slots(key, mask, b);
    if (placement_find_in_slots(table, key, a, &hit_idx)) {
        eviction_on_put_hit(a[hit_idx]);
        table[a[hit_idx]] = (hot_index_t){key, warm_idx, 0};
        return hit_idx;
    }
    if (placement_find_in_slots(table, key, b, &hit_idx)) {
        eviction_on_put_hit(b[hit_idx]);
        table[b[hit_idx]] = (hot_index_t){key, warm_idx, 0};
        return hit_idx;
    }
    {
        int free_a = placement_first_free_in_slots(table, a);
        int free_b = placement_first_free_in_slots(table, b);
        const uint32_t *chosen = (free_b < free_a) ? b : a;
        return placement_insert_sequence(table, key, warm_idx, chosen);
    }
#elif PLACEMENT_STRATEGY == PLACEMENT_HOPSCOTCH
    {
        uint32_t home = hash_primary(key, mask);

        for (int i = 0; i < HASH_MAX_PROBES; i++) {
            uint32_t s = (home + i) & mask;
            if (table[home].hop_info & (1u << i)) {
                if (table[s].key == key && table[s].warm_idx >= 0) {
                    eviction_on_put_hit(s);
                    table[s].warm_idx = warm_idx;
                    return i;
                }
            }
        }

        for (int i = 0; i < HASH_MAX_PROBES; i++) {
            uint32_t s = (home + i) & mask;
            if (table[s].warm_idx < 0) {
                table[s].key = key;
                table[s].warm_idx = warm_idx;
                table[home].hop_info |= (1u << i);
                return i;
            }
        }

        {
            uint32_t free_slot = (home + HASH_MAX_PROBES) & mask;
            int free_dist = HASH_MAX_PROBES;
            int max_range = HASH_MAX_PROBES * 16;

            while (free_dist < max_range) {
                if (table[free_slot].warm_idx < 0) break;
                free_dist++;
                free_slot = (free_slot + 1) & mask;
            }

            if (free_dist < max_range) {
                while (free_dist >= HASH_MAX_PROBES) {
                    int moved = 0;
                    for (int di = HASH_MAX_PROBES - 1; di >= 1; di--) {
                        uint32_t cand = (free_slot - di) & mask;
                        if (table[cand].warm_idx < 0) continue;

                        uint32_t cand_home = hash_primary(table[cand].key, mask);
                        uint32_t cand_off = (cand - cand_home) & mask;
                        uint32_t new_off = (free_slot - cand_home) & mask;

                        if (new_off < (uint32_t)HASH_MAX_PROBES) {
                            table[free_slot].key = table[cand].key;
                            table[free_slot].warm_idx = table[cand].warm_idx;

                            table[cand_home].hop_info &= ~(1u << cand_off);
                            table[cand_home].hop_info |= (1u << new_off);

                            table[cand].key = 0;
                            table[cand].warm_idx = -1;

                            free_slot = cand;
                            free_dist -= di;
                            moved = 1;
                            break;
                        }
                    }
                    if (!moved) break;
                }

                if (free_dist < HASH_MAX_PROBES) {
                    int offset = (int)((free_slot - home) & mask);
                    table[free_slot].key = key;
                    table[free_slot].warm_idx = warm_idx;
                    table[home].hop_info |= (1u << offset);
                    return offset;
                }
            }
        }

        {
            uint32_t slots[HASH_MAX_PROBES];
            for (int i = 0; i < HASH_MAX_PROBES; i++)
                slots[i] = (home + i) & mask;
            uint32_t victim = eviction_select_victim_slots(slots, HASH_MAX_PROBES);

            if (table[victim].warm_idx >= 0) {
                uint32_t old_home = hash_primary(table[victim].key, mask);
                uint32_t old_off = (victim - old_home) & mask;
                table[old_home].hop_info &= ~(1u << old_off);
            }

            table[victim].key = key;
            table[victim].warm_idx = warm_idx;
            uint32_t new_off = (victim - home) & mask;
            table[home].hop_info |= (1u << new_off);
            return HASH_MAX_PROBES;
        }
    }
#else
    return HASH_MAX_PROBES;
#endif
}

static inline int32_t placement_lookup_hot(hot_index_t *table, uint64_t key, uint32_t mask,
                                           int *out_probe_bucket, int *out_collided) {
    int32_t warm_idx = -1;
    int collided = 0;
#if PLACEMENT_STRATEGY == PLACEMENT_TWO_CHOICE
    uint32_t a[HASH_MAX_PROBES], b[HASH_MAX_PROBES];
    int inspected = 0;
    placement_fill_primary_slots(key, mask, a);
    placement_fill_alt_slots(key, mask, b);
    for (int round = 0; round < HASH_MAX_PROBES; round++) {
        uint32_t s0 = a[round];
        hot_index_t e0 = table[s0];
        inspected++;
        if (e0.warm_idx >= 0 && e0.key == key) {
            warm_idx = e0.warm_idx;
            eviction_on_get(s0);
            if (out_probe_bucket) *out_probe_bucket = (inspected > HASH_MAX_PROBES ? HASH_MAX_PROBES - 1 : inspected - 1);
            if (out_collided) *out_collided = collided;
            return warm_idx;
        }
        if (e0.warm_idx >= 0) collided = 1;

        uint32_t s1 = b[round];
        if (s1 != s0) {
            hot_index_t e1 = table[s1];
            inspected++;
            if (e1.warm_idx >= 0 && e1.key == key) {
                warm_idx = e1.warm_idx;
                eviction_on_get(s1);
                if (out_probe_bucket) *out_probe_bucket = (inspected > HASH_MAX_PROBES ? HASH_MAX_PROBES - 1 : inspected - 1);
                if (out_collided) *out_collided = collided;
                return warm_idx;
            }
            if (e1.warm_idx >= 0) collided = 1;
        }

        if (e0.warm_idx < 0 && table[s1].warm_idx < 0) break;
    }
    if (out_probe_bucket) *out_probe_bucket = HASH_MAX_PROBES;
    if (out_collided) *out_collided = collided;
    return -1;
#elif PLACEMENT_STRATEGY == PLACEMENT_HOPSCOTCH
    {
        uint32_t home = hash_primary(key, mask);
        uint32_t hop = table[home].hop_info;
        for (int i = 0; i < HASH_MAX_PROBES; i++) {
            if (hop & (1u << i)) {
                uint32_t s = (home + i) & mask;
                hot_index_t e = table[s];
                if (e.warm_idx >= 0 && e.key == key) {
                    warm_idx = e.warm_idx;
                    eviction_on_get(s);
                    if (out_probe_bucket) *out_probe_bucket = i;
                    if (out_collided) *out_collided = (i > 0);
                    return warm_idx;
                }
                collided = 1;
            }
        }
        if (out_probe_bucket) *out_probe_bucket = HASH_MAX_PROBES;
        if (out_collided) *out_collided = collided;
        return -1;
    }
#else
    for (int j = 0; j < HASH_MAX_PROBES; j++) {
        uint32_t s = hash_probe(key, mask, j);
        hot_index_t e = table[s];
        if (e.warm_idx >= 0 && e.key == key) {
            warm_idx = e.warm_idx;
            eviction_on_get(s);
            if (out_probe_bucket) *out_probe_bucket = j;
            if (out_collided) *out_collided = collided;
            return warm_idx;
        }
        if (e.warm_idx >= 0) collided = 1;
        if (e.warm_idx < 0) break;
    }
    if (out_probe_bucket) *out_probe_bucket = HASH_MAX_PROBES;
    if (out_collided) *out_collided = collided;
    return -1;
#endif
}

#endif /* __PLACEMENT_STRATEGY_H */
