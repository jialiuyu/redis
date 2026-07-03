```c
size_t rem = size / sizeof(float);
const float *srcp = (const float *)src;
float *dstp = (float *)dst;

svbool_t pg = svptrue_b32();

// 1. 初始化一个步长为 1 的基础索引向量: {0, 1, 2, ..., VL-1}
svuint32_t indices = svindex_u32(0, 1); 

while (rem >= vl * 4u) {
    // 保持原有的预取逻辑
    __builtin_prefetch(srcp + vl * 8u, 0, 3);

    // 2. 使用 svld1_gather_u32index_f32 平替 svld1_f32
    // 语义：从基地址 (srcp) 开始，按 indices 向量中的索引分别取值
    svfloat32_t v0 = svld1_gather_u32index_f32(pg, srcp, indices);
    svfloat32_t v1 = svld1_gather_u32index_f32(pg, srcp + vl, indices);
    svfloat32_t v2 = svld1_gather_u32index_f32(pg, srcp + vl * 2u, indices);
    svfloat32_t v3 = svld1_gather_u32index_f32(pg, srcp + vl * 3u, indices);

    // Store 逻辑保持不变（若也想测 Scatter Store 可换成 svst1_scatter）
    svst1_f32(pg, dstp, v0);
    svst1_f32(pg, dstp + vl, v1);
    svst1_f32(pg, dstp + vl * 2u, v2);
    svst1_f32(pg, dstp + vl * 3u, v3);

    srcp += vl * 4u;
    dstp += vl * 4u;
    rem -= vl * 4u;
}
```