/* Quick v7-vs-v8 GEMM comparison on 20 cores */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <arm_sve.h>

static inline uint64_t now_ns(void){struct timespec ts;clock_gettime(CLOCK_MONOTONIC,&ts);return(uint64_t)ts.tv_sec*1000000000ULL+ts.tv_nsec;}

/* OLD v7: naive single-row SVE GEMM */
static void old_gemm(const float *A,size_t lda,const float *B,size_t ldb,float *C,size_t ldc,size_t M,size_t N,size_t K){
    size_t vl=svcntw();
    for(size_t i=0;i<M;i++)for(size_t j=0;j<N;j+=vl){
        svbool_t pg=svwhilelt_b32_u64(j,N);svfloat32_t acc=svld1_f32(pg,&C[i*ldc+j]);
        for(size_t k=0;k<K;k++)acc=svmla_f32_m(pg,acc,svdup_f32(A[i*lda+k]),svld1_f32(pg,&B[k*ldb+j]));
        svst1_f32(pg,&C[i*ldc+j],acc);
    }
}

/* NEW v8: 4-acc GEMV + L3-tiled 4-row blocked GEMM */
static void new_gemv(const float *x,const float *W,float *y,size_t K,size_t N){
    size_t vl=svcntw();
    for(size_t j=0;j<N;j+=vl){
        svbool_t pg=svwhilelt_b32_u64(j,N);
        svfloat32_t a0=svld1_f32(pg,&y[j]),a1=svdup_f32(0),a2=svdup_f32(0),a3=svdup_f32(0);
        size_t k=0;
        for(;k+3<K;k+=4){
            if(k+8<K)__builtin_prefetch(&W[(k+8)*N+j],0,3);
            a0=svmla_f32_m(pg,a0,svdup_f32(x[k+0]),svld1_f32(pg,&W[(k+0)*N+j]));
            a1=svmla_f32_m(pg,a1,svdup_f32(x[k+1]),svld1_f32(pg,&W[(k+1)*N+j]));
            a2=svmla_f32_m(pg,a2,svdup_f32(x[k+2]),svld1_f32(pg,&W[(k+2)*N+j]));
            a3=svmla_f32_m(pg,a3,svdup_f32(x[k+3]),svld1_f32(pg,&W[(k+3)*N+j]));
        }
        for(;k<K;k++)a0=svmla_f32_m(pg,a0,svdup_f32(x[k]),svld1_f32(pg,&W[k*N+j]));
        a0=svadd_f32_m(pg,a0,a1);a2=svadd_f32_m(pg,a2,a3);a0=svadd_f32_m(pg,a0,a2);
        svst1_f32(pg,&y[j],a0);
    }
}
static void new_gemm(const float *A,size_t lda,const float *B,size_t ldb,float *C,size_t ldc,size_t M,size_t N,size_t K){
    if(M<=4){for(size_t b=0;b<M;b++){memset(C+b*ldc,0,N*sizeof(float));new_gemv(A+b*lda,B,C+b*ldc,K,N);}return;}
    size_t vl=svcntw();
    for(size_t k0=0;k0<K;k0+=256){
        size_t ke=k0+256>K?K:k0+256,kl=ke-k0;size_t i=0;
        for(;i+4<=M;i+=4){
            const float *a0=A+(i+0)*lda+k0,*a1=A+(i+1)*lda+k0,*a2=A+(i+2)*lda+k0,*a3=A+(i+3)*lda+k0;
            for(size_t j=0;j<N;j+=vl){
                svbool_t pg=svwhilelt_b32_u64(j,N);
                svfloat32_t c0=k0==0?svdup_f32(0):svld1_f32(pg,&C[(i+0)*ldc+j]);
                svfloat32_t c1=k0==0?svdup_f32(0):svld1_f32(pg,&C[(i+1)*ldc+j]);
                svfloat32_t c2=k0==0?svdup_f32(0):svld1_f32(pg,&C[(i+2)*ldc+j]);
                svfloat32_t c3=k0==0?svdup_f32(0):svld1_f32(pg,&C[(i+3)*ldc+j]);
                for(size_t k=0;k<kl;k++){
                    svfloat32_t vb=svld1_f32(pg,&B[(k0+k)*ldb+j]);
                    if(k+4<kl)__builtin_prefetch(&B[(k0+k+4)*ldb+j],0,3);
                    c0=svmla_f32_m(pg,c0,svdup_f32(a0[k]),vb);c1=svmla_f32_m(pg,c1,svdup_f32(a1[k]),vb);
                    c2=svmla_f32_m(pg,c2,svdup_f32(a2[k]),vb);c3=svmla_f32_m(pg,c3,svdup_f32(a3[k]),vb);
                }
                svst1_f32(pg,&C[(i+0)*ldc+j],c0);svst1_f32(pg,&C[(i+1)*ldc+j],c1);
                svst1_f32(pg,&C[(i+2)*ldc+j],c2);svst1_f32(pg,&C[(i+3)*ldc+j],c3);
            }
        }
        for(;i<M;i++){const float *ai=A+i*lda+k0;
            for(size_t j=0;j<N;j+=vl){svbool_t pg=svwhilelt_b32_u64(j,N);
            svfloat32_t acc=k0==0?svdup_f32(0):svld1_f32(pg,&C[i*ldc+j]);
            for(size_t k=0;k<kl;k++)acc=svmla_f32_m(pg,acc,svdup_f32(ai[k]),svld1_f32(pg,&B[(k0+k)*ldb+j]));
            svst1_f32(pg,&C[i*ldc+j],acc);}
        }
    }
}

typedef void(*gemm_fn)(const float*,size_t,const float*,size_t,float*,size_t,size_t,size_t,size_t);
typedef struct{const float*A;const float*B;float*C;int M,K,N,rs,re;gemm_fn fn;}task_t;
static void *worker(void *a){task_t *t=a;int r=t->re-t->rs;if(r>0)t->fn(t->A+t->rs*t->K,t->K,t->B,t->N,t->C+t->rs*t->N,t->N,r,t->N,t->K);return NULL;}

static double run(int M,int K,int N,int nt,int iters,gemm_fn fn){
    if(nt>M)nt=M;
    float *A=calloc(M*K,sizeof(float)),*B=calloc(K*N,sizeof(float)),*C=calloc(M*N,sizeof(float));
    unsigned s=999;for(int i=0;i<M*K;i++)A[i]=((float)rand_r(&s)/RAND_MAX-0.5f)*0.1f;
    for(int i=0;i<K*N;i++)B[i]=((float)rand_r(&s)/RAND_MAX-0.5f)*0.1f;
    task_t *ts=calloc(nt,sizeof(*ts));pthread_t *pt=calloc(nt,sizeof(*pt));
    uint64_t tot=0;
    for(int it=0;it<iters;it++){memset(C,0,M*N*sizeof(float));int rp=(M+nt-1)/nt;uint64_t st=now_ns();
    for(int i=0;i<nt;i++){ts[i]=(task_t){A,B,C,M,K,N,i*rp,(i+1)*rp>M?M:(i+1)*rp,fn};pthread_create(&pt[i],NULL,worker,&ts[i]);}
    for(int i=0;i<nt;i++)pthread_join(pt[i],NULL);tot+=now_ns()-st;}
    double avg=(double)tot/iters/1e9;free(A);free(B);free(C);free(ts);free(pt);return 2.0*M*K*N/avg/1e9;
}

int main(){
    int cases[][3]={{1,4096,11008},{2,4096,11008},{4,4096,11008},{256,1024,256},{512,2048,512},{1024,4096,1024}};
    const char *labels[]={"ColdExpert B=1 (4K×11K)","ColdExpert B=2 (4K×11K)","ColdExpert B=4 (4K×11K)",
                          "Small (256×1K×256)","Medium (512×2K×512)","Large (1K×4K×1K)"};
    printf("\n%-30s %12s %12s %10s\n","Case","v7(old) GF/s","v8(new) GF/s","Speedup");
    printf("%-30s %12s %12s %10s\n","------------------------------","------------","------------","----------");
    for(int c=0;c<6;c++){
        int M=cases[c][0],K=cases[c][1],N=cases[c][2],iters=M<=4?30:5;
        double old_gf=run(M,K,N,20,iters,old_gemm);
        double new_gf=run(M,K,N,20,iters,new_gemm);
        printf("%-30s %12.2f %12.2f %9.1fx\n",labels[c],old_gf,new_gf,new_gf/old_gf);
    }
    printf("\n");return 0;
}
