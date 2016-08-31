/**
 * types.h
 * define all types
 *
 * copyright axon LTD.
 */
#ifndef __TYPES_H__
#define __TYPES_H__

#include <stdio.h>
#include <stddef.h>
#include <inttypes.h>

typedef unsigned long long u64;
typedef signed long long   s64;
typedef unsigned int       u32;
typedef signed int         s32;
typedef unsigned short     u16;
typedef signed short       s16;
typedef unsigned char      u8;
typedef signed char        s8;

typedef u8   __u8;
typedef u16  __u16;
typedef u32  __u32;
typedef u64  __u64;

typedef u16   __be16;
typedef u32   __be32;
typedef u16   __sum16;

typedef  unsigned long  ulong;
typedef  unsigned char  uchar;
typedef  unsigned int   uint;
typedef  unsigned short ushort;


typedef volatile int32_t atomic_t;
typedef volatile int64_t atomic64_t;

#define rte_atomic32_t atomic_t
#define rte_atomic64_t atomic64_t

#define atomic_set(k, v)       (k = v)
#define atomic_inc(k)          (k += 1)
#define atomic_dec_and_test(k) (-- k == 0)
#define atomic_dec(k)          (k --)
#define atomic_read(k)         (k)

#define rte_atomic64_inc(ptr)  ((*ptr)+=1)
#define rte_atomic64_dec(ptr)  ((*ptr)--)

// 多线程时的原子操作
#define sync_atomic_inc(ptr)        do { __sync_add_and_fetch(ptr, 1); } while(0)
#define sync_atomic_dec(ptr)        do { __sync_sub_and_fetch(ptr, 1); } while(0)
#define sync_atomic_read(ptr, val)  do { val = *ptr; } while(0)
#define sync_atomic_set(ptr, val)   do { *ptr = val; } while(0)
#define sync_atomic_init(ptr)       do { *ptr = 0; } while(0)

#ifndef ALIGN
#define ALIGN(x, a)    (((x) + (a) - 1) & ~((a) - 1))
#endif

#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

#ifndef container_of
/**
 * container_of - cast a member of a structure out to the containing structure
 * @ptr:    the pointer to the member.
 * @type:   the type of the container struct this is embedded in.
 * @member: the name of the member within the struct.
 *
 */
#define container_of(ptr, type, member) ({            \
    const typeof(((type *)0)->member) * __mptr = (ptr);    \
    (type *)((char *)(unsigned long)__mptr - offsetof(type, member)); })
#endif


#define _ADDR0( val )   ((u8)((u32)(val)&(0xff)))
#define _ADDR1( val )   ((u8)((u32)(val)>>8&(0xff)))
#define _ADDR2( val )   ((u8)((u32)(val)>>16&(0xff)))
#define _ADDR3( val )   ((u8)((u32)(val)>>24&(0xff)))


#define MK_ADDR(v1,v2,v3,v4)  ( ((v1) |(v2)<<8 | (v3)<<16 | (v4)<<24 ))
#define _ADDR(val) \
        _ADDR0( val ),_ADDR1( val ),_ADDR2( val ),_ADDR3( val )

#define _HOST_ADDR(val) \
			_ADDR3( val ),_ADDR2( val ),_ADDR1( val ),_ADDR0( val )

#define AX_ASSERT(expr)                                   \
 do {                                                     \
  if (!(expr)) {                                          \
    fprintf(stderr,                                       \
            "Assertion failed in %s on line %d: %s\n",    \
            __FILE__,                                     \
            __LINE__,                                     \
            #expr);                                       \
    abort();                                              \
  }                                                       \
 } while (0)



#endif  /* __TYPES_H__ */



