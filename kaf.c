/*
 *	kafka producer, base on libuv, focus on performance, simple
 *
 *	feature:
 *		metadata, producer
 *
 *	gcc -o kaf kaf.c -luv -g
 *
 *	kafka协议中文翻译: http://watchword.space/blog/?p=39
 *
 */
#define _GNU_SOURCE 
#include <uv.h>
#include <zlib.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <sys/epoll.h>
#include <errno.h>
#include <unistd.h>
#include <endian.h>
#include <stdint.h>

#include "types.h"
#include "list.h"

#define MSG_FIELD_LEN        4
#define MAX_KAFKA_BROKERS    8
#define MAX_BROKER_ADDR_LEN  64

#define KAFKA_BROKER_RECONNECT_SECOND  15  // kafka broker 失败重连间隔
#define MAX_BROKERS_HASH_NODES         17
#define BROKER_REQUEST_TIMEOUT         10   // kafka broker request timeout

// broker status
#define BROKER_STATUS_INIT        0
#define BROKER_STATUS_CONNECTING  1
#define BROKER_STATUS_CONNECTED   2

// api key defines from rdkafka
#define RD_KAFKAP_Produce          0
#define RD_KAFKAP_Fetch            1
#define RD_KAFKAP_Offset           2
#define RD_KAFKAP_Metadata         3
// Non-user facing control APIs	   4-7
#define RD_KAFKAP_OffsetCommit     8
#define RD_KAFKAP_OffsetFetch      9
#define RD_KAFKAP_GroupCoordinator 10
#define RD_KAFKAP_JoinGroup        11
#define RD_KAFKAP_Heartbeat        12
#define RD_KAFKAP_LeaveGroup       13
#define RD_KAFKAP_SyncGroup        14
#define RD_KAFKAP_DescribeGroups   15
#define RD_KAFKAP_ListGroups       16
#define RD_KAFKAP_SaslHandshake    17
#define RD_KAFKAP_ApiVersion       18
#define RD_KAFKAP__NUM             19

// log
#ifndef MOD_SYS
#define MOD_SYS  0
#endif

#ifndef LOG_ERR
#define LOG_ERR      4
#endif

#ifndef LOG_INFO
#define LOG_INFO     6
#endif

#define AP_MALLOC(size, name)  malloc(size)
#define AP_FREE(ptr, name)     free(ptr)

#define KAF_LOG(level, mod, fmt, ...) do { (void)level; (void)mod; printf(fmt, ##__VA_ARGS__); } while(0)

// string
typedef struct {
	size_t len;
	char   *s;
} ax_string_t;

typedef struct {
	size_t   len;
	char    *base;
} ax_buf_t;

// 数组
// 数组的元素通过build_element 函数创建
typedef struct {
	int  items;
	int  complex;  // 数组的元素是否是复合元素, int bool float等元素是非复合元素
	int  (*create_element)(void **, char *);   // 分析创建元素的函数, 元素是动态创建的
	void (*destory_element)(void *);           // 释放、销毁元素的函数
	void (*print_element)(void *, const char *, int fd);
	void **elements;
} ax_array_t;

// 接收response缓存区
// kafka通信协议规定，第一个int是消息长度
struct kafkap_req_buf_s;
struct recv_buf_s {
#define BROKER_RECV_STATE_INIT    0
#define BROKER_RECV_STATE_HEAD    1
#define BROKER_RECV_STATE_BODY    2
#define BROKER_RECV_STATE_FINISH  3
	int state;     // 状态
	int recv_len;  // 已接受消息总长度
	int total;     // 消息总长度
	int corr_id;
	struct kafkap_req_buf_s *req;

	union {
		int  ilen;     // 网络字节序
		char slen[4];
	};
	char *buffer;
};

typedef struct recv_buf_s recv_buf_t;

struct ax_kafka_s;
// kafka brokers
struct ax_broker_s {
    uv_tcp_t      tcp;
    uv_timer_t    timer;  // connect timer
    uv_connect_t  connect_req;

    ax_string_t     *saddr;
    struct in_addr  addr;
    unsigned short  port;

    int status;
    recv_buf_t recv;

    // pointer to kafka server
    struct ax_kafka_s *kafka;

    // 已经发出, 还没收到响应的请求列表
    struct list_head  requests[MAX_BROKERS_HASH_NODES];
};

typedef struct ax_broker_s ax_broker_t;
struct ax_kafka_s;

typedef struct {
	ax_string_t *topicname;
	ax_broker_t *brk;
	struct ax_kafka_s  *kaf;
} ax_kaf_topic_t;

// kafka server 定义
struct ax_kafka_s {
	uv_loop_t     loop;
	int           brkrs_cnt;   // 有多少个broker
	ax_string_t  *clientid;    // client标示
	int64_t       corr_id;     // 全局递增

	struct ax_broker_s  brkrs[MAX_KAFKA_BROKERS];
};

typedef struct ax_kafka_s ax_kafka_t;

/**
 * Request types
 * 请求通用头部
 */
struct rd_kafkap_reqhdr {
	int32_t  size;
	int16_t  api_key;
	int16_t  version;
	int32_t  corr_id;
	/* ClientId follows */
} __attribute__((packed));

// request请求
struct kafkap_req_buf_s {
	struct list_head list;
	int    req_typ;

    uv_write_t  req;
    uv_buf_t    buf;
    uv_timer_t  tmo;  // 超时
    ax_broker_t *brk;
    void *data;        // 必要的信息, 例如ax_kaf_topic_t

	int offset;
	int length;
	int corr_id;

	char *buffer;
};

typedef struct kafkap_req_buf_s kafkap_req_buf_t;
/*
 * Response
 * Response 头部
 */
struct ax_kafka_resp_hdr_s {
	int32_t size;
	int32_t corr_id;
} __attribute__((packed));

typedef struct ax_kafka_resp_hdr_s ax_kafka_resp_hdr_t;
/*
	MetadataResponse => [Broker][TopicMetadata]
	  Broker => NodeId Host Port  (any number of brokers may be returned)
	    NodeId => int32
	    Host => string
	    Port => int32
	  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
	    TopicErrorCode => int16
	  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
	    PartitionErrorCode => int16
	    PartitionId => int32
	    Leader => int32
	    Replicas => [int32]
	    Isr => [int32]
*/
// broker
typedef struct {
	int32_t      nodeid;
	ax_string_t  *host;
	int32_t      port;
} resp_broker_t;

typedef struct {
	int16_t errorcode;
	int32_t part_id;
	int32_t leader;
	ax_array_t  *replicas;
	int32_t     isr;
} partion_metadata_t;

typedef struct {
	int16_t      errorcode;
	ax_string_t  *topicname;
	ax_array_t   *partitions;
} resp_topic_metadata_t;

// response metadata
typedef struct {
	ax_array_t *brokers;
	ax_array_t *topic_metadatas;
} resp_meta_t;

// [Partition ErrorCode Offset]
typedef struct {
	int32_t partition;
	int16_t errorcode;
	int64_t offset;
} resp_msg_ack_t;

typedef struct {
	ax_string_t *topicname;
	ax_array_t  *acks;
} resp_prod_topic_ack_t;

// response of produce
// v0
// ProduceResponse => [TopicName [Partition ErrorCode Offset]]
//   TopicName => string
//   Partition => int32
//   ErrorCode => int16
//   Offset => int64
typedef struct {
	ax_array_t *topics;
} resp_produce_t;

//
// message
// message header
struct ax_kafka_msg_hdr_s {
	int32_t crc;
	int8_t  magic;
	int8_t  attr;
}  __attribute__((packed));

typedef struct ax_kafka_msg_hdr_s ax_kafka_msg_hdr_t;

struct ax_message_set_s {
	int64_t offset;
	int32_t size;
	// message_fmt_t
}  __attribute__((packed));


typedef struct {
	ax_string_t *topic;
	int  compress;
	int  howmany_bufs;
	int  timeout;
	int  partition;
	int  index;            // 标示当前的bufs数组下标
	ax_buf_t *bufs;
} ax_produce_topic_msg_t;

typedef struct ax_message_set_s ax_message_set_t;

#define PUSH_BUFF_OFFSET(buf, s)            do { (buf)->offset += s; } while(0)  // 增加buffer的offset偏移
#define PUSH_BUFF_OFFSET_INT8(buf)          PUSH_BUFF_OFFSET(buf, sizeof(int8_t))
#define PUSH_BUFF_OFFSET_INT16(buf)         PUSH_BUFF_OFFSET(buf, sizeof(int16_t))
#define PUSH_BUFF_OFFSET_INT32(buf)         PUSH_BUFF_OFFSET(buf, sizeof(int32_t))
#define PUSH_BUFF_OFFSET_INT64(buf)         PUSH_BUFF_OFFSET(buf, sizeof(int64_t))

#define PUSH_BUFF_OFFSET_AX_STRING(buf, as) do { (buf)->offset += as->len + 2; } while(0)
#define PUSH_BUFF_OFFSET_AX_BYTES(buf, as)  do { (buf)->offset += as->len + 4; } while(0)

#define WRITE_BUFFER_INT8(ptr, i)        do { *(int8_t *)ptr = i; ptr += 1; } while (0)
#define WRITE_BUFFER_INT16(ptr, i)       do { *(int16_t *)ptr = htons(i); ptr += 2; } while (0)
#define WRITE_BUFFER_INT32(ptr, i)       do { *(int32_t *)(ptr) = htonl(i); ptr += 4; } while (0)
#define WRITE_BUFFER_INT64(ptr, i)       do { *(int64_t *)(ptr) = htobe64(i); ptr += 8; } while (0)

#define WRITE_BUFFER_AX_STRING(ptr, as)  do {                                           \
												*(int16_t *)ptr = htons(as->len);       \
												ptr += 2; strncpy(ptr, as->s, as->len); \
												ptr += as->len;                         \
											} while(0)
#define WRITE_BUFFER_AX_BYTES(ptr, as)   do {                                           \
												if (as) {                               \
													*(int32_t *)ptr = htonl((as)->len); \
											  		ptr += 4;                           \
											  		memcpy(ptr, (as)->base, (as)->len); \
											  		ptr += (as)->len;                   \
											  	} else {                                \
											  		*(int32_t *)ptr = 0xffffffff;       \
											  		ptr += 4;                           \
											  	}                                       \
											} while(0)

// 将buffer message 长度写入buffer中
#define WRITE_BUFFER_SIZE(buf)       *(int32_t *)((buf)->buffer) = htonl((buf)->offset - 4);
// 先分析接收长度
#define GET_RESPONSE_LEN(rbuf)        do { (rbuf)->total = htonl((rbuf)->ilen) + MSG_FIELD_LEN; } while(0)
// 发送缓冲的指针地址
#define GET_BUFFER_LATEST_PTR(buf)   ((buf)->buffer + (buf)->offset)
// 设置发送缓冲的corr id
#define SET_RESPONSE_CORR_ID(rbuf)   do { rbuf->corr_id = ntohl(*(int *)(rbuf->buffer + 4)); } while(0)

// kafka request 头部长度 :  request header + clientid
#define KAFKA_REQUEST_HEADER_LEN(client)  (sizeof(struct rd_kafkap_reqhdr) + 2 + (client)->len)
// 计算request buffer的总长度, 参数len未不包括request公共头部(头部和client_id)的长度
#define KAFKA_REQUEST_LEN(kaf, length)    (sizeof(struct kafkap_req_buf_s) +   \
											sizeof(struct rd_kafkap_reqhdr) +  \
											2 + (kaf)->clientid->len + length)

// message set 长度 [offset(8) + size(4) + crc[4] + magicbyte(1) + attributes(1) + key + value]
// 26 + keylen + vallen
#define KAFKA_MESSAGE_SET_LEN(keylen, vallen)    (8 + 4 + 4 + 1 + 1 + (4 + keylen) + (4 + vallen))
// 单条produce 消息长度 requestack(2) + timeout(4) + [topicname [partition(4) + messagesize(4) + messageset(n)]]
// n为messageset的长度
#define KAFKA_PRODUCE_REQUEST_LEN(tn, n)  (2 + 4 + 4 + ((tn)->len + 2 + 4 + (4 + 4 + n)))  // = 24 + tn->len + n

// 上面的逆函数, 发送数据的长度, offset + 4
#define KAFKA_REQUEST_BUFF_LEN(s)    ((s) - sizeof(struct kafkap_req_buf_s))
// kafka request hash算法
#define KAFKA_REQUEST_HASH(id, sz)   ((id) % (sz))

#define SET_RESPONSE_INT64(ptr, val, len)        	do { val = be64toh(*(int64_t *)(ptr)); ptr += 8; len += 8; } while(0)
#define SET_RESPONSE_INT32(ptr, val, len)        	do { val = ntohl(*(int32_t *)(ptr)); ptr += 4; len += 4; } while(0)
#define SET_RESPONSE_INT16(ptr, val, len)        	do { val = ntohs(*(int16_t *)(ptr)); ptr += 2; len += 2; } while(0)
#define SET_RESPONSE_STRING(ptr, as, slen, len)  	do {                                            \
														slen = ntohs(*(int16_t *)(ptr));            \
														ptr += 2;                                   \
														as = mk_ax_string_by_len(ptr, slen);        \
														ptr += slen;                                \
														len += 2 + (slen);                          \
												 	} while(0)

//#define
#define PRINT_INT16(prefix, name, val)          KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %hd\n", prefix, name, val)
#define PRINT_INT32(prefix, name, val)          KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %d\n", prefix, name, val)
#define PRINT_ARRAY_INT32(prefix, name, val)    KAF_LOG(LOG_INFO, MOD_SYS, "%s[ %s ]: %d\n", prefix, name, val)
#define PRINT_INT64(prefix, name, val)          KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %Ld\n", prefix, name, val)

#define PRINT_BE_INT16(prefix, name, val)       KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %hd\n", prefix, name, ntohs(val))
#define PRINT_BE_INT32(prefix, name, val)       KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %d\n", prefix, name, ntohl(val))

#define PRINT_BE_INT8_PTR(prefix, name, ptr)    do {                                                               \
													KAF_LOG(LOG_INFO, MOD_SYS,                                     \
														"%s%s: %d\n", prefix, name, (int)(*(char *)(ptr)));        \
													ptr ++;                                                        \
												} while(0)

#define PRINT_BE_INT16_PTR(prefix, name, ptr)   do {                                                               \
													KAF_LOG(LOG_INFO, MOD_SYS,                                     \
														"%s%s: %hd\n", prefix, name, ntohs(*(int16_t *)(ptr)));    \
													ptr += 2;                                                      \
												} while(0)

#define PRINT_BE_INT32_PTR(prefix, name, ptr)   do {                                                               \
													KAF_LOG(LOG_INFO, MOD_SYS,                                     \
														"%s%s: %d\n", prefix, name, ntohl(*(int32_t *)(ptr)));     \
													ptr += 4;                                                      \
												} while(0)

#define PRINT_BE_INT64_PTR(prefix, name, ptr)   do {                                                               \
													KAF_LOG(LOG_INFO, MOD_SYS,                                     \
														"%s%s: %Ld\n", prefix, name, be64toh(*(int64_t *)(ptr)));  \
													ptr += 8;                                                      \
												} while(0)

//
#define PRINT_LF                              KAF_LOG(LOG_INFO, MOD_SYS, "\n")
#define PRINT_NONAME_STRING(prefix, s)        KAF_LOG(LOG_INFO, MOD_SYS, "%s%s\n", prefix, s)
#define PRINT_STRING(prefix, name, s)         KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %s\n", prefix, name, s)
#define PRINT_AX_STRING(prefix, name, as)     KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: %s\n", prefix, name, (as->s))

static ax_kafka_t kaf_server;

void connect_kafka_broker_tmr(uv_timer_t* data);
int broker_meta_req(ax_broker_t *brk);
void free_req_buffer(struct kafkap_req_buf_s* buf);

// 打印response
static void print_kafka_resp(void *resp) {
	ax_kafka_resp_hdr_t *hdr = (ax_kafka_resp_hdr_t *)resp;

	KAF_LOG(LOG_INFO, MOD_SYS, "size: %d  corr_id: %d\n", ntohl(hdr->size), ntohl(hdr->corr_id));
}

static void hexdump(char *data, int len) {
	char *ptr = data;

	while(len > 0) {
		int i;

		printf("0x%x ", ptr);
		for (i = 0; i < 8 && i < len; i ++) {
			printf(" %02x ", *(uint8_t *)ptr);
			ptr ++;
		}
		printf("\n");

		len -= 8;
	}
	printf("\n");
}

///////////////////////////////////////hash table operations/////////////////////////////////////////
// 插入hash表
static int insert_to_req_hash(ax_broker_t *brk, kafkap_req_buf_t * req) {
	int key = KAFKA_REQUEST_HASH(req->corr_id, MAX_BROKERS_HASH_NODES);
	struct list_head *head;

	head = &brk->requests[key];
	list_add_tail(&req->list, head);

	return 0;
}

//
// 查找(并删除)hash表
// remove: 1 查找并从hash表中删除
//         0 仅查找, 不删除
static struct kafkap_req_buf_s * find_req_from_hash(ax_broker_t *brk, int corr_id, int remove) {
	int key = KAFKA_REQUEST_HASH(corr_id, MAX_BROKERS_HASH_NODES);
	struct list_head *head = &brk->requests[key];
	struct kafkap_req_buf_s *req, *n;

	list_for_each_entry_safe(req, n, head, list) {
		if (req->corr_id == corr_id) {
			if (remove)
				list_del(&req->list);
			return req;
		}
	}

	return NULL;
}

static void write_crc(char *crc_ptr, const char *data, int len) {
	unsigned long crc;

	crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, data, len);

	WRITE_BUFFER_INT32(crc_ptr, crc);
	return;
}

static ax_string_t *ax_append_string(const char *a, const char *b) {
	int total;
	ax_string_t *str;

	total = strlen(a) + strlen(b);
	str = AP_MALLOC(ALIGN(total + sizeof(ax_string_t) + 1, 16), "");
	if (!str) return NULL;

	str->len = total;
	str->s = (char *)str + sizeof(ax_string_t);
	strncpy(str->s, a, strlen(a));
	strncpy(str->s + strlen(a), b, strlen(b));
	*(str->s + total) = 0;

	return str;
}

//
// 分配空间，构建ax_string_t
static ax_string_t *mk_ax_string_by_len(const char *s, int len) {
	ax_string_t *str;

	str = AP_MALLOC(ALIGN(len + sizeof(ax_string_t) + 1, 16), "");
	if (!str) return NULL;

	str->len = len;
	str->s = (char *)str + sizeof(ax_string_t);
	strncpy(str->s, s, len);
	*(str->s + len) = 0;

	return str;
}

//
// 根据传入参数s，分配一个ax_string_t的结构体, 并返回
static ax_string_t * mk_ax_string(const char *s) {
	return mk_ax_string_by_len(s, strlen(s));
}

static void free_ax_string(ax_string_t * as) {
	free(as);
}

static int print_buffer_as_string(const char *prefix, const char *name, char *ptr) {
	int i;
	ax_string_t *as;

	i = ntohl(*(int32_t *)ptr);
	if (i == -1) {
		KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: empty\n", prefix, name);
		return 4;
	}

	as = mk_ax_string_by_len((const char *)(ptr + 4), i);
	KAF_LOG(LOG_INFO, MOD_SYS, "%s%s: len=%d value=%s\n", prefix, name, i, as->s);
	free_ax_string(as);

	return 4 + i;
}

// 解析响应的 ax_string_t
static ax_string_t * parse_resp_ax_string(char *ptr) {
	int len = ntohs(*(int16_t *)ptr);

	return mk_ax_string_by_len(ptr + 2, len);
}

// 一次分配, 同时分配 ax_array_t 的空间和elements指针的空间
static ax_array_t *alloc_ax_array(int howmany, size_t es) {
	ax_array_t *v;

	v = AP_MALLOC(sizeof(ax_array_t) + es * howmany, "");
	if (!v) return NULL;

	// 初始化
	v->items    = howmany;
	v->elements = (void **)((char *)v + sizeof(ax_array_t));

	return v;
};

// 分析并生成 int32 数组
static ax_array_t *parse_resp_ax_array_int32(char *ptr, int *ret_len) {
	int len = 0, i, ret;
	int items = ntohl(*(int *)ptr);
	ax_array_t *v;
	int32_t *pi;

	len += 4;
	ptr += 4;

	v = alloc_ax_array(items, sizeof(int32_t));
	if (!v) {
		*ret_len = -1;
		return NULL;
	}

	v->complex = 0;
	v->create_element  = NULL;
	v->destory_element = NULL;
	v->print_element   = NULL;
	pi = (int32_t *)v->elements;
	for (i = 0; i < items; i ++) {
		SET_RESPONSE_INT32(ptr, *pi, len);
		pi ++;
	}

	*ret_len = len;
	return v;
}

static void print_ax_array_int32(ax_array_t *v, const char *prefix, const char *name) {
	int i;
	int *pi = (int *)v->elements;

	KAF_LOG(LOG_INFO, MOD_SYS, "%s%s [");
	for (i = 0; i< v->items; i ++) {
		KAF_LOG(LOG_INFO, MOD_SYS, "%d ", pi[i]);
	}
	KAF_LOG(LOG_INFO, MOD_SYS, "]\n");
}

// 释放 int32 数组
void destroy_ax_array_int32(ax_array_t * ai) {
	AP_FREE(ai, "array_int32");
}

/*
 *	@ptr:             array 起始地址
 *  @create_element:  分析创建element, 函数需要将分配的element写入传入参数中
 *  @free_ele:        销毁、释放element
 *	@ret_len:         response中数组的字节数, 返回给调用者使用
 *
 *	@return:          数组指针
 */
static ax_array_t * parse_resp_ax_array(char *ptr,
	int (*create_element)(void **, char *ptr),
	void (*destroy_element)(void *),
	void (*print_element)(void *, const char *, int),
	int *ret_len) {

	int i, len = 0, ret;
	int items = ntohl(*(int *)ptr);
	ax_array_t *v;

	len += 4;
	ptr += 4;

	v = alloc_ax_array(items, sizeof(void *));
	if (!v) {
		*ret_len = -1;
		return NULL;
	}

	// 复合元素
	v->complex = 1;
	v->create_element  =  create_element;
	v->destory_element = destroy_element;
	v->print_element   = print_element;

	for (i = 0; i < items; i ++) {
		v->elements[i] = NULL;
		ret = create_element(&v->elements[i], ptr);
		if (ret < 0) {
			// 失败处理
			for(; i >= 0; i --) {
				destroy_element(v->elements[i]);
			}
			AX_ASSERT(0);
		}
		ptr += ret;
		len += ret;
	}

	// 分析完毕
	*ret_len = len;
	return v;
}

void destroy_ax_array(ax_array_t * v) {
	int i;

	for (i = 0; i < v->items; i ++) {
		v->destory_element(v->elements[i]);
	}
}

void print_ax_array(ax_array_t *v, const char *prefix, const char *name) {
	int i;
	ax_string_t *nprefix = ax_append_string(prefix, "  ");

	PRINT_ARRAY_INT32(prefix, name, v->items);
	for (i = 0; i < v->items; i ++) {
		v->print_element(v->elements[i], (const char *)nprefix->s, 0);
		PRINT_LF;
	}

	AP_FREE(nprefix, "ax_string");
}

/*
typedef struct {
	int32_t      nodeid;
	ax_string_t  *host;
	int32_t      port;
} resp_broker_t;
*/
static int create_response_metadata_broker(void **ele, char *ptr) {
	int len = 0;
	int16_t hostlen;
	resp_broker_t *rbrk;

	rbrk = AP_MALLOC(sizeof(*rbrk), "resp_broker_t");
	if (!rbrk) return -1;

	SET_RESPONSE_INT32(ptr, rbrk->nodeid, len);

	SET_RESPONSE_STRING(ptr, rbrk->host, hostlen, len);
	/*
	SET_RESPONSE_INT16(ptr, hostlen, len);
	rbrk->host = mk_ax_string_by_len(ptr, hostlen);
	ptr += hostlen;
	*/

	SET_RESPONSE_INT32(ptr, rbrk->port, len);

	*ele = rbrk;
	return len;
}

static void destroy_response_metadata_broker(void *ele) {
	resp_broker_t *rbrk = (resp_broker_t *)ele;

	AP_FREE(rbrk->host, "host");
	AP_FREE(rbrk, "resp_broker_t");
}

static void print_response_metadata_broker(void *ele, const char* prefix, int fd) {
	resp_broker_t *rbrk = (resp_broker_t *)ele;

	PRINT_INT32(prefix, "NodeId", rbrk->nodeid);
	PRINT_AX_STRING(prefix, "host", rbrk->host);
	PRINT_INT32(prefix, "port", rbrk->port);
}

/*
typedef struct {
	int16_t errorcode;
	int32_t part_id;
	int32_t leader;
	ax_array_t  *replicas;
	int32_t     isr;
} partion_metadata_t;
*/
static int create_partition_metadata(void **ele, char *ptr) {
	int len;
	int alen = 0;
	partion_metadata_t *rpart;

	rpart = AP_MALLOC(sizeof(*rpart), "");
	AX_ASSERT(rpart != NULL);

	SET_RESPONSE_INT16(ptr, rpart->errorcode, len);
	SET_RESPONSE_INT32(ptr, rpart->part_id,   len);
	SET_RESPONSE_INT32(ptr, rpart->leader,    len);

	rpart->replicas = parse_resp_ax_array_int32(ptr, &alen);
	len += alen;
	ptr += alen;

	SET_RESPONSE_INT32(ptr, rpart->isr, len);
	*ele = rpart;

	return len;
}

static void destroy_partition_metadata(void *ele) {
	partion_metadata_t *rpart = (partion_metadata_t *)ele;

	destroy_ax_array_int32(rpart->replicas);
	AP_FREE(rpart, "partion_metadata_t");
}

static void print_partition_metadata(void *ele, const char* prefix, int fd) {
	partion_metadata_t *rpart = (partion_metadata_t *)ele;

	PRINT_INT16(prefix, "error code", rpart->errorcode);
	PRINT_INT32(prefix, "part id", rpart->part_id);
	PRINT_INT32(prefix, "leader", rpart->leader);
	print_ax_array_int32(rpart->replicas, prefix, "replicas:");
	PRINT_INT32(prefix, "isr", rpart->isr);
	PRINT_LF;
}

/*
typedef struct {
	int16_t      errorcode;
	ax_string_t  *topicname;
	ax_array_t   *partitions;
} resp_topic_metadata_t;
*/
static int create_response_metadata_topic_metadata(void **ele, char *ptr) {
	int len = 0;
	int16_t tnlen; // topicname
	resp_topic_metadata_t *rtopic;
	int part_len;

	rtopic = AP_MALLOC(sizeof(*rtopic), "resp_topic_metadata_t");
	AX_ASSERT(rtopic != NULL);

	SET_RESPONSE_INT16(ptr, rtopic->errorcode, len);
	SET_RESPONSE_STRING(ptr, rtopic->topicname, tnlen, len);

	rtopic->partitions = parse_resp_ax_array(ptr,
		create_partition_metadata,
		destroy_partition_metadata,
		print_partition_metadata,
		&part_len);

	*ele = rtopic;
	return len + part_len;
}

static void destroy_response_metadata_topic_metadata(void *ele) {
	resp_topic_metadata_t *rtopic = (resp_topic_metadata_t *)ele;

	AP_FREE(rtopic->topicname, "topicname");
	destroy_ax_array(rtopic->partitions); // ->destory_element();
	AP_FREE(rtopic, "rtopic");
}

static void print_response_metadata_topic_metadata(void *ele, const char* prefix, int fd) {
	ax_string_t *nprefix = ax_append_string(prefix, "  ");
	resp_topic_metadata_t *rtopic = (resp_topic_metadata_t *)ele;

	PRINT_INT32(prefix, "error code", rtopic->errorcode);
	PRINT_AX_STRING(prefix, "topic name", rtopic->topicname);
	print_ax_array(rtopic->partitions, nprefix->s, "partitions");

	AP_FREE(nprefix, "ax_string");
}

//
// 释放 resp_meta_t
static void destory_resp_metadata(resp_meta_t *rmeta, int free_self) {
	destroy_ax_array(rmeta->brokers);
	destroy_ax_array(rmeta->topic_metadatas);

	if (free_self)
		AP_FREE(rmeta, "rmeta");
}

/**
 * 分析 metadata 的响应
 */
static resp_meta_t * prase_response_metadata(ax_broker_t *brk, kafkap_req_buf_t *req) {
	recv_buf_t *recv = &brk->recv;
	resp_meta_t *rmeta;
	int ret;

	rmeta = AP_MALLOC(sizeof(*rmeta), "rmeta");
	if (!rmeta)
		return NULL;

	rmeta->brokers = parse_resp_ax_array(recv->buffer + sizeof(ax_kafka_resp_hdr_t),
		create_response_metadata_broker,
		destroy_response_metadata_broker,
		print_response_metadata_broker,
		&ret);
	rmeta->topic_metadatas = parse_resp_ax_array(recv->buffer + sizeof(ax_kafka_resp_hdr_t) + ret,
		create_response_metadata_topic_metadata, 
		destroy_response_metadata_topic_metadata,
		print_response_metadata_topic_metadata,
		&ret);

	return rmeta;
}

/////////////////////////////////////////////////////////////////////////////////
/// parse produce response
static int create_resp_prod_ack(void **ele, char *ptr) {
	int32_t len = 0; // topicname
	resp_msg_ack_t *rtopic;

	rtopic = AP_MALLOC(sizeof(*rtopic), "resp_msg_ack_t");
	AX_ASSERT(rtopic != NULL);

	SET_RESPONSE_INT32(ptr, rtopic->partition, len);
	SET_RESPONSE_INT16(ptr, rtopic->errorcode, len);
	SET_RESPONSE_INT64(ptr, rtopic->offset, len);

	*ele = rtopic;
	return len;
}

static void destroy_resp_prod_ack(void *ele) {
	resp_msg_ack_t *rtopic = (resp_msg_ack_t *)ele;

	AP_FREE(rtopic, "rtopic");
}

static void print_resp_prod_ack(void *ele, const char* prefix, int fd) {
	ax_string_t *nprefix = ax_append_string(prefix, "  ");
	resp_msg_ack_t *rtopic = (resp_msg_ack_t *)ele;

	PRINT_INT32(nprefix->s, "partition", rtopic->partition);
	PRINT_INT16(nprefix->s, "error code", rtopic->errorcode);
	PRINT_INT64(nprefix->s, "offset", rtopic->offset);

	AP_FREE(nprefix, "ax_string");
}

////////////////////////////////////////////////////////////////////////////////
static int create_response_topic_ack(void **ele, char *ptr) {
	int16_t tnlen; // topicname
	resp_prod_topic_ack_t *rtopic;
	int part_len;

	rtopic = AP_MALLOC(sizeof(*rtopic), "resp_prod_topic_ack_t");
	AX_ASSERT(rtopic != NULL);

	rtopic->topicname = parse_resp_ax_string(ptr);
	tnlen =rtopic->topicname->len + 2;
	ptr += tnlen;

	rtopic->acks = parse_resp_ax_array(ptr,
		create_resp_prod_ack,
		destroy_resp_prod_ack,
		print_resp_prod_ack,
		&part_len);

	*ele = rtopic;
	return tnlen + part_len;
}

static void destroy_response_topic_ack(void *ele) {
	resp_prod_topic_ack_t *rtopic = (resp_prod_topic_ack_t *)ele;

	AP_FREE(rtopic->topicname, "topicname");
	destroy_ax_array(rtopic->acks); // ->destory_element();
	AP_FREE(rtopic, "rtopic");
}

static void print_response_topic_ack(void *ele, const char* prefix, int fd) {
	ax_string_t *nprefix = ax_append_string(prefix, "  ");
	resp_prod_topic_ack_t *rtopic = (resp_prod_topic_ack_t *)ele;

	PRINT_AX_STRING(prefix, "topic name", rtopic->topicname);
	print_ax_array(rtopic->acks, nprefix->s, "acks");

	AP_FREE(nprefix, "ax_string");
}

static resp_produce_t *parse_response_produce(ax_broker_t *brk, kafkap_req_buf_t *req) {
	int ret;
	resp_produce_t *rprod;
	recv_buf_t *recv = &brk->recv;

	rprod = AP_MALLOC(sizeof(*rprod), "rprod");
	if (!rprod)
		return NULL;

	rprod->topics = parse_resp_ax_array(recv->buffer + sizeof(ax_kafka_resp_hdr_t),
		create_response_topic_ack,
		destroy_response_topic_ack,
		print_response_topic_ack,
		&ret);

	return rprod;
}

static void print_kafka_resp_produce(resp_produce_t *rprod) {
	KAF_LOG(LOG_INFO, MOD_SYS, "produce response:\n");

	//PRINT_AX_STRING("  ", "topic name", rprod->topicname);

	print_ax_array(rprod->topics, "  ", "Topic Acks");
}

static void destroy_resp_produce(resp_produce_t *rprod, int self) {
	destroy_ax_array(rprod->topics);
	if (self)
		AP_FREE(rprod, "resp_produce_t");
}

// 打印meta data response
static void print_kafka_resp_meta(resp_meta_t *rmeta) {
	int i;

	KAF_LOG(LOG_INFO, MOD_SYS, "metadata response:\n");

	print_ax_array(rmeta->brokers, "  ", "brokers");
	print_ax_array(rmeta->topic_metadatas, "  ", "topic metadatas");
}

// 分析 响应
static void parse_response(recv_buf_t *rbuf) {
	ax_broker_t *brk = container_of(rbuf, ax_broker_t, recv);
	kafkap_req_buf_t *req;
	resp_meta_t *rmeta;
	resp_produce_t * rprod;

	// get corr_id
	SET_RESPONSE_CORR_ID(rbuf);

	print_kafka_resp(rbuf->buffer);

	req = find_req_from_hash(brk, rbuf->corr_id, 1);
	if (!req) {
		KAF_LOG(LOG_ERR, MOD_SYS, "Not found request in hash.\n");
		return;
	}

	switch (req->req_typ) {
	case RD_KAFKAP_Produce:
		hexdump(rbuf->buffer, rbuf->recv_len);
		rprod = parse_response_produce(brk, req);
		print_kafka_resp_produce(rprod);

		free_req_buffer(req);
		destroy_resp_produce(rprod, 1);
		break;

	case RD_KAFKAP_Metadata:
		rmeta = prase_response_metadata(brk, req);
		print_kafka_resp_meta(rmeta);

		// 释放request
		free_req_buffer(req);
		destory_resp_metadata(rmeta, 1);
		break;

	default:
		break;
	}
}

//
// 分配接收空间
static void broker_alloc_cb(uv_handle_t* handle,
    __attribute__((unused)) size_t size,
    uv_buf_t* buf) {
	ax_broker_t *brk = container_of((uv_tcp_t *)handle, ax_broker_t, tcp);

	switch (brk->recv.state) {
	case BROKER_RECV_STATE_INIT:
		buf->len  = MSG_FIELD_LEN;
		buf->base = brk->recv.slen;
		break;

	case BROKER_RECV_STATE_HEAD:
		AX_ASSERT(brk->recv.recv_len < MSG_FIELD_LEN);
		buf->len  = MSG_FIELD_LEN - brk->recv.recv_len;
		buf->base = brk->recv.slen + brk->recv.recv_len;
		break;

	case BROKER_RECV_STATE_BODY:
		AX_ASSERT(brk->recv.total > brk->recv.recv_len);
		buf->len  = brk->recv.total - brk->recv.recv_len;
		buf->base = brk->recv.buffer + brk->recv.recv_len;
		break;

	case BROKER_RECV_STATE_FINISH:
	default:
		AX_ASSERT(0);
		break;
	}
}

// response长度接收后, 知道了response的总长度, 分配空间
//
static int alloc_broker_resp_buf(recv_buf_t *rbuf) {
	AX_ASSERT(rbuf->total > 4);

	rbuf->buffer = AP_MALLOC(rbuf->total, "");

	if (rbuf->buffer == NULL)
		return -1;

	// message length
	*(int *)rbuf->buffer = rbuf->ilen;
	return 0;
}

// 
static int reset_broker_recv(recv_buf_t *rbuf) {
	if (rbuf->buffer != NULL) {
		free(rbuf->buffer);
		rbuf->buffer = NULL;
	}
	rbuf->state = BROKER_RECV_STATE_INIT;
	rbuf->recv_len = 0;
	rbuf->total = 0;

	return 0;
}

//
// 接收回调函数
static void broker_read_cb(__attribute__((unused)) uv_stream_t* stream,
                        ssize_t nread,
                        __attribute__((unused)) const uv_buf_t* buf) {
	ax_broker_t *brk = container_of((uv_tcp_t *)stream, ax_broker_t, tcp);

	// 失败的情况
	if (nread < 0) {
		// should close broker connection
		return;
	}
	// EAGAIN
	if (nread == 0) return;

	KAF_LOG(LOG_INFO, MOD_SYS, "nread=%d\n", nread);

	brk->recv.recv_len += nread;
	if (brk->recv.state == BROKER_RECV_STATE_INIT ||
		brk->recv.state == BROKER_RECV_STATE_HEAD) {
		// read head
		if (brk->recv.recv_len >= MSG_FIELD_LEN) {
			// 只能等于4
			AX_ASSERT(brk->recv.recv_len == MSG_FIELD_LEN);
			brk->recv.state = BROKER_RECV_STATE_BODY;

			GET_RESPONSE_LEN(&brk->recv);
			// 分配内容
			if (alloc_broker_resp_buf(&brk->recv) != 0) {
				reset_broker_recv(&brk->recv);
			}
		} else {
			brk->recv.state = BROKER_RECV_STATE_HEAD;
		}

		return;
	}

	AX_ASSERT(brk->recv.state == BROKER_RECV_STATE_BODY);
	// read body
	if (brk->recv.total < brk->recv.recv_len) {
		// 还没有接收完
		return;
	}
	// 处理消息
	parse_response(&brk->recv);

	reset_broker_recv(&brk->recv);
}


// 分解"host:port"格式的字符串, 填充到host和port数组中
static int parse_broker_addr(const char *addr, char *host, char *port, int host_len, int port_len) {
	char *ptr;

	ptr = strchr(addr, ':');
	if (!ptr) return -1;

	if (ptr - addr > host_len) return -2;

	strncpy(host, addr, ptr - addr);
	ptr ++;

	strncpy(port, ptr, addr + strlen(addr) - ptr);
	return 0;
}

//
// 初始化kafka broker(broker的地址已经填充)
// 填充地址，端口
// 设置status
// 初始化timer
static int init_kafka_broker(ax_broker_t *brk, ax_kafka_t *kaf) {
	int i, ret;
	char host[MAX_BROKER_ADDR_LEN] = "";
	char port[8] = "";
    in_addr_t paddr;

	ret = parse_broker_addr(brk->saddr->s, host, port, sizeof(host), sizeof(port));
	if (ret < 0) {
		AX_ASSERT(0);
	}

	// strncpy(brk->saddr, addr, strlen(addr) > sizeof(brk->saddr) - 1 ? sizeof(brk->saddr) - 1 : strlen(addr));
	
	paddr = inet_addr((const char *)host);
    if (paddr == INADDR_NONE) {
        KAF_LOG(LOG_ERR, MOD_SYS, "convert kafka broker %s failed!\n", host);
        AX_ASSERT(0);
    }

    // 初始化hash表头
    for (i = 0; i < MAX_BROKERS_HASH_NODES; i ++) {
    	INIT_LIST_HEAD(&brk->requests[i]);
    }

    brk->addr.s_addr = paddr;
    brk->port = atoi(port);
    AX_ASSERT(brk->port > 0 && brk->port < 65536);
    brk->status = BROKER_STATUS_INIT;
    brk->kafka = kaf;

	AX_ASSERT(uv_timer_init(&kaf_server.loop, &brk->timer) == 0);
    uv_timer_start(&brk->timer, connect_kafka_broker_tmr, 1000, 0);

	return 0;
}

//
//从kafka server中获得一个空闲的broker结构
static ax_broker_t *get_idle_broker(ax_kafka_t *kaf) {
	ax_broker_t *brk;

	if (kaf->brkrs_cnt >= MAX_KAFKA_BROKERS)
		return NULL;

	brk = &kaf->brkrs[kaf->brkrs_cnt];
	kaf->brkrs_cnt ++;
	return brk;
}

//
// 先kafka server中增加一个broker, 多个broker地址使用","分隔
static int add_kaf_brokers(ax_kafka_t *kaf, const char *addr) {
	ax_broker_t *brk;
	char *ptr = (char *)addr;
	char *end = (char *)addr + strlen(addr);
	char *comma;

	for (;;) {
		brk = get_idle_broker(kaf);
		if (!brk) {
			KAF_LOG(LOG_ERR, MOD_SYS, "get_idle_broker failed!\n");
			break;
		}

		comma = strchr(ptr, ',');
		if (comma) {
			// 有个多broker
			brk->saddr = mk_ax_string_by_len(ptr, comma - ptr);
			ptr = comma + 1;

			init_kafka_broker(brk, kaf);
		} else {
			// 最后一个或仅有一个
			brk->saddr = mk_ax_string_by_len(ptr, strlen(addr));
			init_kafka_broker(brk, kaf);

			break;
		}
	}

	return 0;
}


// 先buffer中填充string
static int write_buffer_string(struct kafkap_req_buf_s *buf, ax_string_t *s) {
	char *ptr = GET_BUFFER_LATEST_PTR(buf);

	if (buf->offset + 2 + s->len > buf->length) {
		return -1;
	}

	WRITE_BUFFER_INT16(ptr, (int16_t)s->len);
	strncpy(ptr, s->s, s->len);
	PUSH_BUFF_OFFSET(buf, 2+s->len);

	return 0;
}

static int fill_ax_string(struct kafkap_req_buf_s * req, void * data) {
	ax_string_t *as = (ax_string_t *)data;

	return write_buffer_string(req, as);
}

//
// 填充数组
// elements: elements数组指针
// items:    elements数量
static int write_buffer_array(struct kafkap_req_buf_s *buf,
	void **elements,
	int items,
	int fill_header,
	int (*fn)(struct kafkap_req_buf_s *, void *)) {
	char *array_head; // 需要填充的数组长度	
	int i;
	int tlen = 0;

	if (fill_header) {
		array_head = GET_BUFFER_LATEST_PTR(buf);
		WRITE_BUFFER_INT32(array_head, items);
		PUSH_BUFF_OFFSET(buf, 4);
	}

	for (i = 0; i < items; i ++) {
		tlen += fn(buf, elements[i]);
	}

	if (fill_header)
		return tlen + 4;

	return tlen;
}

//
// 根据链表填充数组
// typ:    element元素类型
// head:   链表头指针
// items:  elements数量
static int write_buffer_array_from_list(struct kafkap_req_buf_s *buf,
		int typ,
		struct list_head *head,
		int (*fn)(struct kafkap_req_buf_s *, struct list_head *)) {
	char *array_head; // 需要填充的数组长度	
	int items = 0;
	struct list_head *pos;

	// 保存数组长度的位置, 循环结束后填充
	array_head = GET_BUFFER_LATEST_PTR(buf);
	PUSH_BUFF_OFFSET(buf, 4);
	list_for_each(pos, head) {
		fn(buf, pos);
		items ++;
	}
	WRITE_BUFFER_INT32(array_head, items);
	PUSH_BUFF_OFFSET(buf, 4);

	return 0;
}

//
// 填充request 头部
// size: 待定
// api key:     参数
// api version: version
// correlationId: 递增
// client_id:     字符串
static int build_kafka_req(ax_kafka_t *server, struct kafkap_req_buf_s *req) {
	struct rd_kafkap_reqhdr *hdr = (struct rd_kafkap_reqhdr *)(req->buffer);

	hdr->api_key = htons((int16_t)req->req_typ);
	hdr->version = 0;
	req->corr_id = ++server->corr_id;
	hdr->corr_id = htonl(req->corr_id);
	PUSH_BUFF_OFFSET(req, sizeof(*hdr));
	// copy client_id
	write_buffer_string(req, server->clientid);

	return 0;
}

//
// 分配并初始化请求buffer
static struct kafkap_req_buf_s* alloc_req_buffer(ax_broker_t *brk, size_t s, int typ) {
	struct kafkap_req_buf_s* buf = NULL;

	buf = AP_MALLOC(ALIGN(s, 16), "");
	if (!buf)
		return NULL;

	INIT_LIST_HEAD(&buf->list);
	buf->req_typ = typ;
	buf->brk = brk;
	buf->length = KAFKA_REQUEST_BUFF_LEN(s);
	buf->offset = 0;
	buf->buffer = (char *)buf + offsetof(struct kafkap_req_buf_s, buffer) + sizeof(char *);

	return buf;
}

// 
// 释放资源
void free_req_buffer(struct kafkap_req_buf_s* buf) {
	free(buf);
}

//
// 构造meta 请求
static struct kafkap_req_buf_s* mk_broker_meta_req(ax_broker_t *brk, const char *topic) {
	ax_string_t *as;
	struct kafkap_req_buf_s* req;

	as = mk_ax_string((char *)topic);

	req = alloc_req_buffer(brk, KAFKA_REQUEST_LEN(brk->kafka, 4 + as->len + 2), RD_KAFKAP_Metadata);  //

	if (!req) {
		AX_ASSERT(0);
	}

	build_kafka_req(brk->kafka, req);

	// 填充meta request topic数组
	// nd_logout-00
	write_buffer_array(req, (void **)&as, 1, 1, fill_ax_string);

	// 填充request长度
	WRITE_BUFFER_SIZE(req);

	free_ax_string(as);

	return req;
}

static void send_request_cb(uv_write_t* req, int status) {
	struct kafkap_req_buf_s* buf = container_of(req, struct kafkap_req_buf_s, req);

	if (status < 0) {
		KAF_LOG(LOG_ERR, MOD_SYS, "send request failed: status = %d\n", status);
	}

	insert_to_req_hash(buf->brk, buf);
}

// 发送meta request
static int send_broker_request(struct kafkap_req_buf_s* req) {
	ax_broker_t *brk = req->brk;

	req->buf.base = req->buffer;
	req->buf.len  = req->offset;
	uv_write(&req->req, (uv_stream_t*)&brk->tcp, &req->buf, 1, send_request_cb);

	return 0;
}

static int print_bytes_array(const char *prefix, const char *name, char *data,
	int (*fn)(const char *, char *)) {
	char *ptr = (char *)data;

	int i = 0, ret;
	int items = ntohl(*(int *)ptr);
	ax_string_t *nprefix = ax_append_string(prefix, "  ");

	ptr += 4;
	for (i = 0; i < items; i ++) {
		ret = fn((const char *)nprefix->s, ptr);
		ptr += ret;
	}

	free_ax_string(nprefix);

	return ptr - data;
}

static int print_byte_string(const char *prefix, char *data) {
	int len = ntohs(*(int16_t *)data);
	ax_string_t *nprefix = ax_append_string(prefix, "  ");
	ax_string_t *as;

	as = mk_ax_string_by_len((const char *)(data+2), len);
	PRINT_NONAME_STRING(nprefix->s, as->s);

	free_ax_string(as);
	free_ax_string(nprefix);

	return 2 + len;
}
// MessageSet => [Offset MessageSize Message]
//   Offset => int64
//   MessageSize => int32
// v0
// Message => Crc MagicByte Attributes Key Value
//   Crc => int32
//   MagicByte => int8
//   Attributes => int8
//   Key => bytes
//   Value => bytes
static int print_message_set_content(const char *prefix, char *data) {
	int len;
	ax_string_t *nprefix = ax_append_string(prefix, "  ");
	char *orig = data;

	PRINT_BE_INT64_PTR(nprefix->s, "offset", data);
	PRINT_BE_INT32_PTR(nprefix->s, "MessageSize", data);
	PRINT_BE_INT32_PTR(nprefix->s, "crc", data);
	PRINT_BE_INT8_PTR(nprefix->s, "MagicByte", data);
	PRINT_BE_INT8_PTR(nprefix->s, "Attributes", data);
	data += print_buffer_as_string(nprefix->s, "Key", data);
	data += print_buffer_as_string(nprefix->s, "Value", data);

	free_ax_string(nprefix);

	return data - orig;
}

//[Partition MessageSetSize MessageSet]
static int print_message_set(const char *prefix, char *data) {
	int len;
	ax_string_t *nprefix = ax_append_string(prefix, "  ");

	PRINT_BE_INT32_PTR(nprefix->s, "Partition", data);
	PRINT_BE_INT32_PTR(nprefix->s, "MessageSetSize", data);

	len = 8;
	len += print_bytes_array(nprefix->s, "meesage set", data, print_message_set_content);

	free_ax_string(nprefix);

	return len;
}

//[TopicName [Partition MessageSetSize MessageSet]]
static int print_topic_messages(const char *prefix, char *data) {
	ax_string_t *nprefix = ax_append_string(prefix, "  ");
	int len = ntohs(*(int16_t *)data);
	ax_string_t *as;

	as = mk_ax_string_by_len((const char *)(data+2), len);
	PRINT_STRING(nprefix->s, "Topic Name", as->s);
	free_ax_string(as);

	data += 2 + len;
	len += 2;
	len += print_bytes_array(nprefix->s, "messageset array", data, print_message_set);

	free_ax_string(nprefix);

	return len;
}



static void print_meta_request(const char *data, int len) {
	char *ptr = (char *)data;

	print_bytes_array("", "Topic", ptr, print_byte_string);
	PRINT_LF;
}

static void print_produce_request(const char *data, int len) {
	char *ptr = (char *)data;

	PRINT_BE_INT16_PTR("", "request ack", ptr);
	PRINT_BE_INT32_PTR("", "timeout", ptr);

	print_bytes_array("", "Topic messages", ptr, print_topic_messages);

	PRINT_LF;
}

static void print_request(struct kafkap_req_buf_s* req) {
	static const char *req_names[] = {
		"produce",
		"fetch",
		"offset",
		"meta",
		"reserve4",
		"reserve5",
		"reserve6",
		"reserve7",
	};
	ax_string_t *clientid;

	PRINT_STRING("", "kafka request", req_names[req->req_typ]);
	PRINT_INT32("", "struct len", req->length);
	PRINT_INT32("", "request body len", req->offset);
	PRINT_INT32("", "message size", ntohl(*(int*)req->buffer));
	PRINT_INT32("", "request type", req->req_typ);
	PRINT_INT32("", "correlationId", req->corr_id);
	clientid = req->brk->kafka->clientid;
	PRINT_AX_STRING("", "clientid", clientid);

	switch (req->req_typ) {
	case RD_KAFKAP_Produce:
		print_produce_request(req->buffer + KAFKA_REQUEST_HEADER_LEN(clientid),
			req->offset - KAFKA_REQUEST_HEADER_LEN(clientid));
		break;

	case RD_KAFKAP_Fetch:
		break;

	case RD_KAFKAP_Offset:
		break;

	case RD_KAFKAP_Metadata:
		print_meta_request(req->buffer + KAFKA_REQUEST_HEADER_LEN(clientid),
			req->offset - KAFKA_REQUEST_HEADER_LEN(clientid));
		break;
	}
}

// 构建并发送 meta request
static int do_meta_request(ax_broker_t *brk, const char *name, ax_kaf_topic_t *topic) {
	struct kafkap_req_buf_s* req;

	req = mk_broker_meta_req(brk, name);
	if (!req) return -1;

	req->data = topic;
	print_request(req);

	send_broker_request(req);
	return 0;
}

// 发送所有的request
static int do_all_meta_request(ax_broker_t *brk, const char *topic, int total) {
	char name[256];
	int i;

	for (i = 0; i < total; i ++) {
		snprintf(name, sizeof(name) - 1, "%s-%02d", topic, i);
		do_meta_request(brk, name, NULL);
	}
}

// MessageSet => [Offset MessageSize Message]
//   Offset => int64
//   MessageSize => int32
//
// v0
// Message => Crc MagicByte Attributes Key Value
//   Crc => int32
//   MagicByte => int8
//   Attributes => int8
//   Key => bytes
//   Value => bytes
static int fill_produce_message_nocompress(struct kafkap_req_buf_s *req, void *data) {
	char *ptr = GET_BUFFER_LATEST_PTR(req);
	int msg_len;
	//int crc;
	char *crc_ptr;
	ax_produce_topic_msg_t *msg = (ax_produce_topic_msg_t *)data;
	ax_buf_t *buf = &msg->bufs[msg->index];

	// offset 无压缩
	WRITE_BUFFER_INT64(ptr, 0);
	PUSH_BUFF_OFFSET(req, 8);
	// crc + magic + attr + key + value
	msg_len = 4 + 1 + 1 + 4 + (4 + buf->len); // 14 + buf->len
	WRITE_BUFFER_INT32(ptr, msg_len);
	PUSH_BUFF_OFFSET(req, 4);

	// 保存crc指针位置
	crc_ptr = ptr;
	ptr += 4;
	PUSH_BUFF_OFFSET(req, 4);

	WRITE_BUFFER_INT8(ptr, 0);  // magic, 0
	WRITE_BUFFER_INT8(ptr, 0);  // 无压缩
	PUSH_BUFF_OFFSET(req, 2);

	// key
	WRITE_BUFFER_AX_BYTES(ptr, (ax_buf_t *)NULL);
	PUSH_BUFF_OFFSET(req, 4);

	// value
	WRITE_BUFFER_AX_BYTES(ptr, buf);
	PUSH_BUFF_OFFSET(req, buf->len + 4);

	// 计算crc
	write_crc(crc_ptr, (crc_ptr + 4), 2 + 4 + 4 + buf->len);

	return msg_len + 12;
}

// [Partition MessageSetSize MessageSet]
static int fill_produce_message_record(struct kafkap_req_buf_s *req, void *data) {
	ax_produce_topic_msg_t *msg = (ax_produce_topic_msg_t *)data;
	char *ptr = GET_BUFFER_LATEST_PTR(req);
	char *ptr_len;
	int msg_len = 0;

	WRITE_BUFFER_INT32(ptr, msg->partition);
	PUSH_BUFF_OFFSET(req, 4);

	ptr_len = ptr;
	ptr += 4;
	PUSH_BUFF_OFFSET(req, 4);

	// message set
	msg_len = write_buffer_array(req, (void **)&msg, 1, 0, fill_produce_message_nocompress);

	// 延迟写入
	WRITE_BUFFER_INT32(ptr_len, msg_len);

	return 8 + msg_len;
}

// [TopicName [Partition MessageSetSize MessageSet]]
static int fill_produce_topic_msg(struct kafkap_req_buf_s *req, void *data) {
	ax_produce_topic_msg_t *msg = (ax_produce_topic_msg_t *)data;
	ax_produce_topic_msg_t *msgs[1];
	char *ptr = GET_BUFFER_LATEST_PTR(req);
	int tlen = 0;

	WRITE_BUFFER_AX_STRING(ptr, msg->topic);
	tlen = msg->topic->len + 2;
	PUSH_BUFF_OFFSET(req, tlen);

	msgs[0] = msg;
	tlen += write_buffer_array(req, (void **)msgs, 1, 1, fill_produce_message_record);

	return tlen;
}

// 构造 request
// ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
//   RequiredAcks => int16
//   Timeout => int32
//   Partition => int32
//   MessageSetSize => int32
static build_kafka_request_msg(struct kafkap_req_buf_s* req, ax_kaf_topic_t *topic,
		int16_t ack, int tmo, int partition, int compress,
		const char *data, int len) {
	char *ptr = GET_BUFFER_LATEST_PTR(req);
	ax_produce_topic_msg_t msg;
	ax_produce_topic_msg_t *msgs[1];
	ax_buf_t buf;

	WRITE_BUFFER_INT16(ptr, ack);
	WRITE_BUFFER_INT32(ptr, tmo);
	PUSH_BUFF_OFFSET(req, 6);

	buf.base = (char *)data;
	buf.len  = len;

	msg.topic = topic->topicname;
	msg.compress  = compress;
	msg.partition = partition;
	msg.bufs = &buf;
	msg.howmany_bufs = 1;
	msg.index = 0;

	msgs[0] = &msg;
	write_buffer_array(req, (void **)msgs, 1, 1, fill_produce_topic_msg);
}

//
// 构造 producer 请求
static struct kafkap_req_buf_s* mk_broker_producer_req(ax_kaf_topic_t *topic, const char *data, int len) {
	ax_broker_t *brk = topic->brk;
	struct kafkap_req_buf_s* req;

	req = alloc_req_buffer(brk, KAFKA_REQUEST_LEN(brk->kafka,
		KAFKA_PRODUCE_REQUEST_LEN(topic->topicname, KAFKA_MESSAGE_SET_LEN(0, len))), RD_KAFKAP_Produce);  //

	if (!req) {
		AX_ASSERT(0);
	}

	build_kafka_req(brk->kafka, req);

	build_kafka_request_msg(req, topic, 1, 10000, 0, 0, data, len);

	// 填充request长度
	WRITE_BUFFER_SIZE(req);

	return req;
}

// 发送消息接口
static int do_kaf_produce_request(ax_kaf_topic_t *topic, const char *data, int len) {
	struct kafkap_req_buf_s *req;

	req = mk_broker_producer_req(topic, data, len);
	print_request(req);
	hexdump(req->buffer, req->offset);

	send_broker_request(req);
	return 0;
}

// 创建 topic
static ax_kaf_topic_t * create_new_topic(ax_kafka_t *server, char *name) {
	ax_kaf_topic_t *topic;

	topic = AP_MALLOC(sizeof(*topic), "ax_kaf_topic");
	if (!topic)
		return NULL;

	topic->topicname = mk_ax_string(name);
	topic->kaf = server;

	// 发送 meta request
	return topic;
}

//
// 连接kafka broker callback
static void connect_kafka_broker_cb(uv_connect_t *req, int status) {
	ax_broker_t *brk = container_of(req, ax_broker_t, connect_req);

	if (status < 0) {
		KAF_LOG(LOG_ERR, MOD_SYS, "connect_kafka_broker_cb: failed, status=%d\n", status);

        uv_close((uv_handle_t*)&brk->tcp, NULL);
		// 启动连接定时器
        uv_timer_start(&brk->timer, connect_kafka_broker_tmr, KAFKA_BROKER_RECONNECT_SECOND * 1000, 0);
		return;
	}

	KAF_LOG(LOG_INFO, MOD_SYS, "connect to kafka broker %s success ....\n", brk->saddr->s);
	brk->status = BROKER_STATUS_CONNECTED;
	brk->recv.state = BROKER_RECV_STATE_INIT;

	// 
	do_all_meta_request(brk, "nd_logout", 1);
	//do_all_meta_request(brk, "nd_http_url", 100);
	//do_all_meta_request(brk, "nd_http_req", 100);

    // uv_tcp_connect 内部会设置req->handle = tcp; 因此，这里的tcp就是 req->handle
	uv_read_start((uv_stream_t *)&brk->tcp, broker_alloc_cb, broker_read_cb);

	// ax_topic_t
	ax_kaf_topic_t *topic;
	topic = create_new_topic(brk->kafka, "nd_logout-00");
	topic->brk = brk;

	do_kaf_produce_request(topic, "hello, world", strlen("hello, world"));
}

//
// 使用定时器连接kafka broker
void connect_kafka_broker_tmr(uv_timer_t* data) {
	ax_broker_t *brk = container_of(data, ax_broker_t, timer);
    struct sockaddr_in *paddr;
    struct sockaddr_storage addr_s;

    // 停止连接定时器
    uv_timer_stop(&brk->timer);
    brk->status = BROKER_STATUS_CONNECTING;

    //AX_ASSERT(atomic_read(&peer->status) == 0);
    KAF_LOG(LOG_INFO, MOD_SYS, "connect to kafka broker %s %u.%u.%u.%u %hd ....\n",
    	brk->saddr->s, _ADDR(brk->addr.s_addr), brk->port);

    AX_ASSERT(uv_tcp_init(&kaf_server.loop, &brk->tcp) == 0);

    paddr = (struct sockaddr_in *)&addr_s;
    paddr->sin_family = AF_INET;
    paddr->sin_port = htons( brk->port );
    paddr->sin_addr = brk->addr;

    AX_ASSERT(uv_tcp_connect(&brk->connect_req,
        &brk->tcp, (const struct sockaddr*) paddr, connect_kafka_broker_cb) == 0);
}

//
// 初始化 kaf_server的uv_loop
static int init_uv_loop(void) {
	int ret;

	ret = uv_loop_init(&kaf_server.loop);
	AX_ASSERT(ret == 0);

	return ret;
}

static void init_kaf_server(ax_kafka_t *server) {
	memset(server, 0, sizeof(ax_kafka_t));

	server->clientid = mk_ax_string("rdkafka");
}

int main(int argc, char *argv[]) {
	init_kaf_server(&kaf_server);
	init_uv_loop();

	add_kaf_brokers(&kaf_server, "10.10.141.55:9092");

	uv_run(&kaf_server.loop, UV_RUN_DEFAULT);

	return 0;
}
