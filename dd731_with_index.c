/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2020 Intel Corporation.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"
#include "spdk/config.h"

#include "spdk/bdev.h"
#include "spdk/event.h"
#include "spdk/fd.h"
#include "spdk/json.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/vmd.h"

#include <libaio.h>
#include <dirent.h>

#ifdef SPDK_CONFIG_URING
#include <liburing.h>
#endif

#define TIMESPEC_TO_MS(time) ((time.tv_sec * 1000) + (time.tv_nsec / 1000000))
#define STATUS_POLLER_PERIOD_SEC 1
#define DD_MIN_WQE_SIZE (64U * 1024U)
#define DD_MAX_WQE_SIZE (8U * 1024U * 1024U)
#define DD_WQE_STEP_SIZE (64U * 1024U)
#define DD_ADAPT_INTERVAL_MS 1000.0
#define DD_ADAPT_POLLER_PERIOD_US (250 * 1000)
#define DD_ADAPT_MIN_COMPLETIONS 16U
#define DD_ADAPT_WARMUP_WINDOWS 2U
#define DD_PRIORITY_HIGH_WEIGHT 4U
#define DD_PRIORITY_MEDIUM_WEIGHT 2U
#define DD_PRIORITY_LOW_WEIGHT 1U
#define DD_STREAM_DEFAULT_FRAME_DEADLINE_US 0U
#define DD_MAX_FLOWS 128U

struct spdk_dd_opts {
	char		*input_file;
	char		*output_file;
	char		*input_file_flags;
	char		*output_file_flags;
	char		*input_bdev;
	char		*output_bdev;
	char		*flow_config_file;
	char		*index_file;       /* 索引文件路径 */
	char		*input_dir;        /* 小文件打包：输入目录路径 */
	uint64_t	input_offset;
	uint64_t	output_offset;
	int64_t		io_unit_size;
	int64_t		io_unit_count;
	uint32_t	queue_depth;
	bool		aio;
	bool		sparse;
	/* ===== 新增选项 ===== */
    bool            adaptive_mode;     /* 是否启用自适应模式 */
    uint32_t        priority_mode;     /* 0=无，1=三级优先级 */
	uint32_t	stream_priority;  /* 用户指定的固定优先级 */
	bool		stream_mode;      /* 连续流模式 */
	uint64_t	frame_deadline_us; /* 超过该等待时延后可丢弃旧帧 */
	uint32_t	drop_policy;      /* 0=none, 1=drop-oldest */
	bool		priority_explicit; /* 命令行显式指定 --priority */
	bool		dump_sgl;          /* 是否打印每次IO的SGL */
	bool		dump_prio;         /* 是否打印每次IO的优先级 */
};

static struct spdk_dd_opts g_opts = {
	.io_unit_size = 4096,
	.queue_depth = 2,
	
	.adaptive_mode = true,     /* 新增默认值 */
	.priority_mode = 1,
	.stream_priority = 1,      /* DD_PRIO_MEDIUM */
	.stream_mode = false,
	.frame_deadline_us = DD_STREAM_DEFAULT_FRAME_DEADLINE_US,
	.drop_policy = 1,
	.priority_explicit = false,
	.dump_sgl = false,
	.dump_prio = false,
};

enum dd_submit_type {
	DD_POPULATE,
	DD_READ,
	DD_WRITE,
};
/* ===== 新增：优先级定义 ===== */
enum dd_data_priority {
    DD_PRIO_LOW = 0,
    DD_PRIO_MEDIUM = 1,
    DD_PRIO_HIGH = 2,
};

enum dd_drop_policy {
	DD_DROP_NONE = 0,
	DD_DROP_OLDEST = 1,
};

struct dd_flow {
	uint64_t	input_region_start;
	uint64_t	output_region_start;
	uint64_t	copy_size;
	uint64_t	input_pos;
	enum dd_data_priority priority;
	uint64_t	submitted_ios;
	uint64_t	completed_ios;
};

struct dd_flow_entry_cfg {
	uint64_t	input_offset;
	uint64_t	output_offset;
	uint64_t	io_unit_count;
	char		*priority;
};

struct dd_flow_cfg {
	size_t	num_flows;
	struct dd_flow_entry_cfg entries[DD_MAX_FLOWS];
};

struct dd_io {
	uint64_t		offset;
	uint64_t		length;
	struct iocb		iocb;
	enum dd_submit_type	type;
	enum dd_data_priority priority;  /* 新增：优先级 */
	struct dd_flow	*flow;
	uint32_t	flow_idx;
#ifdef SPDK_CONFIG_URING
	int			idx;
#endif
	void			*buf;
	bool			ready;  // 添加这一行
	STAILQ_ENTRY(dd_io)	link;

    /* ===== 新增：SGL 链条 ===== */
    struct iovec    *iovs;       /* iov 数组 */
    int         iovpos;      /* 当前位置 */
    int         iovcnt;      /* 总数量 */
    uint32_t        iov_offset;  /* 当前偏移 */
	uint32_t        sgl_wqe_size;    /* 当前 SGL 对应的 WQE 大小 */
	bool            sgl_needs_rebuild; /* WQE 变化后等待重建 */

    /* ===== 新增：时间戳（用于指标采集）===== */
    uint64_t        enqueue_tsc;  /* 入队时刻 */
    uint64_t        submit_tsc;   /* 下发时刻 */
    uint64_t        complete_tsc; /* 完成时刻 */
	uint64_t        last_latency_ticks; /* 最近一次提交到完成时延 */
};

enum dd_target_type {
	DD_TARGET_TYPE_FILE,
	DD_TARGET_TYPE_BDEV,
};

struct dd_target {
	enum dd_target_type	type;

	union {
		struct {
			struct spdk_bdev *bdev;
			struct spdk_bdev_desc *desc;
			struct spdk_io_channel *ch;
		} bdev;

#ifdef SPDK_CONFIG_URING
		struct {
			int fd;
			int idx;
		} uring;
#endif
		struct {
			int fd;
		} aio;
	} u;

	/* Block size of underlying device. */
	uint32_t	block_size;

	/* Position of next I/O in bytes */
	uint64_t	pos;

	/* Total size of target in bytes */
	uint64_t	total_size;

	bool open;
};

struct dd_job {
	struct dd_target	input;
	struct dd_target	output;

	struct dd_io		*ios;

	union {
#ifdef SPDK_CONFIG_URING
		struct {
			struct io_uring ring;
			bool active;
			struct spdk_poller *poller;
		} uring;
#endif
		struct {
			io_context_t io_ctx;
			struct spdk_poller *poller;
		} aio;
	} u;

	uint32_t		outstanding;
	uint64_t		copy_size;
	struct dd_flow		*flows;
	uint32_t		num_flows;
	STAILQ_HEAD(, dd_io)	seek_queue;

	struct timespec		start_time;
	uint64_t		total_bytes;
	uint64_t		incremental_bytes;
	struct spdk_poller	*status_poller;
	struct spdk_poller	*adaptive_poller;


	/* ===== 新增：优先级队列 ===== */
    STAILQ_HEAD(, dd_io) high_prio_queue;
    STAILQ_HEAD(, dd_io) medium_prio_queue;
    STAILQ_HEAD(, dd_io) low_prio_queue;
	uint32_t        prio_high_credits;
	uint32_t        prio_medium_credits;
	uint32_t        prio_low_credits;

    /* ===== 新增：自适应参数 ===== */
    uint32_t        base_wqe_size;       /* 基础 WQE 大小（配置） */
    uint32_t        current_wqe_size;    /* 当前 WQE 大小（动态） */
    uint64_t        last_adapt_tsc;      /* 上次调整时刻 */

    /* ===== 新增：性能指标窗口 ===== */
    uint64_t        window_completed_ios;   /* 窗口内完成 IO 数 */
	uint64_t        total_completed_ios;    /* 生命周期完成 IO 数 */
    uint64_t        window_lost_ios;        /* 窗口内丢失 IO 数 */
    uint64_t        recent_rtt_ticks;       /* 最近 RTT（tick） */
    double          recent_loss_rate;       /* 最近丢包率 */

    /* ===== 新增：性能输出指标 ===== */
    double          effective_throughput_mbps;
    double          avg_e2e_latency_us;
    double          data_freshness_us;      /* 平均等待时间 */
	uint64_t	prio_dispatch_ios[3];

	/* ===== 小文件打包状态 ===== */
	char	      **pack_file_list;    /* 目录扫描出的文件路径列表 */
	int		pack_file_count;   /* 文件总数 */
	int		pack_file_cursor;  /* 当前处理到哪个文件 */
	uint64_t	pack_file_inner_offset; /* 当前文件已读偏移（支持大文件跨WQE续读） */
	int		pack_fd;           /* 当前打开文件的 fd（-1 表示未打开） */
};

static struct dd_job g_job = {
    .base_wqe_size = 4096,      /* 新增初始化 */
    .current_wqe_size = 4096,
    .pack_fd = -1,
};


/* ✅ 新增：TSC 相关全局变量 */
static uint64_t g_tsc_rate = 0;                    // CPU TSC 频率
static uint64_t g_total_bytes_submitted = 0;
static uint64_t g_total_bytes_completed = 0;
static uint64_t g_packets_lost = 0;
static uint64_t g_adaptive_adjustments = 0;
static uint64_t g_stream_dropped_ios = 0;
static uint64_t g_stream_input_wraps = 0;
static uint64_t *g_latency_samples = NULL;
static size_t g_latency_sample_count = 0;
static size_t g_latency_sample_capacity = 0;
static uint64_t g_peak_outstanding = 0;
static uint64_t g_wqe_size_sum = 0;
static uint64_t g_wqe_size_samples = 0;
static uint32_t g_wqe_size_min = UINT32_MAX;
static uint32_t g_wqe_size_max = 0;
static bool g_progress_line_active = false;
static pthread_mutex_t g_metrics_mutex = PTHREAD_MUTEX_INITIALIZER; 

static int
dd_compare_latency_ticks(const void *a, const void *b)
{
	uint64_t left = *(const uint64_t *)a;
	uint64_t right = *(const uint64_t *)b;
	return left < right ? -1 : (left > right ? 1 : 0);
}

struct dd_flags {
	char *name;
	int flag;
};

static struct dd_flags g_flags[] = {
	{"append", O_APPEND},
	{"direct", O_DIRECT},
	{"directory", O_DIRECTORY},
	{"dsync", O_DSYNC},
	{"noatime", O_NOATIME},
	{"noctty", O_NOCTTY},
	{"nofollow", O_NOFOLLOW},
	{"nonblock", O_NONBLOCK},
	{"sync", O_SYNC},
	{NULL, 0}
};

//static struct dd_job g_job = {};
static int g_error = 0;
static bool g_interrupt;

static void dd_target_seek(struct dd_io *io);
static void _dd_bdev_seek_hole_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg);
static bool dd_should_print_progress(void);
static void dd_show_progress_adaptive(struct dd_job *job, bool finish);
static void dd_show_progress_finish(void);
static void dd_print_queue_status(struct dd_job *job);
static void dd_reset_sgl(struct dd_io *io, uint32_t sgl_offset);
static int dd_next_sge(struct dd_io *io, void **address, uint32_t *length);
static int dd_rebuild_io_sgl(struct dd_io *io, uint32_t wqe_size);
static void dd_dump_io_sgl(struct dd_io *io, const char *stage);
static void dd_dump_io_priority(struct dd_io *io, const char *stage);
static void dd_refill_priority_credits(struct dd_job *job);
static void dd_update_io_priority(struct dd_job *job, struct dd_io *io);
static void dd_schedule_next_io(void);
static void dd_progress_line_break(void);
static int dd_setup_flows(void);
static uint64_t dd_get_completed_bytes(void);
static char **dd_scan_dir(const char *dirpath, int *out_count);
static char **dd_scan_dir_recursive_root(const char *dirpath, int *out_count);
static int dd_write_pack_index(const char *index_path, const char *dirpath,
			       char **files, int file_count);
static uint64_t dd_pack_files_into_buf(struct dd_io *io);

static int
dd_cmp_str(const void *a, const void *b)
{
	return strcmp(*(const char **)a, *(const char **)b);
}

static void
dd_cleanup_bdev(struct dd_target io)
{
	/* This can be only on the error path.
	 * To prevent the SEGV, need add checks here.
	 */
	if (io.u.bdev.ch) {
		spdk_put_io_channel(io.u.bdev.ch);
	}

	spdk_bdev_close(io.u.bdev.desc);
}

static void
dd_exit(int rc)
{
	if (g_job.input.type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			close(g_job.input.u.uring.fd);
		} else
#endif
		{
			close(g_job.input.u.aio.fd);
		}
	} else if (g_job.input.type == DD_TARGET_TYPE_BDEV && g_job.input.open) {
		dd_cleanup_bdev(g_job.input);
	}

	if (g_job.output.type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			close(g_job.output.u.uring.fd);
		} else
#endif
		{
			close(g_job.output.u.aio.fd);
		}
	} else if (g_job.output.type == DD_TARGET_TYPE_BDEV && g_job.output.open) {
		dd_cleanup_bdev(g_job.output);
	}

	if (g_job.input.type == DD_TARGET_TYPE_FILE || g_job.output.type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			spdk_poller_unregister(&g_job.u.uring.poller);
		} else
#endif
		{
			spdk_poller_unregister(&g_job.u.aio.poller);
		}
	}

	spdk_poller_unregister(&g_job.status_poller);
	spdk_poller_unregister(&g_job.adaptive_poller);

	/* 写入索引条目 */
	if (rc == 0 && g_opts.index_file != NULL && g_opts.output_bdev != NULL &&
	    g_opts.input_dir == NULL) {
		FILE *idx = fopen(g_opts.index_file, "a");
		if (idx) {
			const char *fname = g_opts.input_file ? g_opts.input_file : "";
			uint64_t offset_bytes = g_opts.output_offset * (uint64_t)g_opts.io_unit_size;
			uint64_t total = dd_get_completed_bytes();

			fprintf(idx,
				"{\"name\":\"%s\",\"offset\":%lu,\"size\":%lu}\n",
				fname,
				(unsigned long)offset_bytes,
				(unsigned long)total);
			fclose(idx);
		}
	}

	spdk_app_stop(rc);
}

static uint64_t
dd_get_completed_bytes(void)
{
	uint64_t completed_bytes;

	pthread_mutex_lock(&g_metrics_mutex);
	completed_bytes = g_total_bytes_completed;
	pthread_mutex_unlock(&g_metrics_mutex);

	return completed_bytes;
}

static void
dd_progress_line_break(void)
{
	if (!g_progress_line_active) {
		return;
	}

	fprintf(stderr, "\n");
	fflush(stderr);
	g_progress_line_active = false;
}

static void
dd_show_progress(bool finish)
{
	char *unit_str[5] = {"", "k", "M", "G", "T"};
	char *speed_type_str[2] = {"", "average "};
	char *size_unit_str = "";
	char *speed_unit_str = "";
	char *speed_type;
	uint64_t size_unit = 1;
	uint64_t speed_unit = 1;
	uint64_t speed, tmp_speed;
	int i = 0;
	uint64_t milliseconds;
	uint64_t size, tmp_size;

	size = g_job.incremental_bytes;

	g_job.incremental_bytes = 0;
	g_job.total_bytes += size;

	if (finish) {
		struct timespec time_now;

		clock_gettime(CLOCK_REALTIME, &time_now);

		milliseconds = spdk_max(1, TIMESPEC_TO_MS(time_now) - TIMESPEC_TO_MS(g_job.start_time));
		size = g_job.total_bytes;
	} else {
		milliseconds = STATUS_POLLER_PERIOD_SEC * 1000;
	}

	/* Find the right unit for size displaying (B vs kB vs MB vs GB vs TB) */
	tmp_size = size;
	while (tmp_size > 1024 * 10) {
		tmp_size >>= 10;
		size_unit <<= 10;
		size_unit_str = unit_str[++i];
		if (i == 4) {
			break;
		}
	}

	speed_type = finish ? speed_type_str[1] : speed_type_str[0];
	speed = (size * 1000) / milliseconds;

	i = 0;

	/* Find the right unit for speed displaying (Bps vs kBps vs MBps vs GBps vs TBps) */
	tmp_speed = speed;
	while (tmp_speed > 1024 * 10) {
		tmp_speed >>= 10;
		speed_unit <<= 10;
		speed_unit_str = unit_str[++i];
		if (i == 4) {
			break;
		}
	}

	printf("\33[2K\rCopying: %" PRIu64 "/%" PRIu64 " [%sB] (%s%" PRIu64 " %sBps)",
	       g_job.total_bytes / size_unit, g_job.copy_size / size_unit, size_unit_str, speed_type,
	       speed / speed_unit, speed_unit_str);
	fflush(stdout);
}

static int
dd_status_poller(void *ctx)
{
	if (g_opts.adaptive_mode) {
		if (dd_should_print_progress()) {
			dd_show_progress_adaptive(&g_job, false);
		}
	} else {
		dd_show_progress(false);
	}

	if (g_opts.priority_mode == 1) {
		dd_print_queue_status(&g_job);
	}

	return SPDK_POLLER_BUSY;
}

static void
dd_show_progress_finish(void)
{
	if (g_opts.adaptive_mode) {
		dd_show_progress_adaptive(&g_job, true);
	} else {
		dd_show_progress(true);
	}

	printf("\n\n");
}

static void
dd_finalize_output(void)
{
	off_t curr_offset;
	int rc = 0;

	if (g_job.outstanding > 0) {
		return;
	}

	if (g_opts.output_file) {
		curr_offset = lseek(g_job.output.u.aio.fd, 0, SEEK_END);
		if (curr_offset == (off_t) -1) {
			SPDK_ERRLOG("Could not seek output file for finalize: %s\n", strerror(errno));
			g_error = errno;
		} else if ((uint64_t)curr_offset < g_job.copy_size + g_job.output.pos) {
			rc = ftruncate(g_job.output.u.aio.fd, g_job.copy_size + g_job.output.pos);
			if (rc != 0) {
				SPDK_ERRLOG("Could not truncate output file for finalize: %s\n", strerror(errno));
				g_error = errno;
			}
		}
	}

	if (g_error == 0) {
		dd_show_progress_finish();
	}
	dd_exit(g_error);
}

#ifdef SPDK_CONFIG_URING
static void
dd_uring_submit(struct dd_io *io, struct dd_target *target, uint64_t length, uint64_t offset)
{
	struct io_uring_sqe *sqe;

	sqe = io_uring_get_sqe(&g_job.u.uring.ring);
	if (io->type == DD_READ || io->type == DD_POPULATE) {
		io_uring_prep_read_fixed(sqe, target->u.uring.idx, io->buf, length, offset, io->idx);
	} else {
		io_uring_prep_write_fixed(sqe, target->u.uring.idx, io->buf, length, offset, io->idx);
	}
	sqe->flags |= IOSQE_FIXED_FILE;
	io_uring_sqe_set_data(sqe, io);
	io_uring_submit(&g_job.u.uring.ring);
}
#endif


/* ===================== SGL 管理函数（参考 perf.c）===================== */

/**
 * 为 dd_io 分配 SGL 链条
 * @param io: dd_io 结构
 * @param buf: 数据缓冲区指针
 * @param length: 总数据长度（字节）
 * @param wqe_size: 单个 WQE/iov 的大小
 * @return: 0 成功，-ENOMEM 分配失败
 */
/**
 * 为 dd_io 分配 SGL 链条 - 改进版
 */
static int
dd_allocate_sgl(struct dd_io *io, void *buf, uint32_t length, uint32_t wqe_size)
{
	int iovpos = 0;
	uint32_t offset = 0;
	void *tmp_addr = NULL;
	uint32_t tmp_len = 0;
	struct iovec *iov;

	if (wqe_size == 0) {
		fprintf(stderr, "ERROR: wqe_size is 0\n");
		return -EINVAL;
	}

	/* 计算需要的 iov 数量 */
	io->iovcnt = (length + wqe_size - 1) / wqe_size;  /* 等同于 CEIL_DIV */

	/* 分配 iovec 数组 */
	io->iovs = calloc(io->iovcnt, sizeof(struct iovec));
	if (!io->iovs) {
		fprintf(stderr, "Failed to allocate iov array (%d entries)\n", io->iovcnt);
		return -ENOMEM;
	}

	/* 填充每个 iovec */
	while (length > 0 && iovpos < io->iovcnt) {
		iov = &io->iovs[iovpos];
		iov->iov_len = (length > wqe_size) ? wqe_size : length;
		iov->iov_base = (void *)((uintptr_t)buf + offset);

		length -= iov->iov_len;
		offset += iov->iov_len;
		iovpos++;
	}

	/* 初始化位置指针 */
	io->iovpos = 0;
	io->iov_offset = 0;

	/* Touch first SGE once so helper path stays validated, then rewind to origin. */
	if (io->iovcnt > 0) {
		(void)dd_next_sge(io, &tmp_addr, &tmp_len);
		dd_reset_sgl(io, 0);
	}

	io->sgl_wqe_size = wqe_size;
	io->sgl_needs_rebuild = false;

	return 0;
}

/**
 * 释放 SGL 链条
 */
static void
dd_free_sgl(struct dd_io *io)
{
    if (io->iovs) {
        free(io->iovs);
        io->iovs = NULL;
        io->iovcnt = 0;
    }

	io->sgl_wqe_size = 0;
	io->sgl_needs_rebuild = false;
}

/**
 * 重置 SGL 到指定偏移量（用于续传）
 */
static void
dd_reset_sgl(struct dd_io *io, uint32_t sgl_offset)
{
    struct iovec *iov;

    io->iov_offset = sgl_offset;
    for (io->iovpos = 0; io->iovpos < io->iovcnt; io->iovpos++) {
        iov = &io->iovs[io->iovpos];
        if (io->iov_offset < iov->iov_len) {
            break;
        }
        io->iov_offset -= iov->iov_len;
    }
}

/**
 * 取下一个 SGE（Scatter-Gather Element）
 * @param io: dd_io 结构
 * @param address: 输出参数，地址
 * @param length: 输出参数，长度
 * @return: 0 成功，-1 已到末尾
 */
static int
dd_next_sge(struct dd_io *io, void **address, uint32_t *length)
{
    struct iovec *iov;

    if (io->iovpos >= io->iovcnt) {
        return -1;  /* 已处理完所有数据 */
    }

    iov = &io->iovs[io->iovpos];
    *address = (void *)((uintptr_t)iov->iov_base + io->iov_offset);
    *length = iov->iov_len - io->iov_offset;

    io->iovpos++;
    io->iov_offset = 0;

    return 0;
}

static int
dd_rebuild_io_sgl(struct dd_io *io, uint32_t wqe_size)
{
	int rc;

	if (!g_opts.adaptive_mode) {
		return 0;
	}

	if (wqe_size == 0) {
		return -EINVAL;
	}

	dd_free_sgl(io);
	rc = dd_allocate_sgl(io, io->buf, g_opts.io_unit_size, wqe_size);
	if (rc != 0) {
		return rc;
	}

	dd_reset_sgl(io, 0);
	return 0;
}

static const char *
dd_submit_type_to_str(enum dd_submit_type type)
{
	switch (type) {
	case DD_POPULATE:
		return "POPULATE";
	case DD_READ:
		return "READ";
	case DD_WRITE:
		return "WRITE";
	default:
		return "UNKNOWN";
	}
}

static const char *
dd_priority_to_str(enum dd_data_priority priority)
{
	switch (priority) {
	case DD_PRIO_HIGH:
		return "HIGH";
	case DD_PRIO_MEDIUM:
		return "MEDIUM";
	case DD_PRIO_LOW:
		return "LOW";
	default:
		return "UNKNOWN";
	}
}

static int
dd_parse_priority_arg(const char *arg, enum dd_data_priority *priority)
{
	long value;

	if (arg == NULL || priority == NULL) {
		return -EINVAL;
	}

	if (strcasecmp(arg, "low") == 0) {
		*priority = DD_PRIO_LOW;
		return 0;
	}

	if (strcasecmp(arg, "medium") == 0) {
		*priority = DD_PRIO_MEDIUM;
		return 0;
	}

	if (strcasecmp(arg, "high") == 0) {
		*priority = DD_PRIO_HIGH;
		return 0;
	}

	value = spdk_strtol(arg, 10);
	if (value < DD_PRIO_LOW || value > DD_PRIO_HIGH) {
		return -EINVAL;
	}

	*priority = (enum dd_data_priority)value;
	return 0;
}

static int
dd_parse_drop_policy_arg(const char *arg, enum dd_drop_policy *policy)
{
	if (arg == NULL || policy == NULL) {
		return -EINVAL;
	}

	if (strcasecmp(arg, "none") == 0 || strcmp(arg, "0") == 0) {
		*policy = DD_DROP_NONE;
		return 0;
	}

	if (strcasecmp(arg, "drop-oldest") == 0 ||
	    strcasecmp(arg, "drop_oldest") == 0 ||
	    strcmp(arg, "1") == 0) {
		*policy = DD_DROP_OLDEST;
		return 0;
	}

	return -EINVAL;
}

static const char *
dd_drop_policy_to_str(enum dd_drop_policy policy)
{
	switch (policy) {
	case DD_DROP_NONE:
		return "none";
	case DD_DROP_OLDEST:
		return "drop-oldest";
	default:
		return "unknown";
	}
}

static int
dd_decode_flow_entry(const struct spdk_json_val *val, void *out)
{
	struct dd_flow_entry_cfg *entry = out;
	static const struct spdk_json_object_decoder decoders[] = {
		{"input_offset", offsetof(struct dd_flow_entry_cfg, input_offset), spdk_json_decode_uint64, true},
		{"output_offset", offsetof(struct dd_flow_entry_cfg, output_offset), spdk_json_decode_uint64, true},
		{"io_unit_count", offsetof(struct dd_flow_entry_cfg, io_unit_count), spdk_json_decode_uint64, true},
		{"priority", offsetof(struct dd_flow_entry_cfg, priority), spdk_json_decode_string},
	};

	memset(entry, 0, sizeof(*entry));
	return spdk_json_decode_object(val, decoders, SPDK_COUNTOF(decoders), entry);
}

static int
dd_decode_flow_entries(const struct spdk_json_val *val, void *out)
{
	struct dd_flow_cfg *cfg = out;

	return spdk_json_decode_array(val, dd_decode_flow_entry, cfg->entries,
				      DD_MAX_FLOWS, &cfg->num_flows,
				      sizeof(cfg->entries[0]));
}

static void
dd_free_flow_cfg(struct dd_flow_cfg *cfg)
{
	size_t i;

	if (cfg == NULL) {
		return;
	}

	for (i = 0; i < cfg->num_flows; i++) {
		free(cfg->entries[i].priority);
		cfg->entries[i].priority = NULL;
	}

	cfg->num_flows = 0;
}

static int
dd_load_flow_cfg(const char *path, struct dd_flow_cfg *cfg)
{
	FILE *fp = NULL;
	struct spdk_json_val *values = NULL;
	void *end = NULL;
	char *buf = NULL;
	long file_size;
	ssize_t rc;
	size_t values_cnt;
	static const struct spdk_json_object_decoder cfg_decoders[] = {
		{"flows", 0, dd_decode_flow_entries},
	};

	if (path == NULL || cfg == NULL) {
		return -EINVAL;
	}

	memset(cfg, 0, sizeof(*cfg));

	fp = fopen(path, "r");
	if (fp == NULL) {
		SPDK_ERRLOG("Could not open flow config file %s: %s\n", path, strerror(errno));
		return -errno;
	}

	if (fseek(fp, 0, SEEK_END) != 0) {
		rc = -errno;
		SPDK_ERRLOG("Could not seek flow config file %s: %s\n", path, strerror(errno));
		goto out;
	}

	file_size = ftell(fp);
	if (file_size <= 0) {
		rc = -EINVAL;
		SPDK_ERRLOG("Invalid flow config file size for %s\n", path);
		goto out;
	}

	if (fseek(fp, 0, SEEK_SET) != 0) {
		rc = -errno;
		SPDK_ERRLOG("Could not rewind flow config file %s: %s\n", path, strerror(errno));
		goto out;
	}

	buf = calloc(1, (size_t)file_size + 1);
	if (buf == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	if (fread(buf, 1, (size_t)file_size, fp) != (size_t)file_size) {
		rc = -EIO;
		SPDK_ERRLOG("Could not read flow config file %s\n", path);
		goto out;
	}

	rc = spdk_json_parse(buf, (size_t)file_size, NULL, 0, &end,
			     SPDK_JSON_PARSE_FLAG_ALLOW_COMMENTS);
	if (rc <= 0) {
		SPDK_ERRLOG("Flow config JSON parse failed (%zd) for %s\n", rc, path);
		rc = -EINVAL;
		goto out;
	}

	values_cnt = (size_t)rc;
	values = calloc(values_cnt, sizeof(*values));
	if (values == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	rc = spdk_json_parse(buf, (size_t)file_size, values, values_cnt, &end,
			     SPDK_JSON_PARSE_FLAG_ALLOW_COMMENTS |
			     SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if ((size_t)rc != values_cnt) {
		SPDK_ERRLOG("Flow config JSON second parse failed (%zd) for %s\n", rc, path);
		rc = -EINVAL;
		goto out;
	}

	if (values[0].type == SPDK_JSON_VAL_ARRAY_BEGIN) {
		rc = dd_decode_flow_entries(values, cfg);
	} else if (values[0].type == SPDK_JSON_VAL_OBJECT_BEGIN) {
		rc = spdk_json_decode_object(values, cfg_decoders,
				     SPDK_COUNTOF(cfg_decoders), cfg);
	} else {
		rc = -EINVAL;
	}

	if (rc != 0 || cfg->num_flows == 0) {
		SPDK_ERRLOG("Flow config decode failed for %s\n", path);
		rc = -EINVAL;
		goto out;
	}

	rc = 0;

out:
	if (rc != 0) {
		dd_free_flow_cfg(cfg);
	}

	free(values);
	free(buf);
	if (fp) {
		fclose(fp);
	}

	return (int)rc;
}

static struct dd_flow *
dd_get_io_flow(struct dd_io *io)
{
	if (io != NULL && io->flow != NULL) {
		return io->flow;
	}

	if (g_job.flows != NULL && g_job.num_flows > 0) {
		return &g_job.flows[0];
	}

	return NULL;
}

static uint64_t
dd_stream_region_start(struct dd_flow *flow)
{
	if (flow == NULL) {
		return 0;
	}

	return flow->input_region_start;
}

static uint64_t
dd_stream_region_end(struct dd_flow *flow)
{
	uint64_t start;

	if (flow == NULL) {
		return 0;
	}

	start = dd_stream_region_start(flow);

	if (flow->copy_size > UINT64_MAX - start) {
		return UINT64_MAX;
	}

	return start + flow->copy_size;
}

static void
dd_stream_wrap_input_pos(struct dd_flow *flow)
{
	uint64_t start, end;

	if (!g_opts.stream_mode || flow == NULL || flow->copy_size == 0) {
		return;
	}

	start = dd_stream_region_start(flow);
	end = dd_stream_region_end(flow);

	if (flow->input_pos < start || flow->input_pos >= end) {
		flow->input_pos = start;
		g_stream_input_wraps++;
	}
}

static void
dd_stream_skip_one_chunk(struct dd_flow *flow)
{
	uint64_t start, read_offset, skip;

	if (!g_opts.stream_mode || flow == NULL || flow->copy_size == 0) {
		return;
	}

	dd_stream_wrap_input_pos(flow);
	start = dd_stream_region_start(flow);
	read_offset = flow->input_pos - start;
	skip = spdk_min((uint64_t)g_opts.io_unit_size, flow->copy_size);

	if (read_offset + skip >= flow->copy_size) {
		flow->input_pos = start;
		g_stream_input_wraps++;
	} else {
		flow->input_pos += skip;
	}

	g_stream_dropped_ios++;
}

static bool
dd_stream_should_drop_io(struct dd_io *io)
{
	uint64_t now_ticks;
	double wait_us;

	if (!g_opts.stream_mode || g_opts.drop_policy != DD_DROP_OLDEST ||
	    g_opts.frame_deadline_us == 0 || g_tsc_rate == 0 ||
	    io == NULL || io->enqueue_tsc == 0) {
		return false;
	}

	now_ticks = spdk_get_ticks();
	if (now_ticks <= io->enqueue_tsc) {
		return false;
	}

	wait_us = (double)(now_ticks - io->enqueue_tsc) * 1000000.0 / g_tsc_rate;
	return wait_us > (double)g_opts.frame_deadline_us;
}

static int
dd_get_io_index(struct dd_io *io)
{
	if (g_job.ios == NULL) {
		return -1;
	}

	if (io < g_job.ios || io >= g_job.ios + g_opts.queue_depth) {
		return -1;
	}

	return (int)(io - g_job.ios);
}

static void
dd_dump_io_sgl(struct dd_io *io, const char *stage)
{
	int io_idx;
	int i;

	if (!g_opts.adaptive_mode || !g_opts.dump_sgl || io == NULL || io->iovs == NULL || io->iovcnt <= 0) {
		return;
	}

	io_idx = dd_get_io_index(io);

	fprintf(stderr,
		"[SGL_DUMP] stage=%s io=%d type=%s prio=%s(%d) offset=%" PRIu64 " length=%" PRIu64 " wqe=%u iovcnt=%d\n",
		stage != NULL ? stage : "submit",
		io_idx,
		dd_submit_type_to_str(io->type),
		dd_priority_to_str(io->priority),
		(int)io->priority,
		io->offset,
		io->length,
		io->sgl_wqe_size,
		io->iovcnt);

	for (i = 0; i < io->iovcnt; i++) {
		fprintf(stderr,
			"[SGL_DUMP] io=%d iov[%d] base=%p len=%zu\n",
			io_idx,
			i,
			io->iovs[i].iov_base,
			io->iovs[i].iov_len);
	}
}

static void
dd_dump_io_priority(struct dd_io *io, const char *stage)
{
	int io_idx;

	if (!g_opts.dump_prio || io == NULL) {
		return;
	}

	io_idx = dd_get_io_index(io);

	fprintf(stderr,
		"[PRIO_DUMP] stage=%s io=%d type=%s prio=%s(%d) offset=%" PRIu64 " length=%" PRIu64 "\n",
		stage != NULL ? stage : "submit",
		io_idx,
		dd_submit_type_to_str(io->type),
		dd_priority_to_str(io->priority),
		(int)io->priority,
		io->offset,
		io->length);
}

static void
dd_refill_priority_credits(struct dd_job *job)
{
	job->prio_high_credits = DD_PRIORITY_HIGH_WEIGHT;
	job->prio_medium_credits = DD_PRIORITY_MEDIUM_WEIGHT;
	job->prio_low_credits = DD_PRIORITY_LOW_WEIGHT;
}

static void
dd_update_io_priority(struct dd_job *job, struct dd_io *io)
{
	(void)job;
	if (io == NULL) {
		return;
	}

	if (!g_opts.priority_mode) {
		/* No priority mode: collapse into a single FIFO queue. */
		io->priority = DD_PRIO_MEDIUM;
		return;
	}

	if (io->flow != NULL) {
		io->priority = io->flow->priority;
		return;
	}

	io->priority = (enum dd_data_priority)g_opts.stream_priority;
}


/* ===================== 优先级队列函数 ===================== */

/**
 * 按优先级将 IO 入队
 */
static void
dd_enqueue_io_prio(struct dd_job *job, struct dd_io *io)
{
	dd_update_io_priority(job, io);

    /* 记录入队时刻 */
    io->enqueue_tsc = spdk_get_ticks();

    switch (io->priority) {
    case DD_PRIO_HIGH:
        STAILQ_INSERT_TAIL(&job->high_prio_queue, io, link);
        break;
    case DD_PRIO_MEDIUM:
        STAILQ_INSERT_TAIL(&job->medium_prio_queue, io, link);
        break;
    case DD_PRIO_LOW:
    default:
        STAILQ_INSERT_TAIL(&job->low_prio_queue, io, link);
        break;
    }
}

/**
 * 按优先级出队（优先级高的先出）
 * @return: IO 指针，或 NULL 如果所有队列为空
 */
static struct dd_io*
dd_dequeue_io(struct dd_job *job)
{
    struct dd_io *io = NULL;
	uint32_t i;

	if (!g_opts.priority_mode) {
		if (!STAILQ_EMPTY(&job->medium_prio_queue)) {
			io = STAILQ_FIRST(&job->medium_prio_queue);
			STAILQ_REMOVE_HEAD(&job->medium_prio_queue, link);
			job->prio_dispatch_ios[DD_PRIO_MEDIUM]++;
			io->submit_tsc = spdk_get_ticks();
			return io;
		}

		if (!STAILQ_EMPTY(&job->high_prio_queue)) {
			io = STAILQ_FIRST(&job->high_prio_queue);
			STAILQ_REMOVE_HEAD(&job->high_prio_queue, link);
			job->prio_dispatch_ios[DD_PRIO_HIGH]++;
			io->submit_tsc = spdk_get_ticks();
			return io;
		}

		if (!STAILQ_EMPTY(&job->low_prio_queue)) {
			io = STAILQ_FIRST(&job->low_prio_queue);
			STAILQ_REMOVE_HEAD(&job->low_prio_queue, link);
			job->prio_dispatch_ios[DD_PRIO_LOW]++;
			io->submit_tsc = spdk_get_ticks();
			return io;
		}

		return NULL;
	}

	if (job->prio_high_credits == 0 && job->prio_medium_credits == 0 &&
	    job->prio_low_credits == 0) {
		dd_refill_priority_credits(job);
	}

	for (i = 0; i < 3; i++) {
		if (job->prio_high_credits > 0) {
			if (!STAILQ_EMPTY(&job->high_prio_queue)) {
				job->prio_high_credits--;
				io = STAILQ_FIRST(&job->high_prio_queue);
				STAILQ_REMOVE_HEAD(&job->high_prio_queue, link);
				job->prio_dispatch_ios[DD_PRIO_HIGH]++;
				io->submit_tsc = spdk_get_ticks();
				return io;
			}
			job->prio_high_credits = 0;
		}

		if (job->prio_medium_credits > 0) {
			if (!STAILQ_EMPTY(&job->medium_prio_queue)) {
				job->prio_medium_credits--;
				io = STAILQ_FIRST(&job->medium_prio_queue);
				STAILQ_REMOVE_HEAD(&job->medium_prio_queue, link);
				job->prio_dispatch_ios[DD_PRIO_MEDIUM]++;
				io->submit_tsc = spdk_get_ticks();
				return io;
			}
			job->prio_medium_credits = 0;
		}

		if (job->prio_low_credits > 0) {
			if (!STAILQ_EMPTY(&job->low_prio_queue)) {
				job->prio_low_credits--;
				io = STAILQ_FIRST(&job->low_prio_queue);
				STAILQ_REMOVE_HEAD(&job->low_prio_queue, link);
				job->prio_dispatch_ios[DD_PRIO_LOW]++;
				io->submit_tsc = spdk_get_ticks();
				return io;
			}
			job->prio_low_credits = 0;
		}

		dd_refill_priority_credits(job);
	}

    /* 优先检查高优先级队列 */
    if (!STAILQ_EMPTY(&job->high_prio_queue)) {
        io = STAILQ_FIRST(&job->high_prio_queue);
        STAILQ_REMOVE_HEAD(&job->high_prio_queue, link);
		job->prio_dispatch_ios[DD_PRIO_HIGH]++;
        io->submit_tsc = spdk_get_ticks();
        return io;
    }

    /* 然后是中优先级 */
    if (!STAILQ_EMPTY(&job->medium_prio_queue)) {
        io = STAILQ_FIRST(&job->medium_prio_queue);
        STAILQ_REMOVE_HEAD(&job->medium_prio_queue, link);
		job->prio_dispatch_ios[DD_PRIO_MEDIUM]++;
        io->submit_tsc = spdk_get_ticks();
        return io;
    }

    /* 最后是低优先级 */
    if (!STAILQ_EMPTY(&job->low_prio_queue)) {
        io = STAILQ_FIRST(&job->low_prio_queue);
        STAILQ_REMOVE_HEAD(&job->low_prio_queue, link);
		job->prio_dispatch_ios[DD_PRIO_LOW]++;
        io->submit_tsc = spdk_get_ticks();
        return io;
    }

    return NULL;
}

static void
dd_schedule_next_io(void)
{
	struct dd_io *next_io;
	int rc;

	if (g_job.outstanding >= g_opts.queue_depth) {
		return;
	}

	while (true) {
		next_io = dd_dequeue_io(&g_job);
		if (next_io == NULL) {
			return;
		}

		if (dd_stream_should_drop_io(next_io)) {
			dd_stream_skip_one_chunk(dd_get_io_flow(next_io));
			next_io->ready = true;
			dd_enqueue_io_prio(&g_job, next_io);
			continue;
		}

		break;
	}

	if (g_opts.adaptive_mode &&
	    (next_io->sgl_needs_rebuild || next_io->sgl_wqe_size != g_job.current_wqe_size)) {
		rc = dd_rebuild_io_sgl(next_io, g_job.current_wqe_size);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to rebuild SGL before submit: %d\n", rc);
			g_error = rc;
			dd_exit(rc);
			return;
		}
	}

	next_io->ready = false;
	dd_target_seek(next_io);
}

/**
 * 查询优先级队列状态（调试用）
 */
static void
dd_print_queue_status(struct dd_job *job)
{
    int high_cnt = 0, med_cnt = 0, low_cnt = 0;
    struct dd_io *io;

    STAILQ_FOREACH(io, &job->high_prio_queue, link) high_cnt++;
    STAILQ_FOREACH(io, &job->medium_prio_queue, link) med_cnt++;
    STAILQ_FOREACH(io, &job->low_prio_queue, link) low_cnt++;

		dd_progress_line_break();
	    fprintf(stderr, "[QUEUE] High:%d  Medium:%d  Low:%d  Outstanding:%u/%u\n",
		    high_cnt, med_cnt, low_cnt, job->outstanding, g_opts.queue_depth);
}

/* ===================== 性能采集与统计函数 ===================== */

/**
 * 采集单个 IO 的性能指标
 * 在 IO 完成时调用
 */
static void
dd_collect_metrics(struct dd_io *io, struct dd_job *job)
{
    uint64_t latency_ticks, wait_ticks;
	double wait_us;

    if (g_tsc_rate == 0) {
        return;
    }

    io->complete_tsc = spdk_get_ticks();

    /* 计算端到端延迟（从下发到完成） */
	if (io->submit_tsc > 0) {
		latency_ticks = io->complete_tsc - io->submit_tsc;
		io->last_latency_ticks = latency_ticks;
		if (g_latency_sample_count == g_latency_sample_capacity) {
			size_t new_capacity = g_latency_sample_capacity == 0 ? 1024 :
				g_latency_sample_capacity * 2;
			uint64_t *new_samples = realloc(g_latency_samples,
					new_capacity * sizeof(*new_samples));
			if (new_samples != NULL) {
				g_latency_samples = new_samples;
				g_latency_sample_capacity = new_capacity;
			}
		}
		if (g_latency_sample_count < g_latency_sample_capacity) {
			g_latency_samples[g_latency_sample_count++] = latency_ticks;
		}
        /* 使用指数移动平均更新 RTT */
        if (job->recent_rtt_ticks == 0) {
            job->recent_rtt_ticks = latency_ticks;
        } else {
            job->recent_rtt_ticks = (job->recent_rtt_ticks * 3 + latency_ticks) / 4;
        }
	} else {
		io->last_latency_ticks = 0;
    }

    /* 计算数据新鲜度（从入队到下发的等待时间） */
    if (io->enqueue_tsc > 0 && io->submit_tsc > io->enqueue_tsc) {
        wait_ticks = io->submit_tsc - io->enqueue_tsc;
        wait_us = (double)wait_ticks * 1000000.0 / g_tsc_rate;
        /* 滑动平均 */
        if (job->data_freshness_us == 0.0) {
            job->data_freshness_us = wait_us;
        } else {
            job->data_freshness_us = job->data_freshness_us * 0.9 + wait_us * 0.1;
        }
    }

	job->window_completed_ios++;
	job->total_completed_ios++;
}

/**
 * 估算网络指标（简化版）
 * 基于最近完成的 IO
 */
static void
dd_estimate_network_metrics(struct dd_job *job)
{
	uint64_t total_samples;

	total_samples = job->window_completed_ios + job->window_lost_ios;
	if (total_samples == 0) {
		job->recent_loss_rate = 0.0;
		return;
	}

	/* 计算丢失率：loss / (loss + success) */
	job->recent_loss_rate = (double)job->window_lost_ios / (double)total_samples;
}

static void
dd_record_io_loss(struct dd_job *job)
{
	job->window_lost_ios++;
	pthread_mutex_lock(&g_metrics_mutex);
	g_packets_lost++;
	pthread_mutex_unlock(&g_metrics_mutex);
}

static uint32_t
dd_align_wqe_size(uint32_t size)
{
	size = spdk_max(size, DD_MIN_WQE_SIZE);
	size = spdk_min(size, DD_MAX_WQE_SIZE);
	size = SPDK_CEIL_DIV(size, DD_WQE_STEP_SIZE) * DD_WQE_STEP_SIZE;

	return spdk_min(size, DD_MAX_WQE_SIZE);
}

/**
 * 打印性能报告
 */
static void
dd_print_performance_report(struct dd_job *job, struct timespec *start_time)
{
    struct timespec now;
    uint64_t elapsed_ms;
	uint64_t completed_bytes;
	uint64_t total_dispatch;
	double rtt_us;
	double p95_us = 0.0, p99_us = 0.0;
	double avg_io_kb = 0.0, io_per_sec = 0.0;
	uint64_t submitted_ios = 0;
	uint64_t failed_ios;
	uint64_t p95_index, p99_index;
	uint64_t *sorted_latencies;
	size_t i;

    clock_gettime(CLOCK_REALTIME, &now);
    
    elapsed_ms = (now.tv_sec - start_time->tv_sec) * 1000 +
                 (now.tv_nsec - start_time->tv_nsec) / 1000000;

    if (elapsed_ms == 0) {
        return;
    }

    if (g_tsc_rate == 0) {
        return;
    }

	completed_bytes = dd_get_completed_bytes();
	job->total_bytes = completed_bytes;
	dd_progress_line_break();

    /* 计算有效吞吐 */
	job->effective_throughput_mbps = (double)completed_bytes / elapsed_ms / 1024.0;
	submitted_ios = g_total_bytes_submitted / (g_opts.io_unit_size ? g_opts.io_unit_size : 1);
	failed_ios = g_packets_lost;
	if (g_latency_sample_count > 0) {
		sorted_latencies = malloc(g_latency_sample_count * sizeof(*sorted_latencies));
		if (sorted_latencies != NULL) {
			memcpy(sorted_latencies, g_latency_samples,
			       g_latency_sample_count * sizeof(*sorted_latencies));
			qsort(sorted_latencies, g_latency_sample_count,
			      sizeof(*sorted_latencies), dd_compare_latency_ticks);
			p95_index = (g_latency_sample_count * 95 + 99) / 100;
			p99_index = (g_latency_sample_count * 99 + 99) / 100;
			p95_index = p95_index == 0 ? 0 : p95_index - 1;
			p99_index = p99_index == 0 ? 0 : p99_index - 1;
			if (p95_index >= g_latency_sample_count) p95_index = g_latency_sample_count - 1;
			if (p99_index >= g_latency_sample_count) p99_index = g_latency_sample_count - 1;
			p95_us = (double)sorted_latencies[p95_index] * 1000000.0 / g_tsc_rate;
			p99_us = (double)sorted_latencies[p99_index] * 1000000.0 / g_tsc_rate;
			free(sorted_latencies);
		}
	}
	if (job->total_completed_ios > 0) {
		avg_io_kb = (double)completed_bytes / job->total_completed_ios / 1024.0;
	}
	if (elapsed_ms > 0) {
		io_per_sec = (double)job->total_completed_ios * 1000.0 / elapsed_ms;
	}

    /* 计算端到端延迟 */
    rtt_us = (double)job->recent_rtt_ticks * 1000000.0 / g_tsc_rate;
    job->avg_e2e_latency_us = rtt_us;

    /* 打印报告 */
    printf("\n========== Adaptive DD Performance Report ==========\n");
    printf("Elapsed Time:              %lu ms\n", elapsed_ms);
	printf("Total Bytes:               %lu bytes\n", completed_bytes);
    printf("Effective Throughput:      %.2f MB/s\n", job->effective_throughput_mbps);
	printf("I/O Rate:                  %.2f IO/s\n", io_per_sec);
	printf("Average I/O Size:          %.2f KB\n", avg_io_kb);
    printf("E2E Latency (avg):         %.2f us\n", job->avg_e2e_latency_us);
	printf("E2E Latency (P95):         %.2f us\n", p95_us);
	printf("E2E Latency (P99):         %.2f us\n", p99_us);
    printf("Data Freshness (wait):     %.2f us\n", job->data_freshness_us);
    printf("Recent RTT:                %.2f us\n", rtt_us);
    printf("Loss Rate:                 %.4f%%\n", job->recent_loss_rate * 100.0);
	printf("Completed IOs (window):    %" PRIu64 "\n", job->window_completed_ios);
	printf("Completed IOs (total):     %" PRIu64 "\n", job->total_completed_ios);
	printf("Submitted IOs:              %" PRIu64 "\n", submitted_ios);
	printf("Failed IOs:                 %" PRIu64 "\n", failed_ios);
	printf("Peak Outstanding IOs:       %" PRIu64 "\n", g_peak_outstanding);
	printf("Latency Samples:            %zu\n", g_latency_sample_count);
	if (g_wqe_size_samples > 0) {
		printf("WQE Size (min/avg/max):     %u / %.0f / %u bytes\n",
		       g_wqe_size_min,
		       (double)g_wqe_size_sum / g_wqe_size_samples,
		       g_wqe_size_max);
	}
	if (g_opts.input_dir != NULL) {
		printf("Packed Files:               %d\n", g_job.pack_file_count);
	}
	total_dispatch = job->prio_dispatch_ios[DD_PRIO_HIGH] +
		job->prio_dispatch_ios[DD_PRIO_MEDIUM] +
		job->prio_dispatch_ios[DD_PRIO_LOW];
	if (total_dispatch > 0) {
		printf("Priority dispatch:         H=%" PRIu64 " M=%" PRIu64 " L=%" PRIu64 "\n",
		       job->prio_dispatch_ios[DD_PRIO_HIGH],
		       job->prio_dispatch_ios[DD_PRIO_MEDIUM],
		       job->prio_dispatch_ios[DD_PRIO_LOW]);
		printf("Priority ratio:            %.2f : %.2f : %.2f\n",
		       (double)job->prio_dispatch_ios[DD_PRIO_HIGH] / total_dispatch,
		       (double)job->prio_dispatch_ios[DD_PRIO_MEDIUM] / total_dispatch,
		       (double)job->prio_dispatch_ios[DD_PRIO_LOW] / total_dispatch);
	}
	if (g_opts.stream_mode) {
		printf("Stream drop policy:        %s\n",
		       dd_drop_policy_to_str((enum dd_drop_policy)g_opts.drop_policy));
		printf("Stream dropped IOs:        %" PRIu64 "\n", g_stream_dropped_ios);
		printf("Stream input wraps:        %" PRIu64 "\n", g_stream_input_wraps);
	}
    printf("====================================================\n\n");
}

/**
 * 简化的吞吐显示（实时）
 */
static void
dd_show_progress_adaptive(struct dd_job *job, bool finish)
{
    char *unit_str[5] = {"", "k", "M", "G", "T"};
    uint64_t size_unit = 1;
	uint64_t completed_bytes;
	uint64_t now_tsc;
	uint64_t elapsed_ms;
	struct timespec now;
	static uint64_t last_tsc;
	static uint64_t last_completed_bytes;
    char *size_unit_str = "";
    int i = 0;

	completed_bytes = dd_get_completed_bytes();

	job->total_bytes = completed_bytes;

	now_tsc = spdk_get_ticks();
	if (g_tsc_rate != 0 && completed_bytes >= last_completed_bytes) {
		if (last_tsc != 0 && now_tsc > last_tsc) {
			double elapsed_s = (double)(now_tsc - last_tsc) / g_tsc_rate;

			if (elapsed_s > 0.0) {
				job->effective_throughput_mbps =
					(double)(completed_bytes - last_completed_bytes) / elapsed_s / (1024.0 * 1024.0);
			}
		} else {
			clock_gettime(CLOCK_REALTIME, &now);
			elapsed_ms = (now.tv_sec - job->start_time.tv_sec) * 1000 +
				(now.tv_nsec - job->start_time.tv_nsec) / 1000000;

			if (elapsed_ms > 0) {
				job->effective_throughput_mbps =
					(double)completed_bytes / ((double)elapsed_ms / 1000.0) / (1024.0 * 1024.0);
			}
		}
	}

	last_tsc = now_tsc;
	last_completed_bytes = completed_bytes;

	uint64_t tmp_size = completed_bytes;
    while (tmp_size > 1024 * 10) {
        tmp_size >>= 10;
        size_unit <<= 10;
        size_unit_str = unit_str[++i];
        if (i == 4) break;
    }

    if (finish) {
		dd_progress_line_break();
        dd_print_performance_report(job, &job->start_time);
    } else {
		fprintf(stderr, "\33[2K\rTransferred: %lu %sB | Throughput: %.2f MB/s",
			   completed_bytes / size_unit, size_unit_str,
               job->effective_throughput_mbps);
		fflush(stderr);
		g_progress_line_active = true;
    }
}


/* ===================== 自适应调度引擎 ===================== */

/**
 * 根据网络状态计算最优 WQE 大小
 * 参考 TCP 拥塞控制算法
 * 
 * 策略：
 *  - RTT 低 + 无丢包 → 增大 WQE（激进）
 *  - RTT 中 → 保持不变
 *  - RTT 高 + 丢包多 → 减小 WQE（保守）
 */
static uint32_t
dd_compute_optimal_wqe_size(struct dd_job *job)
{
    double rtt_us;
    uint32_t new_size;

	if (g_tsc_rate == 0 || job->recent_rtt_ticks == 0) {
		return job->current_wqe_size;
    }

    rtt_us = (double)job->recent_rtt_ticks * 1000000.0 / g_tsc_rate;
	new_size = job->current_wqe_size;

	dd_progress_line_break();
    fprintf(stderr, "[ADAPTIVE] RTT=%.2f us, Loss=%.2f%% -> ",
            rtt_us, job->recent_loss_rate * 100.0);

	/* 使用当前 WQE 做 AIMD 调整，避免来回抖动。 */
    if (rtt_us > 120000.0 || job->recent_loss_rate > 0.10) {
		new_size = spdk_max(DD_MIN_WQE_SIZE, new_size / 2);
        fprintf(stderr, "REDUCE_AGGRESSIVE (%u bytes)\n", new_size);
	} else if (rtt_us > 60000.0 || job->recent_loss_rate > 0.03) {
		new_size = spdk_max(DD_MIN_WQE_SIZE, new_size - DD_WQE_STEP_SIZE * 2);
        fprintf(stderr, "REDUCE (%u bytes)\n", new_size);
	} else if (rtt_us > 35000.0) {
		new_size = spdk_max(DD_MIN_WQE_SIZE, new_size - DD_WQE_STEP_SIZE);
        fprintf(stderr, "REDUCE_GENTLE (%u bytes)\n", new_size);
	} else if (rtt_us < 12000.0 && job->recent_loss_rate == 0.0) {
		new_size = spdk_min(DD_MAX_WQE_SIZE, new_size + DD_WQE_STEP_SIZE * 2);
		fprintf(stderr, "INCREASE_FAST (%u bytes)\n", new_size);
	} else if (rtt_us < 25000.0 && job->recent_loss_rate < 0.01) {
		new_size = spdk_min(DD_MAX_WQE_SIZE, new_size + DD_WQE_STEP_SIZE);
        fprintf(stderr, "INCREASE (%u bytes)\n", new_size);
	} else {
        fprintf(stderr, "STABLE (%u bytes)\n", new_size);
    }

	return dd_align_wqe_size(new_size);
}

/**
 * 更新 WQE 大小（周期性调用，约每 100ms）
 */
static void
dd_update_wqe_size(struct dd_job *job)
{
    uint64_t now_tsc = spdk_get_ticks();
    uint64_t elapsed_tsc = now_tsc - job->last_adapt_tsc;
	static uint32_t warmup_windows;
    double elapsed_ms;
    uint32_t optimal_size;

    if (g_tsc_rate == 0) {
        return;
    }

    elapsed_ms = (double)elapsed_tsc * 1000.0 / g_tsc_rate;

	/* 放宽调整周期，避免抖动 */
	if (elapsed_ms < DD_ADAPT_INTERVAL_MS) {
        return;
    }

    job->last_adapt_tsc = now_tsc;

	if (job->window_completed_ios < DD_ADAPT_MIN_COMPLETIONS) {
		dd_progress_line_break();
		fprintf(stderr, "[ADAPTIVE] SKIP: sample too small (%lu/%u)\n",
				job->window_completed_ios, DD_ADAPT_MIN_COMPLETIONS);
		return;
	}

    /* 估算网络指标 */
    dd_estimate_network_metrics(job);

	if (warmup_windows < DD_ADAPT_WARMUP_WINDOWS) {
		warmup_windows++;
		dd_progress_line_break();
		fprintf(stderr, "[ADAPTIVE] WARMUP: keep %u bytes (window %u/%u)\n",
				job->current_wqe_size, warmup_windows, DD_ADAPT_WARMUP_WINDOWS);
		job->window_completed_ios = 0;
		job->window_lost_ios = 0;
		return;
	}

    /* 计算最优大小 */
    optimal_size = dd_compute_optimal_wqe_size(job);

    /* 如果大小变化，需要重新分配 SGL */
    if (optimal_size != job->current_wqe_size && g_opts.adaptive_mode) {
        int rc;

		dd_progress_line_break();
        fprintf(stderr, "[WQE_UPDATE] Size changed: %u → %u bytes\n",
                job->current_wqe_size, optimal_size);
        job->current_wqe_size = optimal_size;
		g_adaptive_adjustments++;

		for (uint32_t i = 0; i < g_opts.queue_depth; i++) {
			struct dd_io *io = &job->ios[i];

			if (!io->ready) {
				io->sgl_needs_rebuild = true;
				continue;
			}

			rc = dd_rebuild_io_sgl(io, job->current_wqe_size);
			if (rc != 0) {
				SPDK_ERRLOG("Failed to rebuild SGL for IO %u\n", i);
				g_error = rc;
				dd_exit(rc);
				return;
			}
		}
    }

    /* 重置窗口计数器 */
    job->window_completed_ios = 0;
    job->window_lost_ios = 0;
}

static int
dd_adaptive_poller(void *ctx)
{
	dd_update_wqe_size(&g_job);
	return SPDK_POLLER_BUSY;
}

static void
_dd_write_bdev_done(struct spdk_bdev_io *bdev_io,
		    bool success,
		    void *cb_arg)
{
	struct dd_io *io = cb_arg;

	assert(g_job.outstanding > 0);
	g_job.outstanding--;
	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("bdev write I/O completion failed\n");
		dd_record_io_loss(&g_job);
		if (g_error == 0) {
			g_error = -EIO;
		}
		if (g_job.outstanding == 0) {
			dd_exit(g_error);
		}
		return;
	}

	/* 新增：采集性能指标 */
    dd_collect_metrics(io, &g_job);
	pthread_mutex_lock(&g_metrics_mutex);
	g_total_bytes_completed += io->length;
	pthread_mutex_unlock(&g_metrics_mutex);
	if (io->flow != NULL) {
		io->flow->completed_ios++;
	}

	io->ready = true;
	dd_enqueue_io_prio(&g_job, io);
	dd_schedule_next_io();
}

static void
dd_target_write(struct dd_io *io)
{
	struct dd_target *target = &g_job.output;
	struct dd_flow *flow = dd_get_io_flow(io);
	uint64_t length = SPDK_CEIL_DIV(io->length, target->block_size) * target->block_size;
	uint64_t read_region_start;
	uint64_t read_offset;
	uint64_t write_region_start;
	uint64_t write_offset;
	int rc = 0;

	/* pack 模式：io->offset 已是绝对 bdev 写偏移，无需 flow 映射 */
	if (g_opts.input_dir != NULL) {
		write_offset = io->offset;
		goto do_write;
	}

	if (flow == NULL) {
		g_error = -EINVAL;
		dd_exit(g_error);
		return;
	}

	read_region_start = flow->input_region_start;
	write_region_start = flow->output_region_start;
	read_offset = io->offset - read_region_start;
	write_offset = write_region_start + read_offset;

do_write:

	if (g_error != 0 || g_interrupt == true) {
		if (g_job.outstanding == 0) {
			if (g_error == 0) {
				dd_show_progress_finish();
			}
			dd_exit(g_error);
		}
		return;
	}

	g_job.incremental_bytes += io->length;
	if (flow != NULL) {
		flow->submitted_ios++;
	}
	g_job.outstanding++;
	if (g_job.outstanding > g_peak_outstanding) {
		g_peak_outstanding = g_job.outstanding;
	}
	if (g_job.current_wqe_size > 0) {
		g_wqe_size_sum += g_job.current_wqe_size;
		g_wqe_size_samples++;
		g_wqe_size_min = spdk_min(g_wqe_size_min, g_job.current_wqe_size);
		g_wqe_size_max = spdk_max(g_wqe_size_max, g_job.current_wqe_size);
	}
	io->type = DD_WRITE;
	dd_dump_io_priority(io, "write_submit");

	pthread_mutex_lock(&g_metrics_mutex);
	g_total_bytes_submitted += io->length;
	pthread_mutex_unlock(&g_metrics_mutex);

	if (target->type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			dd_uring_submit(io, target, length, write_offset);
		} else
#endif
		{
			struct iocb *iocb = &io->iocb;

			io_prep_pwrite(iocb, target->u.aio.fd, io->buf, length, write_offset);
			iocb->data = io;
			if (io_submit(g_job.u.aio.io_ctx, 1, &iocb) < 0) {
				rc = -errno;
			}
		}
	} else if (target->type == DD_TARGET_TYPE_BDEV) {
		if (g_opts.adaptive_mode && io->iovs && io->iovcnt > 0) {
			dd_dump_io_sgl(io, "writev");
			rc = spdk_bdev_writev(target->u.bdev.desc, target->u.bdev.ch,
					      io->iovs, io->iovcnt, write_offset, length,
					      _dd_write_bdev_done, io);
		} else {
			rc = spdk_bdev_write(target->u.bdev.desc, target->u.bdev.ch, io->buf, write_offset, length,
					     _dd_write_bdev_done, io);
		}
	}

	if (rc != 0) {
		SPDK_ERRLOG("%s\n", strerror(-rc));
		assert(g_job.outstanding > 0);
		g_job.outstanding--;
		g_error = rc;
		dd_record_io_loss(&g_job);
		if (g_job.outstanding == 0) {
			dd_exit(rc);
		}
		return;
	}
}

static void
_dd_read_bdev_done(struct spdk_bdev_io *bdev_io,
		   bool success,
		   void *cb_arg)
{
	struct dd_io *io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	assert(g_job.outstanding > 0);
	g_job.outstanding--;

	if (!success) {
		SPDK_ERRLOG("bdev read I/O completion failed\n");
		dd_record_io_loss(&g_job);
		if (g_error == 0) {
			g_error = -EIO;
		}
		if (g_job.outstanding == 0) {
			dd_exit(g_error);
		}
		return;
	}

	dd_target_write(io);
}

static void
dd_target_read(struct dd_io *io)
{
	struct dd_target *target = &g_job.input;
	int rc = 0;

	if (g_error != 0 || g_interrupt == true) {
		if (g_job.outstanding == 0) {
			dd_exit(g_error);
		}
		return;
	}

	g_job.outstanding++;
	io->type = DD_READ;
	dd_dump_io_priority(io, "read_submit");

	pthread_mutex_lock(&g_metrics_mutex);
	g_total_bytes_submitted += io->length;
	pthread_mutex_unlock(&g_metrics_mutex);

	if (target->type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			dd_uring_submit(io, target, io->length, io->offset);
		} else
#endif
		{
			struct iocb *iocb = &io->iocb;

			io_prep_pread(iocb, target->u.aio.fd, io->buf, io->length, io->offset);
			iocb->data = io;
			if (io_submit(g_job.u.aio.io_ctx, 1, &iocb) < 0) {
				rc = -errno;
			}
		}
	} else if (target->type == DD_TARGET_TYPE_BDEV) {
		if (g_opts.adaptive_mode && io->iovs && io->iovcnt > 0) {
			dd_dump_io_sgl(io, "readv");
			rc = spdk_bdev_readv(target->u.bdev.desc, target->u.bdev.ch,
					     io->iovs, io->iovcnt, io->offset, io->length,
					     _dd_read_bdev_done, io);
		} else {
			rc = spdk_bdev_read(target->u.bdev.desc, target->u.bdev.ch, io->buf, io->offset, io->length,
					    _dd_read_bdev_done, io);
		}
	}

	if (rc != 0) {
		SPDK_ERRLOG("%s\n", strerror(-rc));
		assert(g_job.outstanding > 0);
		g_job.outstanding--;
		g_error = rc;
		dd_record_io_loss(&g_job);
		if (g_job.outstanding == 0) {
			dd_exit(rc);
		}
		return;
	}
}

static void
_dd_target_populate_buffer_done(struct spdk_bdev_io *bdev_io,
				bool success,
				void *cb_arg)
{
	struct dd_io *io = cb_arg;

	assert(g_job.outstanding > 0);
	g_job.outstanding--;
	spdk_bdev_free_io(bdev_io);
	dd_target_read(io);
}

/* ===================== 小文件打包函数 ===================== */

/**
 * 扫描目录，返回所有普通文件的路径列表（调用者负责 free 列表及每个元素）
 * @param dirpath: 目录路径
 * @param out_count: 输出文件数量
 * @return: 文件路径数组，失败返回 NULL
 */
static char **
dd_scan_dir(const char *dirpath, int *out_count)
{
	DIR *dir;
	struct dirent *ent;
	struct stat st;
	char **list = NULL;
	char **tmp;
	int count = 0;
	int capacity = 64;
	char pathbuf[PATH_MAX];

	*out_count = 0;

	dir = opendir(dirpath);
	if (dir == NULL) {
		SPDK_ERRLOG("Cannot open input directory %s: %s\n", dirpath, strerror(errno));
		return NULL;
	}

	list = calloc(capacity, sizeof(char *));
	if (list == NULL) {
		closedir(dir);
		return NULL;
	}

	while ((ent = readdir(dir)) != NULL) {
		if (ent->d_name[0] == '.') {
			continue;  /* 跳过 . 和 .. 及隐藏文件 */
		}

		snprintf(pathbuf, sizeof(pathbuf), "%s/%s", dirpath, ent->d_name);

		if (stat(pathbuf, &st) != 0 || !S_ISREG(st.st_mode)) {
			continue;  /* 只处理普通文件 */
		}

		if (count >= capacity) {
			capacity *= 2;
			tmp = realloc(list, capacity * sizeof(char *));
			if (tmp == NULL) {
				break;
			}
			list = tmp;
		}

		list[count] = strdup(pathbuf);
		if (list[count] == NULL) {
			break;
		}
		count++;
	}

	closedir(dir);
	*out_count = count;

	if (count == 0) {
		free(list);
		return NULL;
	}

	/* 按文件名排序，保证确定性顺序 */
	qsort(list, count, sizeof(char *), dd_cmp_str);

	SPDK_NOTICELOG("Pack mode: found %d files in %s\n", count, dirpath);
	return list;
}

/**
 * 将多个小文件顺序打包填入 io->buf，直到 buf 填满或文件列表耗尽
 * 使用 g_job.pack_file_cursor / pack_file_inner_offset / pack_fd 跟踪进度
 * @return: 实际填入字节数（0 表示所有文件已处理完）
 */
static int
dd_collect_pack_files(const char *dirpath, char ***list, int *count, int *capacity)
{
	DIR *dir;
	struct dirent *ent;
	struct stat st;
	char pathbuf[PATH_MAX];
	char **tmp;

	dir = opendir(dirpath);
	if (dir == NULL) return -errno;
	while ((ent = readdir(dir)) != NULL) {
		if (ent->d_name[0] == '.') continue;
		if (snprintf(pathbuf, sizeof(pathbuf), "%s/%s", dirpath, ent->d_name) >= (int)sizeof(pathbuf)) continue;
		if (lstat(pathbuf, &st) != 0) continue;
		if (S_ISDIR(st.st_mode)) {
			if (dd_collect_pack_files(pathbuf, list, count, capacity) != 0) { closedir(dir); return -errno; }
		} else if (S_ISREG(st.st_mode)) {
			if (*count >= *capacity) {
				*capacity *= 2;
				tmp = realloc(*list, (size_t)*capacity * sizeof(char *));
				if (tmp == NULL) { closedir(dir); return -ENOMEM; }
				*list = tmp;
			}
			(*list)[*count] = strdup(pathbuf);
			if ((*list)[*count] == NULL) { closedir(dir); return -ENOMEM; }
			(*count)++;
		}
	}
	closedir(dir);
	return 0;
}

static char **
dd_scan_dir_recursive_root(const char *dirpath, int *out_count)
{
	char **list = calloc(64, sizeof(char *));
	int count = 0, capacity = 64;
	if (list == NULL || dd_collect_pack_files(dirpath, &list, &count, &capacity) != 0) {
		if (list) { for (int i = 0; i < count; i++) free(list[i]); free(list); }
		*out_count = 0;
		return NULL;
	}
	qsort(list, count, sizeof(char *), dd_cmp_str);
	*out_count = count;
	return count ? list : (free(list), NULL);
}

static int
dd_write_pack_index(const char *index_path, const char *dirpath, char **files, int file_count)
{
	FILE *fp;
	uint64_t offset = 0, total = 0;
	struct stat st;
	const char *rel;

	if (index_path == NULL) return 0;
	fp = fopen(index_path, "w");
	if (fp == NULL) return -errno;
	for (int i = 0; i < file_count; i++) {
		if (stat(files[i], &st) != 0) { fclose(fp); return -errno; }
		total += (uint64_t)st.st_size;
	}
	fprintf(fp, "# PACK_INDEX_V1\n# file_count=%d\n# total_size=%" PRIu64 "\n", file_count, total);
	for (int i = 0; i < file_count; i++) {
		if (stat(files[i], &st) != 0) { fclose(fp); return -errno; }
		rel = files[i] + strlen(dirpath);
		while (*rel == '/' || *rel == '\\') rel++;
		if (strchr(rel, '|') || strchr(rel, '\n')) { fclose(fp); return -EINVAL; }
		fprintf(fp, "%s|%" PRIu64 "|%" PRIu64 "\n", rel, offset, (uint64_t)st.st_size);
		offset += (uint64_t)st.st_size;
	}
	fclose(fp);
	SPDK_NOTICELOG("Pack index written: %s (%d files, %" PRIu64 " bytes)\n", index_path, file_count, total);
	return 0;
}

static uint64_t
dd_pack_files_into_buf(struct dd_io *io)
{
	uint8_t  *buf_ptr = (uint8_t *)io->buf;
	uint64_t  buf_cap = (uint64_t)g_opts.io_unit_size;
	uint64_t  filled = 0;
	ssize_t   n;

	while (filled < buf_cap && g_job.pack_file_cursor < g_job.pack_file_count) {
		/* 懒惰打开：pack_fd == -1 时才 open 当前文件 */
		if (g_job.pack_fd < 0) {
			g_job.pack_fd = open(g_job.pack_file_list[g_job.pack_file_cursor], O_RDONLY);
			if (g_job.pack_fd < 0) {
				SPDK_WARNLOG("Pack: cannot open %s, skipping\n",
					     g_job.pack_file_list[g_job.pack_file_cursor]);
				g_job.pack_file_cursor++;
				g_job.pack_file_inner_offset = 0;
				continue;
			}
		}

		n = pread(g_job.pack_fd,
			  buf_ptr + filled,
			  buf_cap - filled,
			  (off_t)g_job.pack_file_inner_offset);

		if (n < 0) {
			SPDK_WARNLOG("Pack: pread error on %s: %s, skipping\n",
				     g_job.pack_file_list[g_job.pack_file_cursor],
				     strerror(errno));
			close(g_job.pack_fd);
			g_job.pack_fd = -1;
			g_job.pack_file_cursor++;
			g_job.pack_file_inner_offset = 0;
			continue;
		}

		if (n == 0) {
			/* 文件已读完，推进到下一个 */
			close(g_job.pack_fd);
			g_job.pack_fd = -1;
			g_job.pack_file_cursor++;
			g_job.pack_file_inner_offset = 0;
			continue;
		}

		g_job.pack_file_inner_offset += (uint64_t)n;
		filled += (uint64_t)n;
	}

	return filled;
}

static void
dd_target_populate_buffer(struct dd_io *io)
{
	struct dd_target *target = &g_job.output;
	struct dd_flow *flow = dd_get_io_flow(io);
	uint64_t read_region_start;
	uint64_t read_offset;
	uint64_t write_region_start;
	uint64_t write_offset;
	uint64_t length;
	int rc = 0;

	/* ===== 小文件打包模式：同步填满 buf 后直接写入 bdev，跳过 bdev read 阶段 ===== */
	if (g_opts.input_dir != NULL) {
		if (g_error != 0 || g_interrupt == true) {
			if (g_job.outstanding == 0) {
				dd_exit(g_error);
			}
			return;
		}

		io->length = dd_pack_files_into_buf(io);

		if (io->length == 0) {
			/* 所有文件处理完毕 */
			if (g_job.outstanding == 0) {
				dd_show_progress_finish();
				dd_exit(0);
			}
			return;
		}

		/* 对齐到 bdev block size */
		io->length = SPDK_CEIL_DIV(io->length, g_job.output.block_size) *
			     g_job.output.block_size;

		io->offset = g_job.output.pos;
		g_job.output.pos += io->length;

		if (g_opts.adaptive_mode && io->iovs) {
			dd_reset_sgl(io, 0);
		}

		dd_target_write(io);
		return;
	}

	if (flow == NULL) {
		g_error = -EINVAL;
		dd_exit(g_error);
		return;
	}

	read_region_start = flow->input_region_start;
	write_region_start = flow->output_region_start;

	dd_stream_wrap_input_pos(flow);
	read_offset = flow->input_pos - read_region_start;
	write_offset = write_region_start + read_offset;

	io->offset = flow->input_pos;
	if (g_opts.stream_mode) {
		io->length = spdk_min((uint64_t)g_opts.io_unit_size, flow->copy_size);
		if (read_offset + io->length > flow->copy_size) {
			flow->input_pos = read_region_start;
			io->offset = flow->input_pos;
			read_offset = 0;
			write_offset = write_region_start;
			g_stream_input_wraps++;
		}
	} else {
		if (read_offset >= flow->copy_size) {
			io->length = 0;
		} else {
			io->length = spdk_min(io->length, flow->copy_size - read_offset);
		}
	}

	if (g_opts.adaptive_mode && io->iovs) {
		dd_reset_sgl(io, 0);
	}

	if (io->length == 0 || g_error != 0 || g_interrupt == true) {
		if (g_opts.stream_mode && g_error == 0 && g_interrupt == false) {
			io->ready = true;
			dd_enqueue_io_prio(&g_job, io);
			dd_schedule_next_io();
			return;
		}

		if (g_job.outstanding == 0) {
			if (g_error == 0) {
				dd_show_progress_finish();
			}
			dd_exit(g_error);
		}
		return;
	}

	flow->input_pos += io->length;
	dd_stream_wrap_input_pos(flow);
	g_job.input.pos = flow->input_pos;

	if ((io->length % target->block_size) == 0) {
		dd_target_read(io);
		return;
	}

	if (g_opts.stream_mode) {
		g_stream_dropped_ios++;
		io->ready = true;
		dd_enqueue_io_prio(&g_job, io);
		dd_schedule_next_io();
		return;
	}

	/* Read whole blocks from output to combine buffers later */
	g_job.outstanding++;
	io->type = DD_POPULATE;
	dd_dump_io_priority(io, "populate_submit");

	length = SPDK_CEIL_DIV(io->length, target->block_size) * target->block_size;

	if (target->type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			dd_uring_submit(io, target, length, write_offset);
		} else
#endif
		{
			struct iocb *iocb = &io->iocb;

			io_prep_pread(iocb, target->u.aio.fd, io->buf, length, write_offset);
			iocb->data = io;
			if (io_submit(g_job.u.aio.io_ctx, 1, &iocb) < 0) {
				rc = -errno;
			}
		}
	} else if (target->type == DD_TARGET_TYPE_BDEV) {
		rc = spdk_bdev_read(target->u.bdev.desc, target->u.bdev.ch, io->buf, write_offset, length,
				    _dd_target_populate_buffer_done, io);
	}

	if (rc != 0) {
		SPDK_ERRLOG("%s\n", strerror(-rc));
		assert(g_job.outstanding > 0);
		g_job.outstanding--;
		g_error = rc;
		if (g_job.outstanding == 0) {
			dd_exit(rc);
		}
		return;
	}
}

static off_t
dd_file_seek_data(uint64_t start_offset)
{
	off_t next_data_offset = (off_t) -1;

	next_data_offset = lseek(g_job.input.u.aio.fd, start_offset, SEEK_DATA);

	if (next_data_offset == (off_t) -1) {
		/* NXIO with SEEK_DATA means there are no more data to read.
		 * But in case of input and output files, we may have to finalize output file
		 * inserting a hole to the end of the file.
		 */
		if (errno == ENXIO) {
			dd_finalize_output();
		} else if (g_job.outstanding == 0) {
			SPDK_ERRLOG("Could not seek input file for data: %s\n", strerror(errno));
			g_error = errno;
			dd_exit(g_error);
		}
	}

	return next_data_offset;
}

static off_t
dd_file_seek_hole(uint64_t start_offset)
{
	off_t next_hole_offset = (off_t) -1;

	next_hole_offset = lseek(g_job.input.u.aio.fd, start_offset, SEEK_HOLE);

	if (next_hole_offset == (off_t) -1 && g_job.outstanding == 0) {
		SPDK_ERRLOG("Could not seek input file for hole: %s\n", strerror(errno));
		g_error = errno;
		dd_exit(g_error);
	}

	return next_hole_offset;
}

static void
_dd_bdev_seek_data_done(struct spdk_bdev_io *bdev_io,
			bool success,
			void *cb_arg)
{
	struct dd_io *io = cb_arg;
	struct dd_flow *flow = dd_get_io_flow(io);
	uint64_t next_data_offset_blocks = UINT64_MAX;
	struct dd_target *target = &g_job.input;
	int rc = 0;
	uint64_t next_data_offset;

	if (flow == NULL) {
		g_error = -EINVAL;
		dd_exit(g_error);
		return;
	}

	if (g_error != 0 || g_interrupt == true) {
		STAILQ_REMOVE_HEAD(&g_job.seek_queue, link);
		if (g_job.outstanding == 0) {
			if (g_error == 0) {
				dd_show_progress_finish();
			}
			dd_exit(g_error);
		}
		return;
	}

	assert(g_job.outstanding > 0);
	g_job.outstanding--;

	next_data_offset_blocks = spdk_bdev_io_get_seek_offset(bdev_io);
	spdk_bdev_free_io(bdev_io);

	/* UINT64_MAX means there are no more data to read.
	 * But in case of input and output files, we may have to finalize output file
	 * inserting a hole to the end of the file.
	 */
	if (next_data_offset_blocks == UINT64_MAX) {
		STAILQ_REMOVE_HEAD(&g_job.seek_queue, link);
		dd_finalize_output();
		return;
	}

	next_data_offset = next_data_offset_blocks * g_job.input.block_size;
	flow->input_pos = next_data_offset;
	g_job.input.pos = flow->input_pos;

	g_job.outstanding++;
	rc = spdk_bdev_seek_hole(target->u.bdev.desc, target->u.bdev.ch,
				 flow->input_pos / g_job.input.block_size,
				 _dd_bdev_seek_hole_done, io);

	if (rc != 0) {
		SPDK_ERRLOG("%s\n", strerror(-rc));
		STAILQ_REMOVE_HEAD(&g_job.seek_queue, link);
		assert(g_job.outstanding > 0);
		g_job.outstanding--;
		g_error = rc;
		if (g_job.outstanding == 0) {
			dd_exit(rc);
		}
	}
}

static void
_dd_bdev_seek_hole_done(struct spdk_bdev_io *bdev_io,
			bool success,
			void *cb_arg)
{
	struct dd_io *io = cb_arg;
	struct dd_flow *flow = dd_get_io_flow(io);
	struct dd_target *target = &g_job.input;
	uint64_t next_hole_offset_blocks = UINT64_MAX;
	struct dd_io *seek_io;
	int rc = 0;
	uint64_t flow_end;

	if (flow == NULL) {
		g_error = -EINVAL;
		dd_exit(g_error);
		return;
	}

	flow_end = dd_stream_region_end(flow);

	/* First seek operation is the one in progress, i.e. this one just ended */
	STAILQ_REMOVE_HEAD(&g_job.seek_queue, link);

	if (g_error != 0 || g_interrupt == true) {
		if (g_job.outstanding == 0) {
			if (g_error == 0) {
				dd_show_progress_finish();
			}
			dd_exit(g_error);
		}
		return;
	}

	assert(g_job.outstanding > 0);
	g_job.outstanding--;

	next_hole_offset_blocks = spdk_bdev_io_get_seek_offset(bdev_io);
	spdk_bdev_free_io(bdev_io);

	/* UINT64_MAX means there are no more holes. */
	if (next_hole_offset_blocks == UINT64_MAX) {
		io->length = g_opts.io_unit_size;
	} else if (next_hole_offset_blocks * g_job.input.block_size > flow->input_pos) {
		io->length = spdk_min((uint64_t)g_opts.io_unit_size,
				      next_hole_offset_blocks * g_job.input.block_size - flow->input_pos);
	} else {
		io->length = g_opts.io_unit_size;
	}

	dd_target_populate_buffer(io);
	g_job.input.pos = flow->input_pos;

	/* If input reading is not at the end, start following seek operation in the queue */
	if (!STAILQ_EMPTY(&g_job.seek_queue) && flow->input_pos < flow_end) {
		seek_io = STAILQ_FIRST(&g_job.seek_queue);
		assert(seek_io != NULL);
		g_job.outstanding++;
		rc = spdk_bdev_seek_data(target->u.bdev.desc, target->u.bdev.ch,
					 flow->input_pos / g_job.input.block_size,
					 _dd_bdev_seek_data_done, seek_io);

		if (rc != 0) {
			SPDK_ERRLOG("%s\n", strerror(-rc));
			assert(g_job.outstanding > 0);
			g_job.outstanding--;
			g_error = rc;
			if (g_job.outstanding == 0) {
				dd_exit(rc);
			}
		}
	}
}

static void
dd_target_seek(struct dd_io *io)
{
	struct dd_target *target = &g_job.input;
	struct dd_flow *flow = dd_get_io_flow(io);
	uint64_t read_region_start;
	uint64_t read_offset;
	off_t next_data_offset = (off_t) -1;
	off_t next_hole_offset = (off_t) -1;
	int rc = 0;

	if (flow == NULL) {
		g_error = -EINVAL;
		dd_exit(g_error);
		return;
	}

	read_region_start = flow->input_region_start;

	dd_stream_wrap_input_pos(flow);
	read_offset = flow->input_pos - read_region_start;
	g_job.input.pos = flow->input_pos;

	if (!g_opts.sparse) {
		dd_target_populate_buffer(io);
		return;
	}

	if ((!g_opts.stream_mode && read_offset >= flow->copy_size) ||
	    g_error != 0 || g_interrupt == true) {
		if (g_job.outstanding == 0) {
			if (g_error == 0) {
				dd_show_progress_finish();
			}
			dd_exit(g_error);
		}
		return;
	}

	if (target->type == DD_TARGET_TYPE_FILE) {
		next_data_offset = dd_file_seek_data(flow->input_pos);
		if (next_data_offset < 0) {
			return;
		} else if ((uint64_t)next_data_offset > flow->input_pos) {
			flow->input_pos = next_data_offset;
			g_job.input.pos = flow->input_pos;
		}

		next_hole_offset = dd_file_seek_hole(flow->input_pos);
		if (next_hole_offset < 0) {
			return;
		} else if ((uint64_t)next_hole_offset > flow->input_pos) {
			io->length = spdk_min((uint64_t)g_opts.io_unit_size,
					      (uint64_t)(next_hole_offset - flow->input_pos));
		} else {
			io->length = g_opts.io_unit_size;
		}

		dd_target_populate_buffer(io);
	} else if (target->type == DD_TARGET_TYPE_BDEV) {
		if (g_job.num_flows > 1) {
			g_error = -ENOTSUP;
			SPDK_ERRLOG("Sparse bdev seek is not supported in multi-flow mode\n");
			dd_exit(g_error);
			return;
		}

		/* Check if other seek operation is in progress */
		if (STAILQ_EMPTY(&g_job.seek_queue)) {
			g_job.outstanding++;
			rc = spdk_bdev_seek_data(target->u.bdev.desc, target->u.bdev.ch,
						 flow->input_pos / g_job.input.block_size,
						 _dd_bdev_seek_data_done, io);

		}

		STAILQ_INSERT_TAIL(&g_job.seek_queue, io, link);
	}

	if (rc != 0) {
		SPDK_ERRLOG("%s\n", strerror(-rc));
		assert(g_job.outstanding > 0);
		g_job.outstanding--;
		g_error = rc;
		if (g_job.outstanding == 0) {
			dd_exit(rc);
		}
		return;
	}
}

static void
dd_complete_poll(struct dd_io *io)
{
	assert(g_job.outstanding > 0);
	g_job.outstanding--;

	switch (io->type) {
	case DD_POPULATE:
		dd_target_read(io);
		break;
	case DD_READ:
		dd_target_write(io);
		break;
	case DD_WRITE:
		dd_collect_metrics(io, &g_job);
		pthread_mutex_lock(&g_metrics_mutex);
		g_total_bytes_completed += io->length;
		pthread_mutex_unlock(&g_metrics_mutex);
		if (io->flow != NULL) {
			io->flow->completed_ios++;
		}
		io->ready = true;
		dd_enqueue_io_prio(&g_job, io);
		dd_schedule_next_io();
		break;
	default:
		assert(false);
		break;
	}
}

#ifdef SPDK_CONFIG_URING
static int
dd_uring_poll(void *ctx)
{
	struct io_uring_cqe *cqe;
	struct dd_io *io;
	int rc = 0;
	int i;

	for (i = 0; i < (int)g_opts.queue_depth; i++) {
		rc = io_uring_peek_cqe(&g_job.u.uring.ring, &cqe);
		if (rc == 0) {
			if (cqe->res == -EAGAIN) {
				continue;
			} else if (cqe->res < 0) {
				SPDK_ERRLOG("%s\n", strerror(-cqe->res));
				g_error = cqe->res;
			}

			io = io_uring_cqe_get_data(cqe);
			io_uring_cqe_seen(&g_job.u.uring.ring, cqe);

			dd_complete_poll(io);
		} else if (rc != - EAGAIN) {
			SPDK_ERRLOG("%s\n", strerror(-rc));
			g_error = rc;
		}
	}

	return rc;
}
#endif

static int
dd_aio_poll(void *ctx)
{
	struct io_event events[32];
	int rc = 0;
	int i;
	struct timespec timeout;
	struct dd_io *io;

	timeout.tv_sec = 0;
	timeout.tv_nsec = 0;

	rc = io_getevents(g_job.u.aio.io_ctx, 0, 32, events, &timeout);

	if (rc < 0) {
		SPDK_ERRLOG("%s\n", strerror(-rc));
		dd_exit(rc);
	}

	for (i = 0; i < rc; i++) {
		io = events[i].data;
		if (events[i].res != io->length) {
			g_error = -ENOSPC;
		}

		dd_complete_poll(io);
	}

	return rc;
}

static int
dd_find_spdk_remote_nvme(char *path, size_t path_len)
{
	DIR *dir;
	struct dirent *ent;

	dir = opendir("/sys/block");
	if (dir == NULL) {
		return -errno;
	}

	while ((ent = readdir(dir)) != NULL) {
		char dev_path[PATH_MAX];
		char serial_path[PATH_MAX];
		char model_path[PATH_MAX];
		char serial[256] = {0};
		char model[256] = {0};
		FILE *fp;

		if (strncmp(ent->d_name, "nvme", 4) != 0 ||
		    strstr(ent->d_name, "n") == NULL) {
			continue;
		}

		snprintf(dev_path, sizeof(dev_path),
			 "/dev/%s", ent->d_name);

		snprintf(serial_path, sizeof(serial_path),
			 "/sys/block/%s/device/serial", ent->d_name);

		snprintf(model_path, sizeof(model_path),
			 "/sys/block/%s/device/model", ent->d_name);

		fp = fopen(serial_path, "r");
		if (fp != NULL) {
			fgets(serial, sizeof(serial), fp);
			fclose(fp);
		}

		fp = fopen(model_path, "r");
		if (fp != NULL) {
			fgets(model, sizeof(model), fp);
			fclose(fp);
		}

		/* Accept both the old export identity and the current real-NVMe export. */
		if (strstr(serial, "SPDK001") != NULL ||
		    strstr(serial, "SPDKTARGET001") != NULL ||
		    strstr(model, "SPDK bdev Controller") != NULL ||
		    strstr(model, "SPDK NVMe-oF Target") != NULL) {
			snprintf(path, path_len, "%s", dev_path);
			SPDK_NOTICELOG("Auto-detected remote SPDK NVMe namespace: %s (serial=%s, model=%s)\n",
			       path, serial, model);
			closedir(dir);
			return 0;
		}
	}

	closedir(dir);
	return -ENODEV;
}

static int
dd_open_file(struct dd_target *target, const char *fname, int flags, uint64_t skip_blocks,
	     bool input)
{
	int *fd;
	struct stat st;

#ifdef SPDK_CONFIG_URING
	if (g_opts.aio == false) {
		fd = &target->u.uring.fd;
	} else
#endif
	{
		fd = &target->u.aio.fd;
	}

	flags |= O_RDWR;

	/* A discovered NVMe-oF namespace is a block device. Do not create,
	 * truncate, or otherwise treat it like a regular output file. */
	if (stat(fname, &st) == 0 && S_ISBLK(st.st_mode)) {
		flags &= ~(O_CREAT | O_TRUNC);
	} else if (input == false && ((flags & O_DIRECTORY) == 0)) {
		flags |= O_CREAT;
		if ((flags & O_APPEND) == 0) {
			flags |= O_TRUNC;
		}
	}

	target->type = DD_TARGET_TYPE_FILE;
	*fd = open(fname, flags, 0600);
	if (*fd < 0) {
		SPDK_ERRLOG("Could not open file %s: %s\n", fname, strerror(errno));
		return *fd;
	}

	target->block_size = spdk_max(spdk_fd_get_blocklen(*fd), 1);

	target->total_size = spdk_fd_get_size(*fd);
	if (target->total_size == 0) {
		target->total_size = g_opts.io_unit_size * g_opts.io_unit_count;
	}

	if (input == true) {
		g_opts.queue_depth = spdk_min(g_opts.queue_depth,
					      (target->total_size / g_opts.io_unit_size) - skip_blocks + 1);
	}

	if (g_opts.io_unit_count != 0) {
		g_opts.queue_depth = spdk_min(g_opts.queue_depth, g_opts.io_unit_count);
	}

	return 0;
}

static void
dd_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		 void *event_ctx)
{
	SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static int
dd_open_bdev(struct dd_target *target, const char *bdev_name, uint64_t skip_blocks)
{
	int rc;

	target->type = DD_TARGET_TYPE_BDEV;

	rc = spdk_bdev_open_ext(bdev_name, true, dd_bdev_event_cb, NULL, &target->u.bdev.desc);
	if (rc < 0) {
		SPDK_ERRLOG("Could not open bdev %s: %s\n", bdev_name, strerror(-rc));
		return rc;
	}

	target->u.bdev.bdev = spdk_bdev_desc_get_bdev(target->u.bdev.desc);
	target->open = true;

	target->u.bdev.ch = spdk_bdev_get_io_channel(target->u.bdev.desc);
	if (target->u.bdev.ch == NULL) {
		spdk_bdev_close(target->u.bdev.desc);
		SPDK_ERRLOG("Could not get I/O channel: %s\n", strerror(ENOMEM));
		return -ENOMEM;
	}

	target->block_size = spdk_bdev_get_block_size(target->u.bdev.bdev);
	target->total_size = spdk_bdev_get_num_blocks(target->u.bdev.bdev) * target->block_size;

	g_opts.queue_depth = spdk_min(g_opts.queue_depth,
				      (target->total_size / g_opts.io_unit_size) - skip_blocks + 1);

	if (g_opts.io_unit_count != 0) {
		g_opts.queue_depth = spdk_min(g_opts.queue_depth, g_opts.io_unit_count);
	}

	return 0;
}

static void
dd_finish(void)
{
	/* Interrupt operation */
	g_interrupt = true;
}

static int
parse_flags(char *file_flags)
{
	char *input_flag;
	int flags = 0;
	int i;
	bool found = false;

	/* Translate input flags to file open flags */
	while ((input_flag = strsep(&file_flags, ","))) {
		for (i = 0; g_flags[i].name != NULL; i++) {
			if (!strcmp(input_flag, g_flags[i].name)) {
				flags |= g_flags[i].flag;
				found = true;
				break;
			}
		}

		if (found == false) {
			SPDK_ERRLOG("Unknown file flag: %s\n", input_flag);
			dd_exit(-EINVAL);
			return 0;
		}

		found = false;
	}

	return flags;
}

#ifdef SPDK_CONFIG_URING
static bool
dd_is_blk(int fd)
{
	struct stat st;

	if (fstat(fd, &st) != 0) {
		return false;
	}

	return S_ISBLK(st.st_mode);
}

struct dd_uring_init_ctx {
	unsigned int io_uring_flags;
	int rc;
};

static void *
dd_uring_init(void *arg)
{
	struct dd_uring_init_ctx *ctx = arg;

	ctx->rc = io_uring_queue_init(g_opts.queue_depth * 2, &g_job.u.uring.ring, ctx->io_uring_flags);
	return ctx;
}

static int
dd_register_files(void)
{
	int fds[2];
	unsigned count = 0;

	if (g_opts.input_file) {
		fds[count] = g_job.input.u.uring.fd;
		g_job.input.u.uring.idx = count;
		count++;
	}

	if (g_opts.output_file) {
		fds[count] = g_job.output.u.uring.fd;
		g_job.output.u.uring.idx = count;
		count++;
	}

	return io_uring_register_files(&g_job.u.uring.ring, fds, count);

}

static int
dd_register_buffers(void)
{
	struct iovec *iovs;
	int i, rc;

	iovs = calloc(g_opts.queue_depth, sizeof(struct iovec));
	if (iovs == NULL) {
		return -ENOMEM;
	}

	for (i = 0; i < (int)g_opts.queue_depth; i++) {
		iovs[i].iov_base = g_job.ios[i].buf;
		iovs[i].iov_len = g_opts.io_unit_size;
		g_job.ios[i].idx = i;
	}

	rc = io_uring_register_buffers(&g_job.u.uring.ring, iovs, g_opts.queue_depth);

	free(iovs);
	return rc;
}
#endif



/**
 * 判断是否应该打印进度（简化版）
 */
static bool
dd_should_print_progress(void)
{
	static uint64_t last_print_tsc = 0;
	uint64_t now_tsc = spdk_get_ticks();
	double elapsed_ms;

	if (last_print_tsc == 0) {
		last_print_tsc = now_tsc;
		return false;
	}

	elapsed_ms = (double)(now_tsc - last_print_tsc) * 1000.0 / g_tsc_rate;

	if (elapsed_ms > 1000.0) {  /* 每 1 秒打印一次 */
		last_print_tsc = now_tsc;
		return true;
	}

	return false;
}

static int
dd_setup_flows(void)
{
	struct dd_flow_cfg cfg = {};
	uint32_t num_flows = 1;
	bool has_cfg = g_opts.flow_config_file != NULL;
	uint32_t i;
	int rc = 0;

	/* pack 模式没有 input bdev/file，不需要 flow 机制 */
	if (g_opts.input_dir != NULL) {
		g_job.flows = calloc(1, sizeof(*g_job.flows));
		if (g_job.flows == NULL) {
			return -ENOMEM;
		}
		g_job.num_flows = 1;
		g_job.flows[0].priority = (enum dd_data_priority)g_opts.stream_priority;
		g_job.copy_size = g_job.output.total_size;  /* 上限设为 output 大小 */
		return 0;
	}

	free(g_job.flows);
	g_job.flows = NULL;
	g_job.num_flows = 0;

	if (has_cfg) {
		rc = dd_load_flow_cfg(g_opts.flow_config_file, &cfg);
		if (rc != 0) {
			return rc;
		}

		num_flows = (uint32_t)cfg.num_flows;
	}

	g_job.flows = calloc(num_flows, sizeof(*g_job.flows));
	if (g_job.flows == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	g_job.num_flows = num_flows;

	for (i = 0; i < num_flows; i++) {
		struct dd_flow *flow = &g_job.flows[i];
		uint64_t input_offset_blocks;
		uint64_t output_offset_blocks;
		uint64_t io_unit_count;
		uint64_t input_start;
		uint64_t output_start;
		uint64_t copy_size;
		enum dd_data_priority priority = (enum dd_data_priority)g_opts.stream_priority;

		if (has_cfg) {
			input_offset_blocks = cfg.entries[i].input_offset;
			output_offset_blocks = cfg.entries[i].output_offset;
			io_unit_count = cfg.entries[i].io_unit_count;

			if (dd_parse_priority_arg(cfg.entries[i].priority, &priority) != 0) {
				SPDK_ERRLOG("Invalid flow priority at index %u\n", i);
				rc = -EINVAL;
				goto out;
			}
		} else {
			input_offset_blocks = g_opts.input_offset;
			output_offset_blocks = g_opts.output_offset;
			io_unit_count = (uint64_t)g_opts.io_unit_count;
		}

		if (input_offset_blocks > UINT64_MAX / (uint64_t)g_opts.io_unit_size ||
		    output_offset_blocks > UINT64_MAX / (uint64_t)g_opts.io_unit_size) {
			SPDK_ERRLOG("Flow offset overflow at index %u\n", i);
			rc = -EINVAL;
			goto out;
		}

		input_start = input_offset_blocks * (uint64_t)g_opts.io_unit_size;
		output_start = output_offset_blocks * (uint64_t)g_opts.io_unit_size;

		if (input_start >= g_job.input.total_size) {
			SPDK_ERRLOG("Flow input offset out of range at index %u\n", i);
			rc = -EINVAL;
			goto out;
		}

		if (io_unit_count == 0) {
			copy_size = g_job.input.total_size - input_start;
			if (g_job.input.type != DD_TARGET_TYPE_FILE) {
				copy_size = (copy_size / (uint64_t)g_opts.io_unit_size) * (uint64_t)g_opts.io_unit_size;
			}
		} else {
			if (io_unit_count > UINT64_MAX / (uint64_t)g_opts.io_unit_size) {
				SPDK_ERRLOG("Flow io_unit_count overflow at index %u\n", i);
				rc = -EINVAL;
				goto out;
			}

			copy_size = io_unit_count * (uint64_t)g_opts.io_unit_size;
		}

		if (copy_size > g_job.input.total_size - input_start) {
			SPDK_ERRLOG("Flow input range out of bounds at index %u\n", i);
			rc = -ENOSPC;
			goto out;
		}

		if (copy_size == 0) {
			SPDK_ERRLOG("Flow copy_size is zero at index %u\n", i);
			rc = -EINVAL;
			goto out;
		}

		if (g_job.output.type == DD_TARGET_TYPE_BDEV) {
			uint64_t required_size;

			required_size = SPDK_CEIL_DIV(copy_size, g_job.output.block_size) * g_job.output.block_size;
			if (output_start > g_job.output.total_size ||
			    required_size > g_job.output.total_size - output_start) {
				SPDK_ERRLOG("Flow output range out of bdev bounds at index %u\n", i);
				rc = -ENOSPC;
				goto out;
			}
		}

		flow->input_region_start = input_start;
		flow->output_region_start = output_start;
		flow->copy_size = copy_size;
		flow->input_pos = input_start;
		flow->priority = priority;
	}

	g_job.copy_size = g_job.flows[0].copy_size;
	g_job.input.pos = g_job.flows[0].input_pos;

	if (g_opts.queue_depth < g_job.num_flows) {
		SPDK_NOTICELOG("Increasing queue depth from %u to %u to cover all flows\n",
			       g_opts.queue_depth, g_job.num_flows);
		g_opts.queue_depth = g_job.num_flows;
	}

out:
	if (rc != 0) {
		free(g_job.flows);
		g_job.flows = NULL;
		g_job.num_flows = 0;
	}

	dd_free_flow_cfg(&cfg);
	return rc;
}

static void
dd_run(void *arg1)
{
	uint64_t write_size;
	uint32_t i;
	int rc, flags = 0;

	/* ✅ 新增：初始化三个优先级队列 */
	STAILQ_INIT(&g_job.high_prio_queue);
	STAILQ_INIT(&g_job.medium_prio_queue);
	STAILQ_INIT(&g_job.low_prio_queue);
	dd_refill_priority_credits(&g_job);

	/* ✅ 新增：初始化 TSC 相关变量 */
	g_tsc_rate = spdk_get_ticks_hz();
	if (g_tsc_rate == 0) {
		fprintf(stderr, "Warning: Unable to get TSC rate, using default 2.4 GHz\n");
		g_tsc_rate = 2400000000UL;  // 默认 2.4 GHz
	}
	printf("[INIT] TSC Rate: %lu Hz (%.2f GHz)\n", g_tsc_rate, (double)g_tsc_rate / 1e9);
	printf("[INIT] Fixed stream priority: %s(%u)\n",
	       dd_priority_to_str((enum dd_data_priority)g_opts.stream_priority),
	       g_opts.stream_priority);
	printf("[INIT] Stream mode: %s, frame_deadline_us=%" PRIu64 ", drop_policy=%s\n",
	       g_opts.stream_mode ? "on" : "off",
	       g_opts.frame_deadline_us,
	       dd_drop_policy_to_str((enum dd_drop_policy)g_opts.drop_policy));

	/* ===== 原有的文件/bdev 打开逻辑 ===== */
	if (g_opts.input_dir != NULL) {
		/* pack 模式：没有 input bdev/file，input target 留空，
		 * block_size 设为 1 防止后续 CEIL_DIV 除零 */
		g_job.input.block_size = 1;
		g_job.input.total_size = 0;
		g_job.input.pos = 0;
	} else if (g_opts.input_file) {
		if (g_opts.input_file_flags) {
			flags = parse_flags(g_opts.input_file_flags);
		}

		if (dd_open_file(&g_job.input, g_opts.input_file, flags, g_opts.input_offset, true) < 0) {
			SPDK_ERRLOG("%s: %s\n", g_opts.input_file, strerror(errno));
			dd_exit(-errno);
			return;
		}
	} else if (g_opts.input_bdev) {
		rc = dd_open_bdev(&g_job.input, g_opts.input_bdev, g_opts.input_offset);
		if (rc < 0) {
			SPDK_ERRLOG("%s: %s\n", g_opts.input_bdev, strerror(-rc));
			dd_exit(rc);
			return;
		}
	}

	write_size = g_opts.io_unit_count * g_opts.io_unit_size;
	g_job.input.pos = g_opts.input_offset * g_opts.io_unit_size;

	if (g_opts.input_bdev && g_job.input.pos > g_job.input.total_size) {
		SPDK_ERRLOG("--skip value too big (%" PRIu64 ") - only %" PRIu64 " blocks available in input\n",
			    g_opts.input_offset, g_job.input.total_size / g_opts.io_unit_size);
		dd_exit(-ENOSPC);
		return;
	}

	if (g_opts.io_unit_count != 0 && g_opts.input_bdev &&
	    write_size + g_job.input.pos > g_job.input.total_size) {
		SPDK_ERRLOG("--count value too big (%" PRIu64 ") - only %" PRIu64 " blocks available from input\n",
			    g_opts.io_unit_count, (g_job.input.total_size - g_job.input.pos) / g_opts.io_unit_size);
		dd_exit(-ENOSPC);
		return;
	}

	if (g_opts.io_unit_count != 0) {
		g_job.copy_size = write_size;
	} else {
		g_job.copy_size = g_job.input.total_size - g_job.input.pos;
	}

	g_job.output.pos = g_opts.output_offset * g_opts.io_unit_size;

	if (g_opts.output_file) {
		flags = 0;

		if (g_opts.output_file_flags) {
			flags = parse_flags(g_opts.output_file_flags);
		}

		if (dd_open_file(&g_job.output, g_opts.output_file, flags, g_opts.output_offset, false) < 0) {
			SPDK_ERRLOG("%s: %s\n", g_opts.output_file, strerror(errno));
			dd_exit(-errno);
			return;
		}
	} else if (g_opts.output_bdev) {
		rc = dd_open_bdev(&g_job.output, g_opts.output_bdev, g_opts.output_offset);
		if (rc < 0) {
			SPDK_ERRLOG("%s: %s\n", g_opts.output_bdev, strerror(-rc));
			dd_exit(rc);
			return;
		}

		if (g_job.output.pos > g_job.output.total_size) {
			SPDK_ERRLOG("--seek value too big (%" PRIu64 ") - only %" PRIu64 " blocks available in output\n",
				    g_opts.output_offset, g_job.output.total_size / g_opts.io_unit_size);
			dd_exit(-ENOSPC);
			return;
		}

		if (g_opts.io_unit_count != 0 && write_size + g_job.output.pos > g_job.output.total_size) {
			SPDK_ERRLOG("--count value too big (%" PRIu64 ") - only %" PRIu64 " blocks available in output\n",
				    g_opts.io_unit_count, (g_job.output.total_size - g_job.output.pos) / g_opts.io_unit_size);
			dd_exit(-ENOSPC);
			return;
		}
	}

	if ((g_job.output.block_size > g_opts.io_unit_size) ||
	    (g_job.input.block_size > g_opts.io_unit_size)) {
		SPDK_ERRLOG("--bs value cannot be less than input (%d) neither output (%d) native block size\n",
			    g_job.input.block_size, g_job.output.block_size);
		dd_exit(-EINVAL);
		return;
	}

	if (g_opts.input_bdev && g_opts.io_unit_size % g_job.input.block_size != 0) {
		SPDK_ERRLOG("--bs value must be a multiple of input native block size (%d)\n",
			    g_job.input.block_size);
		dd_exit(-EINVAL);
		return;
	}

	if (g_opts.stream_mode) {
		if (g_opts.sparse) {
			SPDK_ERRLOG("--stream mode does not support --sparse\n");
			dd_exit(-EINVAL);
			return;
		}

		if (g_job.copy_size == 0) {
			SPDK_ERRLOG("--stream mode requires non-zero input region\n");
			dd_exit(-EINVAL);
			return;
		}

		if ((g_opts.io_unit_size % g_job.input.block_size) != 0 ||
		    (g_opts.io_unit_size % g_job.output.block_size) != 0) {
			SPDK_ERRLOG("--stream mode requires --bs to be a multiple of input/output block sizes (%u/%u)\n",
				    g_job.input.block_size, g_job.output.block_size);
			dd_exit(-EINVAL);
			return;
		}
	}

	rc = dd_setup_flows();
	if (rc != 0) {
		dd_exit(rc);
		return;
	}

	/* ===== pack 模式：扫描目录，初始化文件列表 ===== */
	if (g_opts.input_dir != NULL) {
		g_job.pack_file_list = dd_scan_dir_recursive_root(g_opts.input_dir, &g_job.pack_file_count);
		if (g_job.pack_file_list == NULL || g_job.pack_file_count == 0) {
			SPDK_ERRLOG("No files found in input directory %s\n", g_opts.input_dir);
			dd_exit(-ENOENT);
			return;
		}
		g_job.pack_file_cursor = 0;
		g_job.pack_file_inner_offset = 0;
		g_job.pack_fd = -1;
		rc = dd_write_pack_index(g_opts.index_file, g_opts.input_dir,
					  g_job.pack_file_list, g_job.pack_file_count);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to write pack index: %d\n", rc);
			dd_exit(rc);
			return;
		}
		printf("[PACK] Will merge %d files from %s into bdev (bs=%ld)\n",
		       g_job.pack_file_count, g_opts.input_dir, g_opts.io_unit_size);
	}

	if (g_opts.flow_config_file != NULL && !g_opts.stream_mode) {
		SPDK_ERRLOG("--flow-config currently requires --stream=1\n");
		dd_exit(-EINVAL);
		return;
	}

	if (g_opts.sparse && g_job.num_flows > 1) {
		SPDK_ERRLOG("--sparse is not supported with multi-flow mode\n");
		dd_exit(-ENOTSUP);
		return;
	}

	if (g_opts.adaptive_mode && g_opts.io_unit_size < DD_MIN_WQE_SIZE) {
		SPDK_NOTICELOG("Disabling adaptive mode: --bs (%" PRId64 ") is smaller than minimum adaptive WQE size (%u)\n",
			       g_opts.io_unit_size, DD_MIN_WQE_SIZE);
		g_opts.adaptive_mode = false;
	}

	g_job.base_wqe_size = dd_align_wqe_size(spdk_min((uint32_t)g_opts.io_unit_size, DD_MAX_WQE_SIZE));
	g_job.current_wqe_size = g_job.base_wqe_size;

	/* ===== 分配 IO 缓冲区和 SGL ===== */
	g_job.ios = calloc(g_opts.queue_depth, sizeof(struct dd_io));
	if (g_job.ios == NULL) {
		SPDK_ERRLOG("%s\n", strerror(ENOMEM));
		dd_exit(-ENOMEM);
		return;
	}

	for (i = 0; i < g_opts.queue_depth; i++) {
		g_job.ios[i].buf = spdk_malloc(g_opts.io_unit_size, 0x1000, NULL, 0, SPDK_MALLOC_DMA);
		if (g_job.ios[i].buf == NULL) {
			SPDK_ERRLOG("%s - try smaller block size value\n", strerror(ENOMEM));
			dd_exit(-ENOMEM);
			return;
		}
		g_job.ios[i].length = (uint64_t)g_opts.io_unit_size;
		g_job.ios[i].last_latency_ticks = 0;
		g_job.ios[i].sgl_needs_rebuild = false;
		g_job.ios[i].sgl_wqe_size = 0;
		g_job.ios[i].flow_idx = i % g_job.num_flows;
		g_job.ios[i].flow = &g_job.flows[g_job.ios[i].flow_idx];

		/* ✅ 新增：使��当前 WQE 大小初始化 SGL */
		if (g_opts.adaptive_mode) {
			rc = dd_allocate_sgl(&g_job.ios[i], g_job.ios[i].buf,
					     g_opts.io_unit_size,
					     g_job.current_wqe_size);
			if (rc != 0) {
				SPDK_ERRLOG("Failed to allocate SGL for IO %u\n", i);
				dd_exit(rc);
				return;
			}
			if (i == 0) {
				printf("[INIT] SGL: %d entries per IO, WQE size %u bytes\n",
				       g_job.ios[i].iovcnt, g_job.current_wqe_size);
			}
		}

		/* 每个 IO 绑定流后，优先级由流指定并在入队时刷新 */
		g_job.ios[i].priority = g_job.ios[i].flow->priority;
		g_job.ios[i].ready = false;
	}

	/* ===== 初始化 aio/uring ===== */
	if (g_opts.input_file || g_opts.output_file) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			struct dd_uring_init_ctx ctx;
			int flags = 0;

			if (g_opts.input_file_flags) {
				flags |= parse_flags(g_opts.input_file_flags);
			}
			if (g_opts.output_file_flags) {
				flags |= parse_flags(g_opts.output_file_flags);
			}

			ctx.io_uring_flags = IORING_SETUP_SQPOLL;
			if ((flags & O_DIRECT) != 0 &&
			    dd_is_blk(g_job.input.u.uring.fd) &&
			    dd_is_blk(g_job.output.u.uring.fd)) {
				ctx.io_uring_flags = IORING_SETUP_IOPOLL;
			}

			g_job.u.uring.poller = SPDK_POLLER_REGISTER(dd_uring_poll, NULL, 0);

			if (spdk_call_unaffinitized(dd_uring_init, &ctx) == NULL || ctx.rc) {
				SPDK_ERRLOG("Failed to create io_uring: %d (%s)\n", ctx.rc, spdk_strerror(-ctx.rc));
				dd_exit(ctx.rc);
				return;
			}
			g_job.u.uring.active = true;

			rc = dd_register_files();
			if (rc) {
				SPDK_ERRLOG("Failed to register files with io_uring: %d (%s)\n", rc, spdk_strerror(-rc));
				dd_exit(rc);
				return;
			}

			rc = dd_register_buffers();
			if (rc) {
				SPDK_ERRLOG("Failed to register buffers with io_uring: %d (%s)\n", rc, spdk_strerror(-rc));
				dd_exit(rc);
				return;
			}

		} else

#endif
		{
			g_job.u.aio.poller = SPDK_POLLER_REGISTER(dd_aio_poll, NULL, 0);
			io_setup(g_opts.queue_depth, &g_job.u.aio.io_ctx);
		}
	}

	clock_gettime(CLOCK_REALTIME, &g_job.start_time);

	g_job.status_poller = SPDK_POLLER_REGISTER(dd_status_poller, NULL,
			      STATUS_POLLER_PERIOD_SEC * SPDK_SEC_TO_USEC);
	if (g_opts.adaptive_mode) {
		g_job.adaptive_poller = SPDK_POLLER_REGISTER(dd_adaptive_poller, NULL,
					       DD_ADAPT_POLLER_PERIOD_US);
	}

	STAILQ_INIT(&g_job.seek_queue);

	/* ✅ 初始化自适应参数 */
	g_job.last_adapt_tsc = spdk_get_ticks();

	/* ===== 提交初始 IO ===== */
	for (i = 0; i < g_opts.queue_depth; i++) {
		g_job.ios[i].ready = true;
		dd_enqueue_io_prio(&g_job, &g_job.ios[i]);
	}

	for (i = 0; i < g_opts.queue_depth; i++) {
		dd_schedule_next_io();
	}
}

enum dd_cmdline_opts {
	DD_OPTION_IF = 0x1000,
	DD_OPTION_OF,
	DD_OPTION_IFLAGS,
	DD_OPTION_OFLAGS,
	DD_OPTION_IB,
	DD_OPTION_OB,
	DD_OPTION_FLOW_CONFIG,
	DD_OPTION_SKIP,
	DD_OPTION_SEEK,
	DD_OPTION_BS,
	DD_OPTION_QD,
	DD_OPTION_COUNT,
	DD_OPTION_AIO,
	DD_OPTION_SPARSE,
	DD_OPTION_ADAPTIVE,
	DD_OPTION_PRIORITY,
	DD_OPTION_STREAM,
	DD_OPTION_FRAME_DEADLINE_US,
	DD_OPTION_DROP_POLICY,
	DD_OPTION_DUMP_SGL,
	DD_OPTION_DUMP_PRIO,
	DD_OPTION_INDEX,
	DD_OPTION_IDIR,
};

static struct option g_cmdline_opts[] = {
	{
		.name = "if",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_IF,
	},
	{
		.name = "of",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_OF,
	},
	{
		.name = "iflag",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_IFLAGS,
	},
	{
		.name = "oflag",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_OFLAGS,
	},
	{
		.name = "ib",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_IB,
	},
	{
		.name = "ob",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_OB,
	},
	{
		.name = "flow-config",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_FLOW_CONFIG,
	},
	{
		.name = "skip",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_SKIP,
	},
	{
		.name = "seek",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_SEEK,
	},
	{
		.name = "bs",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_BS,
	},
	{
		.name = "qd",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_QD,
	},
	{
		.name = "count",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_COUNT,
	},
	{
		.name = "aio",
		.has_arg = 0,
		.flag = NULL,
		.val = DD_OPTION_AIO,
	},
	{
		.name = "sparse",
		.has_arg = 0,
		.flag = NULL,
		.val = DD_OPTION_SPARSE,
	},
	{
		.name = "adaptive",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_ADAPTIVE,
	},
	{
		.name = "priority",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_PRIORITY,
	},
	{
		.name = "stream",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_STREAM,
	},
	{
		.name = "frame-deadline-us",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_FRAME_DEADLINE_US,
	},
	{
		.name = "drop-policy",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_DROP_POLICY,
	},
	{
		.name = "dump-sgl",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_DUMP_SGL,
	},
	{
		.name = "dump-prio",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_DUMP_PRIO,
	},
	{
		.name = "index",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_INDEX,
	},
	{
		.name = "idir",
		.has_arg = 1,
		.flag = NULL,
		.val = DD_OPTION_IDIR,
	},
	{
		.name = NULL
	}
};

static void
usage(void)
{
	printf("[--------- DD Options ---------]\n");
	printf(" --if Input file. Must specify either --if or --ib.\n");
	printf(" --ib Input bdev. Must specifier either --if or --ib\n");
	printf(" --of Output file. Must specify either --of or --ob.\n");
	printf(" --ob Output bdev. Must specify either --of or --ob.\n");
	printf(" --iflag Input file flags.\n");
	printf(" --oflag Output file flags.\n");
	printf(" --flow-config Stream flow config JSON file.\n");
	printf(" --bs I/O unit size (default: %" PRId64 ")\n", g_opts.io_unit_size);
	printf(" --qd Queue depth (default: %d)\n", g_opts.queue_depth);
	printf(" --count I/O unit count. The number of I/O units to copy. (default: all)\n");
	printf(" --skip Skip this many I/O units at start of input. (default: 0)\n");
	printf(" --seek Skip this many I/O units at start of output. (default: 0)\n");
	printf(" --aio Force usage of AIO. (by default io_uring is used if available)\n");
	printf(" --sparse Enable hole skipping in input target\n");
	printf(" --adaptive Enable adaptive I/O scheduling (0 or 1, default: 1)\n");
	printf(" --priority Fixed stream data priority: low|medium|high or 0|1|2 (default: medium)\n");
	printf(" --stream Enable continuous stream mode (0 or 1, default: 0)\n");
	printf(" --frame-deadline-us Max queue wait before dropping old frame in stream mode (default: 0=disabled)\n");
	printf(" --drop-policy Stream drop policy: none|drop-oldest or 0|1 (default: drop-oldest)\n");
	printf(" --dump-sgl Dump SGL for each adaptive readv/writev submit (0 or 1, default: 0)\n");
	printf(" --dump-prio Dump priority for each IO submit (0 or 1, default: 0)\n");
	printf(" --index Index file path. Append a JSON line {name,offset,size} after transfer.\n");
	printf(" --idir  Input directory for small-file pack mode. Merges all files in dir into one bdev write stream.\n");
	printf("         Cannot be combined with --if or --ib. Use --ob or --of.\n");
	printf(" Available iflag and oflag values:\n");
	printf("  append - append mode\n");
	printf("  direct - use direct I/O for data\n");
	printf("  directory - fail unless a directory\n");
	printf("  dsync - use synchronized I/O for data\n");
	printf("  noatime - do not update access time\n");
	printf("  noctty - do not assign controlling terminal from file\n");
	printf("  nofollow - do not follow symlinks\n");
	printf("  nonblock - use non-blocking I/O\n");
	printf("  sync - use synchronized I/O for data and metadata\n");
}

static int
parse_args(int ch, char *arg)
{
	switch (ch) {
	case DD_OPTION_IF:
		free(g_opts.input_file);
		g_opts.input_file = strdup(arg);
		if (g_opts.input_file == NULL) {
			SPDK_ERRLOG("strdup failed for --if\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_OF:
		free(g_opts.output_file);
		g_opts.output_file = strdup(arg);
		if (g_opts.output_file == NULL) {
			SPDK_ERRLOG("strdup failed for --of\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_IFLAGS:
		free(g_opts.input_file_flags);
		g_opts.input_file_flags = strdup(arg);
		if (g_opts.input_file_flags == NULL) {
			SPDK_ERRLOG("strdup failed for --iflag\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_OFLAGS:
		free(g_opts.output_file_flags);
		g_opts.output_file_flags = strdup(arg);
		if (g_opts.output_file_flags == NULL) {
			SPDK_ERRLOG("strdup failed for --oflag\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_IB:
		free(g_opts.input_bdev);
		g_opts.input_bdev = strdup(arg);
		if (g_opts.input_bdev == NULL) {
			SPDK_ERRLOG("strdup failed for --ib\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_OB:
		free(g_opts.output_bdev);
		g_opts.output_bdev = strdup(arg);
		if (g_opts.output_bdev == NULL) {
			SPDK_ERRLOG("strdup failed for --ob\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_FLOW_CONFIG:
		free(g_opts.flow_config_file);
		g_opts.flow_config_file = strdup(arg);
		if (g_opts.flow_config_file == NULL) {
			SPDK_ERRLOG("strdup failed for --flow-config\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_SKIP:
		g_opts.input_offset = spdk_strtol(arg, 10);
		break;
	case DD_OPTION_SEEK:
		g_opts.output_offset = spdk_strtol(arg, 10);
		break;
	case DD_OPTION_BS:
		g_opts.io_unit_size = spdk_strtol(arg, 10);
		break;
	case DD_OPTION_QD:
		g_opts.queue_depth = spdk_strtol(arg, 10);
		break;
	case DD_OPTION_COUNT:
		g_opts.io_unit_count = spdk_strtol(arg, 10);
		break;
	case DD_OPTION_AIO:
		g_opts.aio = true;
		break;
	case DD_OPTION_SPARSE:
		g_opts.sparse = true;
		break;
	case DD_OPTION_ADAPTIVE:
		g_opts.adaptive_mode = (spdk_strtol(arg, 10) != 0);
		break;
	case DD_OPTION_PRIORITY: {
		enum dd_data_priority priority;

		if (dd_parse_priority_arg(arg, &priority) != 0) {
			SPDK_ERRLOG("Invalid --priority value '%s'. Use low|medium|high or 0|1|2\n", arg);
			return -EINVAL;
		}

		g_opts.stream_priority = (uint32_t)priority;
		g_opts.priority_explicit = true;
		break;
	}
	case DD_OPTION_STREAM:
		g_opts.stream_mode = (spdk_strtol(arg, 10) != 0);
		break;
	case DD_OPTION_FRAME_DEADLINE_US: {
		long deadline_us = spdk_strtol(arg, 10);

		if (deadline_us < 0) {
			SPDK_ERRLOG("Invalid --frame-deadline-us value '%s'\n", arg);
			return -EINVAL;
		}

		g_opts.frame_deadline_us = (uint64_t)deadline_us;
		break;
	}
	case DD_OPTION_DROP_POLICY: {
		enum dd_drop_policy policy;

		if (dd_parse_drop_policy_arg(arg, &policy) != 0) {
			SPDK_ERRLOG("Invalid --drop-policy value '%s'. Use none|drop-oldest or 0|1\n", arg);
			return -EINVAL;
		}

		g_opts.drop_policy = (uint32_t)policy;
		break;
	}
	case DD_OPTION_DUMP_SGL:
		g_opts.dump_sgl = (spdk_strtol(arg, 10) != 0);
		break;
	case DD_OPTION_DUMP_PRIO:
		g_opts.dump_prio = (spdk_strtol(arg, 10) != 0);
		break;
	case DD_OPTION_INDEX:
		free(g_opts.index_file);
		g_opts.index_file = strdup(arg);
		if (g_opts.index_file == NULL) {
			SPDK_ERRLOG("strdup failed for --index\n");
			return -ENOMEM;
		}
		break;
	case DD_OPTION_IDIR:
		free(g_opts.input_dir);
		g_opts.input_dir = strdup(arg);
		if (g_opts.input_dir == NULL) {
			SPDK_ERRLOG("strdup failed for --idir\n");
			return -ENOMEM;
		}
		break;
	default:
		usage();
		return -EINVAL;
	}

	return 0;
}

static void
dd_free(void)
{
	uint32_t i;

	free(g_opts.input_file);
	free(g_opts.output_file);
	free(g_opts.input_bdev);
	free(g_opts.output_bdev);
	free(g_opts.flow_config_file);
	free(g_opts.input_file_flags);
	free(g_opts.output_file_flags);
	free(g_opts.index_file);
	free(g_opts.input_dir);

	/* 释放 pack 文件列表 */
	if (g_job.pack_file_list != NULL) {
		int fi;
		for (fi = 0; fi < g_job.pack_file_count; fi++) {
			free(g_job.pack_file_list[fi]);
		}
		free(g_job.pack_file_list);
		g_job.pack_file_list = NULL;
	}
	if (g_job.pack_fd >= 0) {
		close(g_job.pack_fd);
		g_job.pack_fd = -1;
	}


	if (g_job.input.type == DD_TARGET_TYPE_FILE || g_job.output.type == DD_TARGET_TYPE_FILE) {
#ifdef SPDK_CONFIG_URING
		if (g_opts.aio == false) {
			if (g_job.u.uring.active) {
				io_uring_unregister_files(&g_job.u.uring.ring);
				io_uring_queue_exit(&g_job.u.uring.ring);
			}
		} else
#endif
		{
			io_destroy(g_job.u.aio.io_ctx);
		}
	}

	if (g_job.ios) {
		for (i = 0; i < g_opts.queue_depth; i++) {
			spdk_free(g_job.ios[i].buf);

			/* 新增：释放 SGL */
            if (g_opts.adaptive_mode) {
                dd_free_sgl(&g_job.ios[i]);
            }
		}

		free(g_job.ios);
	}

	free(g_job.flows);
}

int
main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	int rc = 1;

	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "spdk_dd";
	/* Reactor core mask is selected with SPDK's standard -m option. */
	opts.shutdown_cb = dd_finish;
	opts.rpc_addr = NULL;
	rc = spdk_app_parse_args(argc, argv, &opts, "", g_cmdline_opts, parse_args, usage);
	if (rc == SPDK_APP_PARSE_ARGS_FAIL) {
		SPDK_ERRLOG("Invalid arguments\n");
		goto end;
	} else if (rc == SPDK_APP_PARSE_ARGS_HELP) {
		goto end;
	}

	/* In pack/send mode, automatically use the Linux NVMe-oF namespace
	 * exported by the remote SPDK target. This is intentionally a file
	 * target, not an SPDK bdev in this process. */
	if (g_opts.input_dir != NULL && g_opts.output_file == NULL &&
	    g_opts.output_bdev == NULL) {
		char remote_nvme[PATH_MAX];

		rc = dd_find_spdk_remote_nvme(remote_nvme, sizeof(remote_nvme));
		if (rc != 0) {
			SPDK_ERRLOG("Could not auto-detect remote SPDK NVMe namespace: %s\n",
				    strerror(-rc));
			goto end;
		}

		g_opts.output_file = strdup(remote_nvme);
		if (g_opts.output_file == NULL) {
			rc = ENOMEM;
			goto end;
		}
		SPDK_NOTICELOG("Auto-detected remote SPDK NVMe namespace: %s\n",
			       g_opts.output_file);
	}

	if (g_opts.input_file != NULL && g_opts.input_bdev != NULL) {
		SPDK_ERRLOG("You may specify either --if or --ib, but not both.\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.input_dir != NULL &&
	    (g_opts.input_file != NULL || g_opts.input_bdev != NULL)) {
		SPDK_ERRLOG("--idir cannot be combined with --if or --ib.\n");
		rc = EINVAL;
		goto end;
	}

	/* Pack mode supports both --ob and --of. */

	if (g_opts.output_file != NULL && g_opts.output_bdev != NULL) {
		SPDK_ERRLOG("You may specify either --of or --ob, but not both.\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.input_file == NULL && g_opts.input_bdev == NULL &&
	    g_opts.input_dir == NULL) {
		SPDK_ERRLOG("You must specify either --if, --ib, or --idir\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.output_file == NULL && g_opts.output_bdev == NULL) {
		SPDK_ERRLOG("You must specify either --of or --ob\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.io_unit_size <= 0) {
		SPDK_ERRLOG("Invalid --bs value\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.io_unit_count < 0) {
		SPDK_ERRLOG("Invalid --count value\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.output_file == NULL && g_opts.output_file_flags != NULL) {
		SPDK_ERRLOG("--oflags may be used only with --of\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.input_file == NULL && g_opts.input_file_flags != NULL) {
		SPDK_ERRLOG("--iflags may be used only with --if\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.stream_mode && g_opts.sparse) {
		SPDK_ERRLOG("--stream mode does not support --sparse\n");
		rc = EINVAL;
		goto end;
	}

	if (g_opts.flow_config_file != NULL && g_opts.priority_explicit) {
		SPDK_ERRLOG("--priority cannot be used with --flow-config; set per-flow priority in config\n");
		rc = EINVAL;
		goto end;
	}

	rc = spdk_app_start(&opts, dd_run, NULL);
	if (rc) {
		SPDK_ERRLOG("Error occurred while performing copy\n");
	}

	dd_free();
	spdk_app_fini();

end:
	return rc;
}
