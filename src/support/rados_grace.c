/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright 2017 Red Hat, Inc. and/or its affiliates.
 * Author: Jeff Layton <jlayton@redhat.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * rados-grace: tool for managing coordinated grace period database
 *
 * The rados-grace database is a rados object with a well-known name that
 * with which all cluster nodes can interact to coordinate grace-period
 * enforcement.
 *
 * It consists of two parts:
 *
 * 1) 2 uint64_t epoch values (stored LE) that indicate the serial number of
 * the current grace period (C) and the serial number of the grace period that
 * from which recovery is currently allowed (R). These are stored as object
 * data.
 *
 * 2) An omap containing a key for each node that currently requires a grace
 * period.
 *
 * Consider a single server epoch (E) of an individual NFS server to be the
 * period between reboots. That consists of an initial grace period and
 * a regular operation period. An epoch value of 0 is never valid.
 *
 * The first value (C) indicates the current server epoch. The client recovery
 * db should be tagged with this value on creation, or when updating the db
 * after the grace period has been fully lifted.
 *
 * The second uint64_t value in the data tells the NFS server from what
 * recovery db it is allowed to reclaim. A value of 0 in this field means that
 * we are out of the cluster-wide grace period and that no recovery is allowed.
 *
 * The cluster manager (or sentient administrator) begins a new grace period by
 * passing in a number of nodes as an initial set. If the current recovery
 * serial number is set to 0, then we'll copy the current value to the recovery
 * serial number, and increment the current value by 1. At that point, a new
 * epoch is established and the cluster-wide grace period begins.
 *
 * As nodes come up, we must decide whether to allow NFS reclaim and what
 * epoch's database the server should use. This requires 2 inputs:
 *
 * 1) whether we were successful in reclaiming the cephfs state of a previous
 *    instance of this ganesha's ceph client.
 *
 * 2) whether we're currently in a cluster-wide grace period.
 *
 * If the cephfs reclaim was successful and we are in a grace period, then
 * NFS reclaim should be allowed from the current reclaim epoch (R). If
 * cephfs reclaim was successful and we are not in a grace period, then NFS
 * reclaim is allowed for the current epoch (C).
 *
 * If the cephfs reclaim is not successful and we are not in a grace period,
 * then no NFS reclaim is allowed. If cephfs reclaim is not successful and we
 * are in a grace period, then we allow reclaim for epoch (R).
 *
 * Each server comes up, and first checks whether a cluster-wide grace period
 * is in force. If it is, then it sets its own grace period request flag (if
 * necessary) and then begins recovery according to the rules above.
 *
 * As each node completes its own recovery, it clears its flag in the omap. The
 * node that clears the last flag will then lift the grace period fully by
 * setting the reclaim epoch R to 0. Eventually other nodes will poll the
 * grace object, notice that the grace period is lifted and will transition
 * to normal operation.
 */
#include "config.h"
#include <stdio.h>
#include <stdint.h>
#include <endian.h>
#include <rados/librados.h>
#include <errno.h>
#include <getopt.h>
#include <stdlib.h>
#include <limits.h>
#include <stdbool.h>

#define MAX_ITEMS			1024

static void rados_grace_notify(rados_ioctx_t io_ctx, const char *oid)
{
	static char *buf;
	static size_t len;

	/* FIXME: we don't really want or need this to be synchronous */
	rados_notify2(io_ctx, oid, "", 0, 3000, &buf, &len);
	rados_buffer_free(buf);
}

int
rados_grace_create(rados_ioctx_t io_ctx, const char *oid)
{
	int			ret;
	rados_write_op_t	op = NULL;
	uint64_t		cur = htole64(1);	// starting epoch
	uint64_t		rec = htole64(0);	// no recovery yet
	char			buf[sizeof(uint64_t) * 2];

	/*
	 * 2 uint64_t's
	 *
	 * The first denotes the current epoch serial number, the epoch serial
	 * number under which new recovery records should be created.
	 *
	 * The second number denotes the epoch from which clients are allowed
	 * to reclaim.
	 *
	 * An epoch of zero is never allowed, so if rec=0, then the grace
	 * period is no longer in effect, and can't be joined.
	 */
	memcpy(buf, (char *)&cur, sizeof(cur));
	memcpy(buf + sizeof(cur), (char *)&rec, sizeof(rec));

	op = rados_create_write_op();
	/* Create the object */
	rados_write_op_create(op, LIBRADOS_CREATE_EXCLUSIVE, NULL);

	/* Set serial numbers if we created the object */
	rados_write_op_write_full(op, buf, sizeof(buf));

	ret = rados_write_op_operate(op, io_ctx, oid, NULL, 0);
	rados_release_write_op(op);
	return ret;
}

int
rados_grace_dump(rados_ioctx_t io_ctx, const char *oid)
{
	int ret;
	rados_omap_iter_t iter;
	rados_read_op_t op;
	char *key_out = NULL;
	char *val_out = NULL;
	unsigned char more = '\0';
	size_t len_out = 0;
	char buf[sizeof(uint64_t) * 2];
	uint64_t	cur, rec;

	op = rados_create_read_op();
	rados_read_op_read(op, 0, sizeof(buf), buf, &len_out, NULL);
	rados_read_op_omap_get_keys2(op, "", MAX_ITEMS, &iter, &more, NULL);
	ret = rados_read_op_operate(op, io_ctx, oid, 0);
	if (ret < 0) {
		printf("%s: ret=%d", __func__, ret);
		goto out;
	}

	if (len_out != sizeof(buf)) {
		ret = -ENOTRECOVERABLE;
		goto out;
	}

	if (more) {
		ret = -ENOTRECOVERABLE;
		goto out;
	}

	cur = le64toh(*(uint64_t *)buf);
	rec = le64toh(*(uint64_t *)(buf + sizeof(uint64_t)));
	printf("cur=%lu rec=%lu\n", cur, rec);
	for (;;) {
		rados_omap_get_next(iter, &key_out, &val_out, &len_out);
		if (key_out == NULL)
			break;
		printf("%s ", key_out);
	}
	printf("\n");
out:
	rados_release_read_op(op);
	return ret;
}

int
rados_grace_epochs(rados_ioctx_t io_ctx, const char *oid,
			uint64_t *cur, uint64_t *rec)
{
	int ret;
	rados_read_op_t op;
	size_t len_out = 0;
	char buf[sizeof(uint64_t) * 2];

	op = rados_create_read_op();
	rados_read_op_read(op, 0, sizeof(buf), buf, &len_out, NULL);
	ret = rados_read_op_operate(op, io_ctx, oid, 0);
	if (ret < 0)
		goto out;

	ret = -ENOTRECOVERABLE;
	if (len_out != sizeof(buf))
		goto out;

	*cur = le64toh(*(uint64_t *)buf);
	*rec = le64toh(*(uint64_t *)(buf + sizeof(uint64_t)));
	ret = 0;
out:
	rados_release_read_op(op);
	return ret;
}

static int
__rados_grace_start(rados_ioctx_t io_ctx, const char *oid, int nodes,
			const char **nodeids, uint64_t *pcur, uint64_t *prec,
			bool start)
{
	int			ret;
	char			buf[sizeof(uint64_t) * 2];
	char			**vals = NULL;
	size_t			*lens = NULL;
	size_t			len = 0;
	uint64_t		cur, rec, ver;

	vals = calloc(nodes, sizeof(char *));
	if (!vals) {
		ret = -ENOMEM;
		goto out;
	}

	lens = calloc(nodes, sizeof(size_t));
	if (!lens) {
		ret = -ENOMEM;
		free(vals);
		goto out;
	}

	do {
		rados_write_op_t	wop;
		rados_read_op_t		rop;

		/* read epoch blob */
		rop = rados_create_read_op();
		rados_read_op_read(rop, 0, sizeof(buf), buf, &len, NULL);
		ret = rados_read_op_operate(rop, io_ctx, oid, 0);
		rados_release_read_op(rop);
		if (ret < 0)
			break;

		if (len != sizeof(buf)) {
			ret = -ENOTRECOVERABLE;
			break;
		}

		/* Get old epoch numbers and version */
		cur = le64toh(*(uint64_t *)buf);
		rec = le64toh(*(uint64_t *)(buf + sizeof(uint64_t)));
		ver = rados_get_last_version(io_ctx);

		/* Only start a new grace period if force flag is set */
		if (rec == 0 && !start)
			break;

		/* Attempt to update object */
		wop = rados_create_write_op();

		/* Ensure that nothing has changed */
		rados_write_op_assert_version(wop, ver);

		/* Update the object iff rec == 0 */
		if (rec == 0) {
			uint64_t tc, tr;

			rec = cur;
			++cur;
			tc = htole64(cur);
			tr = htole64(rec);
			memcpy(buf, (char *)&tc, sizeof(tc));
			memcpy(buf + sizeof(tc), (char *)&tr, sizeof(tr));
			rados_write_op_write_full(wop, buf, sizeof(buf));
		}

		/* Set omap values to given ones */
		rados_write_op_omap_set(wop, nodeids,
				(const char * const*)vals, lens, nodes);

		ret = rados_write_op_operate(wop, io_ctx, oid, NULL, 0);
		rados_release_write_op(wop);
		if (ret >= 0)
			rados_grace_notify(io_ctx, oid);
	} while (ret == -ERANGE);

	if (!ret) {
		*pcur = cur;
		*prec = rec;
	}
out:
	free(lens);
	free(vals);
	return ret;
}

int
rados_grace_start(rados_ioctx_t io_ctx, const char *oid, int nodes,
			const char **nodeids)
{
	uint64_t	cur, rec;

	return __rados_grace_start(io_ctx, oid, nodes, nodeids, &cur,
					&rec, true);
}

int
rados_grace_join(rados_ioctx_t io_ctx, const char *oid, uint32_t nodeid,
			uint64_t *pcur, uint64_t *prec)
{
	char		buf[11];
	const char	*nodeids[1];

	if (nodeid == UINT32_MAX)
		return -EINVAL;

	snprintf(buf, sizeof(buf), "%u", nodeid);
	nodeids[0] = buf;
	return __rados_grace_start(io_ctx, oid, 1, nodeids, pcur, prec, false);
}

static int
__rados_grace_lift(rados_ioctx_t io_ctx, const char *oid, int nodes,
			const char **nodeids, uint64_t *pcur, uint64_t *prec)
{
	int			ret;
	const char		**keys = NULL;
	uint64_t		cur, rec;

	keys = calloc(nodes, sizeof(char *));
	if (!keys) {
		ret = -ENOMEM;
		goto out;
	}

	do {
		int			i, k, cnt;
		rados_write_op_t	wop;
		rados_read_op_t		rop;
		rados_omap_iter_t	iter;
		char			*key, *val;
		size_t			len;
		char			buf[sizeof(uint64_t) * 2];
		unsigned char		more = '\0';
		uint64_t		ver;

		/* read epoch blob */
		rop = rados_create_read_op();
		rados_read_op_read(rop, 0, sizeof(buf), buf, &len, NULL);
		rados_read_op_omap_get_keys2(rop, "", MAX_ITEMS, &iter,
						&more, NULL);
		ret = rados_read_op_operate(rop, io_ctx, oid, 0);
		if (ret < 0) {
			rados_release_read_op(rop);
			break;
		}

		if (more) {
			ret = -ENOTRECOVERABLE;
			break;
		}

		if (len != sizeof(buf)) {
			ret = -ENOTRECOVERABLE;
			break;
		}

		/* Get old epoch numbers and version, iff rec == 0 */
		cur = le64toh(*(uint64_t *)buf);
		rec = le64toh(*(uint64_t *)(buf + sizeof(uint64_t)));
		ver = rados_get_last_version(io_ctx);
		rados_release_read_op(rop);

		/*
		 * If we're not in grace period, then there should be no
		 * records in the omap. Either way, we don't want to alter
		 * anything in this case.
		 */
		if (rec == 0) {
			ret = rados_omap_get_next(iter, &key, &val, &len);
			if (ret < 0)
				break;
			if (key != NULL) {
				ret = -ENOTRECOVERABLE;
			}
			break;
		}

		/*
		 * Walk omap keys, see if it's in nodeids array.
		 * Add any that are to the "keys" array.
		 */
		cnt = 0;
		k = 0;
		for (;;) {
			ret = rados_omap_get_next(iter, &key, &val, &len);
			if (!key)
				break;
			++cnt;
			for (i = 0; i < nodes; ++i) {
				if (strcmp(key, nodeids[i]))
					continue;
				keys[k] = nodeids[i];
				++k;
			}
		};

		/* No matching keys? Nothing to do. */
		if (k == 0)
			break;

		/* Attempt to update object */
		wop = rados_create_write_op();

		/* Ensure that nothing has changed */
		rados_write_op_assert_version(wop, ver);

		/* Remove keys from array */
		rados_write_op_omap_rm_keys(wop, (const char * const*)keys, k);

		/*
		 * If number of omap records we're removing == number of keys,
		 * then fully lift the grace period.
		 */
		if (cnt == k) {
			uint64_t tc, tr;

			rec = 0;
			tr = htole64(rec);
			tc = htole64(cur);
			memcpy(buf, (char *)&tc, sizeof(tc));
			memcpy(buf + sizeof(tc), (char *)&tr, sizeof(tr));
			rados_write_op_write_full(wop, buf, sizeof(buf));
		}

		ret = rados_write_op_operate(wop, io_ctx, oid, NULL, 0);
		rados_release_write_op(wop);
		if (ret >= 0)
			rados_grace_notify(io_ctx, oid);
	} while (ret == -ERANGE);
	if (!ret) {
		*pcur = cur;
		*prec = rec;
	}
out:
	free(keys);
	return ret;
}

int
rados_grace_lift(rados_ioctx_t io_ctx, const char *oid, int nodes,
			const char **nodeids)
{
	uint64_t	cur, rec;

	return __rados_grace_lift(io_ctx, oid, nodes, nodeids, &cur,
					&rec);
}

int
rados_grace_done(rados_ioctx_t io_ctx, const char *oid, uint32_t nodeid,
			uint64_t *pcur, uint64_t *prec)
{
	char		buf[11];
	const char	*nodeids[1];

	if (nodeid == UINT32_MAX)
		return -EINVAL;

	snprintf(buf, sizeof(buf), "%u", nodeid);
	nodeids[0] = buf;
	return __rados_grace_lift(io_ctx, oid, 1, nodeids, pcur, prec);
}
