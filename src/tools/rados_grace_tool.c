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
 * The first uint64_t value indicates the current server epoch. The
 * client recovery db should be tagged with this value on creation, or when
 * updating the db on lifting of the grace period.
 *
 * The second uint64_t value in the data tells the NFS server from what
 * recovery db it is allowed to reclaim. A value of 0 in this field means that
 * we are out of the grace period and that no recovery is allowed.
 *
 * The cluster manager (or sentient administrator) begins a new grace period by
 * passing in a number of nodes as an initial set. If the current recovery
 * serial number is set to 0, then we'll copy the current value to the recovery
 * serial number, and increment the current value by 1. At that point, the
 * cluster-wide grace period has been established.
 *
 * As nodes come up, we must decide whether to allow NFS reclaim and from what
 * epoch's database if it is allowed. This requires 2 inputs:
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
 * setting the reclaim epoch R to 0.
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
#include <rados_grace.h>

#define POOL_ID				"nfs-ganesha"
#define RADOS_GRACE_OID			"grace"

static int
cluster_connect(rados_ioctx_t *io_ctx, const char *pool)
{
	int ret;
	rados_t clnt;

	ret = rados_create(&clnt, NULL);
	if (ret < 0) {
		fprintf(stderr, "rados_create: %d\n", ret);
		return ret;
	}

	ret = rados_conf_read_file(clnt, NULL);
	if (ret < 0) {
		fprintf(stderr, "rados_conf_read_file: %d\n", ret);
		return ret;
	}

	ret = rados_connect(clnt);
	if (ret < 0) {
		fprintf(stderr, "rados_connect: %d\n", ret);
		return ret;
	}

	ret = rados_pool_create(clnt, pool);
	if (ret < 0 && ret != -EEXIST) {
		fprintf(stderr, "rados_pool_create: %d\n", ret);
		return ret;
	}

	ret = rados_ioctx_create(clnt, pool, io_ctx);
	if (ret < 0) {
		fprintf(stderr, "rados_ioctx_create: %d\n", ret);
		return ret;
	}
	return 0;
}

static void usage(char **argv)
{
	fprintf(stderr, "Usage:\%s: [-l] nodeid ...\n", argv[0]);
}

int main(int argc, char **argv)
{
	int		ret, i, nodes;
	bool		lift = false;
	rados_ioctx_t	io_ctx;

	while ((ret = getopt(argc, argv, "l")) != EOF) {
		switch(ret) {
		case 'l':
			lift = true;
			break;
		default:
			usage(argv);
			return 1;
		}
	}

	ret = 0;
	nodes = argc - optind;
	if (nodes > 0) {
		for (i = 0; i < nodes; ++i) {
			unsigned long	val;
			char		*end;
			int		idx = i + optind;

			/* Should never happen with argv, but best to check */
			if (*argv[idx] == '\0')
				ret = -EINVAL;
			/* Ensure any remaining argv values are all decimal ints <UINT_MAX */
			val = strtoul(argv[idx], &end, 10);
			if (*end != '\0')
				ret = -EINVAL;
			if (val >= UINT_MAX)
				ret = -EINVAL;
			if (ret) {
				fprintf(stderr, "Bad nodeid: %s\n", argv[idx]);
				return 1;
			}
		}
	}

	ret = cluster_connect(&io_ctx, POOL_ID);
	if (ret) {
		fprintf(stderr, "Can't connect to cluster: %d\n", ret);
		return 1;
	}

	ret = rados_grace_create(io_ctx, RADOS_GRACE_OID);
	if (ret < 0 && ret != -EEXIST) {
		fprintf(stderr, "Can't create grace db: %d\n", ret);
		return 1;
	}

	/* No nodeids means don't change anything */
	if (nodes) {
		if (lift)
			ret = rados_grace_lift(io_ctx, RADOS_GRACE_OID, nodes,
					 &argv[optind]);
		else
			ret = rados_grace_start(io_ctx, RADOS_GRACE_OID, nodes,
					  &argv[optind]);

		if (ret < 0) {
			fprintf(stderr, "Can't alter grace: %d\n", ret);
			return 1;
		}
	}

	ret = rados_grace_dump(io_ctx, RADOS_GRACE_OID);
	if (ret)
		return 1;
	return 0;
}
