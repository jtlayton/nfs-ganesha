/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright 2018 Red Hat, Inc. and/or its affiliates.
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
 * recovery_rados_cluster: a clustered recovery backing store
 *
 * See ganesha-rados-cluster-design(8) for overall design and theory
 */

#include "config.h"
#include <netdb.h>
#include <rados/librados.h>
#include <rados_grace.h>
#include "log.h"
#include "nfs_core.h"
#include "sal_functions.h"
#include "recovery_rados.h"

/* Use hostname as nodeid in cluster */
char *nodeid;
static uint64_t rados_watch_cookie;

static void rados_grace_watchcb(void *arg, uint64_t notify_id, uint64_t handle,
				uint64_t notifier_id, void *data,
				size_t data_len)
{
	int ret;

	/* ACK it first, so we keep things moving along */
	ret = rados_notify_ack(rados_recov_io_ctx, rados_kv_param.grace_oid,
			       notify_id, rados_watch_cookie, NULL, 0);
	if (ret < 0)
		LogEvent(COMPONENT_CLIENTID,
			 "rados_notify_ack failed: %d", ret);

	/* Now kick the reaper to check things out */
	nfs_notify_grace_waiters();
	reaper_wake();
}

static int rados_cluster_init(void)
{
	int ret;

	/* If no nodeid is specified, then use the hostname */
	if (rados_kv_param.nodeid) {
		nodeid = gsh_strdup(rados_kv_param.nodeid);
	} else {
		long maxlen = sysconf(_SC_HOST_NAME_MAX);

		nodeid = gsh_malloc(maxlen);
		ret = gethostname(nodeid, maxlen);
		if (ret) {
			LogEvent(COMPONENT_CLIENTID, "gethostname failed: %d",
					errno);
			ret = -errno;
			goto out_free_nodeid;
		}
	}

	ret = rados_kv_connect(&rados_recov_io_ctx, rados_kv_param.userid,
			rados_kv_param.ceph_conf, rados_kv_param.pool,
			rados_kv_param.namespace);
	if (ret < 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to connect to cluster: %d", ret);
		goto out_shutdown;
	}

	ret = rados_grace_member(rados_recov_io_ctx, rados_kv_param.grace_oid,
				 nodeid);
	if (ret < 0) {
		LogEvent(COMPONENT_CLIENTID,
			 "Cluster membership check failed: %d", ret);
		goto out_shutdown;
	}

	/* FIXME: not sure about the 30s timeout value here */
	ret = rados_watch3(rados_recov_io_ctx, rados_kv_param.grace_oid,
			   &rados_watch_cookie, rados_grace_watchcb, NULL,
			   30, NULL);
	if (ret < 0) {
		LogEvent(COMPONENT_CLIENTID,
			"Failed to set watch on grace db: %d", ret);
		goto out_shutdown;
	}
	return 0;

out_shutdown:
	rados_kv_shutdown();
out_free_nodeid:
	gsh_free(nodeid);
	nodeid = NULL;
	return ret;
}

/* Try to delete old recovery db */
static void rados_cluster_end_grace(void)
{
	int ret;
	rados_write_op_t wop;
	uint64_t cur, rec;

	if (rados_recov_old_oid[0] == '\0')
		return;

	ret = rados_grace_enforcing_off(rados_recov_io_ctx,
					rados_kv_param.grace_oid, nodeid,
					&cur, &rec);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to set grace off for %s: %d", nodeid, ret);

	wop = rados_create_write_op();
	rados_write_op_remove(wop);
	ret = rados_write_op_operate(wop, rados_recov_io_ctx,
			       rados_recov_old_oid, NULL, 0);
	if (ret)
		LogEvent(COMPONENT_CLIENTID, "Failed to remove %s: %d",
			 rados_recov_old_oid, ret);

	memset(rados_recov_old_oid, '\0', sizeof(rados_recov_old_oid));
}

static void rados_cluster_read_clids(nfs_grace_start_t *gsp,
				add_clid_entry_hook add_clid_entry,
				add_rfh_entry_hook add_rfh_entry)
{
	int ret;
	uint64_t cur, rec;
	rados_write_op_t wop;
	struct pop_args args = {
		.add_clid_entry = add_clid_entry,
		.add_rfh_entry = add_rfh_entry,
	};

	if (gsp) {
		LogEvent(COMPONENT_CLIENTID,
			 "Clustered rados backend does not support takeover!");
		return;
	}

	/* Start or join a grace period */
	ret = rados_grace_join(rados_recov_io_ctx, rados_kv_param.grace_oid,
			       nodeid, &cur, &rec, true);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to join grace period: %d", ret);
		return;
	}

	/*
	 * Recovery db names are "rec-cccccccccccccccc:hostname"
	 *
	 * "rec-" followed by epoch in 16 hex digits + nodeid.
	 */
	snprintf(rados_recov_oid, sizeof(rados_recov_oid),
			"rec-%16.16lx:%s", cur, nodeid);
	wop = rados_create_write_op();
	rados_write_op_create(wop, LIBRADOS_CREATE_IDEMPOTENT, NULL);
	rados_write_op_omap_clear(wop);
	ret = rados_write_op_operate(wop, rados_recov_io_ctx,
				     rados_recov_oid, NULL, 0);
	rados_release_write_op(wop);
	if (ret < 0) {
		LogEvent(COMPONENT_CLIENTID, "Failed to create recovery db");
		return;
	};

	snprintf(rados_recov_old_oid, sizeof(rados_recov_old_oid),
			"rec-%16.16lx:%s", rec, nodeid);
	ret = rados_kv_traverse(rados_kv_pop_clid_entry, &args,
				rados_recov_old_oid);
	if (ret < 0)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to traverse recovery db: %d", ret);
}

static bool rados_cluster_try_lift_grace(void)
{
	int ret;
	uint64_t cur, rec;

	ret = rados_grace_lift(rados_recov_io_ctx, rados_kv_param.grace_oid,
				nodeid, &cur, &rec);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID,
			 "Attempt to lift grace failed: %d", ret);
		return false;
	}

	/* Non-zero rec means grace is still in force */
	return (rec == 0);
}

struct rados_cluster_kv_pairs {
	size_t	slots;			/* Current array size */
	size_t	num;			/* Count of populated elements */
	char	**keys;			/* Array of key strings */
	char	**vals;			/* Array of value blobs */
	size_t	*lens;			/* Array of value lengths */
};

/*
 * FIXME: Since each hash tree is protected by its own mutex, we can't ensure
 *        that we'll get an accurate count before allocating. For now, we just
 *        have a fixed-size cap of 1024 entries in the db, but we should allow
 *        there to be an arbitrary number of entries.
 */
#define RADOS_KV_STARTING_SLOTS		1024

static void rados_set_client_cb(struct rbt_node *pn, void *arg)
{
	struct hash_data *addr = RBT_OPAQ(pn);
	nfs_client_id_t *clientid = addr->val.addr;
	struct rados_cluster_kv_pairs *kvp = arg;
	char ckey[RADOS_KEY_MAX_LEN];
	char cval[RADOS_VAL_MAX_LEN];

	/* FIXME: resize arrays in this case? */
	if (kvp->num >= kvp->slots) {
		LogEvent(COMPONENT_CLIENTID, "too many clients to copy!");
		return;
	}

	rados_kv_create_key(clientid, ckey);
	rados_kv_create_val(clientid, cval);

	kvp->keys[kvp->num] = strdup(ckey);
	kvp->vals[kvp->num] = strdup(cval);
	kvp->lens[kvp->num] = strlen(cval);
	++kvp->num;
}

/**
 * @brief Start local grace period if we're in a global one
 *
 * In clustered setups, other machines in the cluster can start a new
 * grace period. Check for that and enter the grace period if so.
 */
static void rados_cluster_maybe_start_grace(void)
{
	int ret, i;
	nfs_grace_start_t gsp = { .event = EVENT_JUST_GRACE };
	rados_write_op_t wop;
	uint64_t cur, rec;
	char *keys[RADOS_KV_STARTING_SLOTS];
	char *vals[RADOS_KV_STARTING_SLOTS];
	size_t lens[RADOS_KV_STARTING_SLOTS];
	struct rados_cluster_kv_pairs kvp = {
					.slots = RADOS_KV_STARTING_SLOTS,
					.num = 0,
					.keys = keys,
					.vals = vals,
					.lens = lens };


	/* Fix up the strings */
	ret = rados_grace_epochs(rados_recov_io_ctx, rados_kv_param.grace_oid,
				 &cur, &rec);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID, "rados_grace_epochs failed: %d",
				ret);
		return;
	}

	/* No grace period if rec == 0 */
	if (rec == 0)
		return;

	/*
	 * A new epoch has been started and a cluster-wide grace period has
	 * been reqeuested. Make a new DB for "cur" that has all of of the
	 * currently active clients in it.
	 */

	/* Fix up the oid strings */
	snprintf(rados_recov_oid, sizeof(rados_recov_oid),
			"rec-%16.16lx:%s", cur, nodeid);
	snprintf(rados_recov_old_oid, sizeof(rados_recov_old_oid),
			"rec-%16.16lx:%s", rec, nodeid);

	/* Populate key/val/len arrays from confirmed client hash */
	hashtable_for_each(ht_confirmed_client_id, rados_set_client_cb, &kvp);

	/* Create new write op and package it up for callback */
	wop = rados_create_write_op();
	rados_write_op_create(wop, LIBRADOS_CREATE_IDEMPOTENT, NULL);
	rados_write_op_omap_clear(wop);
	rados_write_op_omap_set(wop, (char const * const *)keys,
				     (char const * const *)vals,
				     (const size_t *)lens, kvp.num);
	ret = rados_write_op_operate(wop, rados_recov_io_ctx,
				     rados_recov_oid, NULL, 0);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
				"rados_write_op_operate failed: %d", ret);

	rados_release_write_op(wop);

	/* Free copied strings */
	for (i = 0; i < kvp.num; ++i) {
		free(kvp.keys[i]);
		free(kvp.vals[i]);
	}

	/* Start a new grace period */
	nfs_start_grace(&gsp);
}

static void rados_cluster_shutdown(void)
{
	int		ret;
	uint64_t	cur, rec;

	/*
	 * Request grace on clean shutdown to minimize the chance that we'll
	 * miss the window and the MDS kills off the old session.
	 *
	 * FIXME: only do this if our key is in the omap, and we have a
	 *        non-empty recovery db.
	 */
	ret = rados_grace_join(rados_recov_io_ctx, rados_kv_param.grace_oid,
				nodeid, &cur, &rec, true);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to start grace period on shutdown: %d", ret);

	ret = rados_unwatch2(rados_recov_io_ctx, rados_watch_cookie);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to unwatch grace db: %d", ret);

	rados_kv_shutdown();
	gsh_free(nodeid);
	nodeid = NULL;
}

static void rados_cluster_set_enforcing(void)
{
	int		ret;
	uint64_t	cur, rec;

	ret = rados_grace_enforcing_on(rados_recov_io_ctx,
				       rados_kv_param.grace_oid, nodeid,
				       &cur, &rec);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to set enforcing for %s: %d", nodeid, ret);
}

static bool rados_cluster_grace_enforcing(void)
{
	int		ret;

	ret = rados_grace_enforcing_check(rados_recov_io_ctx,
					  rados_kv_param.grace_oid, nodeid);
	LogEvent(COMPONENT_CLIENTID, "%s: ret=%d", __func__, ret);
	return (ret == 0);
}

static bool rados_cluster_is_member(void)
{
	int	ret = rados_grace_member(rados_recov_io_ctx,
					 rados_kv_param.grace_oid, nodeid);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID,
			 "%s: %s is no longer a cluster member (ret=%d)",
			 __func__, nodeid, ret);
		return false;
	}
	return true;
}

/* OID of clustermap object */
#define CLUSTERMAP_OID		"clustermap"

/*
 * The Linux client currently only allows a max of 10 servers. Let's generously
 * exceed it by a little.
 */
#define MAX_CLUSTER_MEMBERS	16
static int rados_cluster_get_replicas(utf8string **paddr)
{
	int ret;
	unsigned int count = 0;
	rados_omap_iter_t iter = NULL;
	rados_read_op_t op;
	unsigned char more = '\0';
	utf8string *addr = NULL;

	op = rados_create_read_op();
	rados_read_op_omap_get_vals2(op, "", "", MAX_CLUSTER_MEMBERS, &iter,
				     &more, NULL);
	ret = rados_read_op_operate(op, rados_recov_io_ctx, CLUSTERMAP_OID, 0);
	if (ret < 0) {
		LogEvent(COMPONENT_CLIENTID, "RADOS read op failed: %d", ret);
		goto out;
	}

	ret = 0;
	count = rados_omap_iter_size(iter);
	if (!count)
		goto out;

	addr = gsh_calloc(count, sizeof(*addr));
	for (;;) {
		char *key_out = NULL;
		char *val_out = NULL;
		size_t len_out = 0;

		rados_omap_get_next(iter, &key_out, &val_out, &len_out);
		if (key_out == NULL || val_out == NULL)
			break;
		if (unlikely(ret >= count)) {
			LogWarn(COMPONENT_CLIENTID,
				"Array overrun averted: ret=%d count=%u",
				ret, count);
			break;
		}

		/*
		 * RFC5661, section 11.9
		 *
		 * A zero-length string SHOULD be used to indicate the current
		 * address being used for the RPC call.
		 *
		 * If the key and nodeid match, then it's the same server, and
		 * we can just send an empty string.
		 */
		if (strcmp(key_out, nodeid)) {
			addr[ret].utf8string_val = gsh_memdup(val_out, len_out);
			addr[ret].utf8string_len = len_out;
		}
		++ret;
	}
out:
	rados_omap_get_end(iter);
	rados_release_read_op(op);
	*paddr = addr;
	return ret;
}
#undef MAX_CLUSTER_MEMBERS

struct nfs4_recovery_backend rados_cluster_backend = {
	.recovery_init = rados_cluster_init,
	.recovery_shutdown = rados_cluster_shutdown,
	.recovery_read_clids = rados_cluster_read_clids,
	.end_grace = rados_cluster_end_grace,
	.add_clid = rados_kv_add_clid,
	.rm_clid = rados_kv_rm_clid,
	.add_revoke_fh = rados_kv_add_revoke_fh,
	.maybe_start_grace = rados_cluster_maybe_start_grace,
	.try_lift_grace = rados_cluster_try_lift_grace,
	.set_enforcing = rados_cluster_set_enforcing,
	.grace_enforcing = rados_cluster_grace_enforcing,
	.is_member = rados_cluster_is_member,
	.get_replicas = rados_cluster_get_replicas,
};

void rados_cluster_backend_init(struct nfs4_recovery_backend **backend)
{
	*backend = &rados_cluster_backend;
}
