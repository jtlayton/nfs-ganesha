#ifndef _RADOS_GRACE_H
#define _RADOS_GRACE_H
/*
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * ---------------------------------------
 */
int rados_grace_create(rados_ioctx_t io_ctx, const char *oid);
int rados_grace_dump(rados_ioctx_t io_ctx, const char *oid);
int rados_grace_start(rados_ioctx_t io_ctx, const char *oid, int nodes,
			char **nodeids);
int rados_grace_join(rados_ioctx_t io_ctx, const char *oid, uint32_t nodeid,
			uint64_t *pcur, uint64_t *prec);
int rados_grace_lift(rados_ioctx_t io_ctx, const char *oid, int nodes,
			char **nodeids);
int rados_grace_done(rados_ioctx_t io_ctx, const char *oid, uint32_t nodeid,
			uint64_t *pcur, uint64_t *prec);
#endif /* _RADOS_GRACE_H */
