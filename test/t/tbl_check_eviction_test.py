#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class TblCheckEvictionTest(BaseTest):
	"""orioledb_tbl_check() must survive eviction of the checked tree.

	check_walk_btree() releases the page lock while it walks into children,
	waits for downlink IO and loads on-disk pages.  The background writer
	evicts pages without taking any relation lock, so during those windows
	the page being scanned may be evicted and its pool slot reused.  The
	walk used to keep scanning the stale page image, reading garbage
	downlinks: a segfault on release builds, an Assert("blkno <
	orioledb_buffers_count") or a PANIC "error reading downlink" with
	cassert.  It also waited for IO completion while holding the page lock,
	deadlocking against the background writer, whose eviction needs that
	same lock to finish the IO (see the check Walks section of
	src/btree/check.c).
	"""

	def check_under_eviction(self, t0_with_indices):
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()  # start PostgreSQL
		indices_sql = """
						CREATE INDEX ON t0 USING hash (v);
						CREATE INDEX ON t0 (j, v);
						CREATE UNIQUE INDEX ON t0 (i, j);
					""" if t0_with_indices else ""
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS t0 (
				i bigint NOT NULL,
				j bigint NOT NULL,
				v bigint NOT NULL,
				payload text
			) USING orioledb;
			%s
			INSERT INTO t0 SELECT g, g %% 97, g * 2, repeat('x', 2000)
				FROM generate_series(1, 2000) g;
			CREATE TABLE IF NOT EXISTS t1 (
				i bigint NOT NULL,
				payload text
			) USING orioledb WITH (compress = 5);
			INSERT INTO t1 SELECT g, repeat('x', 2000)
				FROM generate_series(1, 2000) g;
			DELETE FROM t1 WHERE i %% 2 = 0;
		""" % indices_sql)
		# Only survival is asserted: the extent accounting under eviction is
		# a separate bug (tbl_check_free_extents_test.py).
		node.execute("SELECT orioledb_tbl_check('t0'::regclass);")
		node.execute("SELECT orioledb_tbl_check('t1'::regclass);")

	def test_tbl_check_under_eviction(self):
		self.check_under_eviction(False)

	def test_tbl_check_under_eviction_indices(self):
		self.check_under_eviction(True)
