#!/usr/bin/env python3
# coding: utf-8

import re
import time

from .base_test import BaseTest, ThreadQueryExecutor, wait_stopevent

# Rows of 400-byte keys give about 16 rows per leaf and 18 downlinks per
# internal page, so this many rows make a three-level tree: a root, 11 pages
# on level 1 and about 190 leaves.  The first level 1 page covers v < 289.
ROWS = 3000
KEY = "lpad(%s::text, 400, '0')"

WAIT_TIMEOUT = 60


class TblCheckEvictionTest(BaseTest):
	"""The B-tree walk of check_btree() must survive the tree changing.

	check_walk_btree() unlocks the page it walks while it walks a child,
	waits for a downlink IO or loads an on-disk child.  Single-page eviction
	takes no relation lock, so meanwhile the page can be evicted and its
	pool slot reused, or a merge of its children can delete downlinks from
	it without changing its change count.  The walk used to go on with the
	stale page image and locator, reading garbage downlinks: a segfault on
	release builds, Assert("blkno < orioledb_buffers_count") or PANIC
	"error reading downlink" with cassert.  It also waited for downlink IO
	with the page locked, a deadlock against whoever runs that IO.

	The 8MB tests are the reproducer from the report and only assert that
	the server survives: the walk makes the page clock write dirty pages of
	the checked tree, which the extent accounting does not expect yet.  The
	other tests park a backend at a stopevent to make one change at a time,
	and go through verify_orioledb(), which shares the walk but only takes
	AccessShareLock on the table, so other sessions can evict pages of it.
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
		node.execute("SELECT orioledb_tbl_check('t0'::regclass);")
		node.execute("SELECT orioledb_tbl_check('t1'::regclass);")

	def test_tbl_check_under_eviction(self):
		self.check_under_eviction(False)

	def test_tbl_check_under_eviction_indices(self):
		self.check_under_eviction(True)

	def start_with_tree(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE tw (
				k text COLLATE "C" PRIMARY KEY,
				v int NOT NULL
			) USING orioledb;
			INSERT INTO tw SELECT %s, g FROM generate_series(1, %d) g;
		""" % (KEY % 'g', ROWS))
		node.execute("CHECKPOINT;")
		self.assertEqual(self.levels(), [2, 1, 0])

	def levels(self):
		structure = self.node.execute(
		    "SELECT orioledb_tbl_structure('tw'::regclass, 'n', 3);")[0][0]
		return sorted(
		    set(int(m) for m in re.findall(r'level = (\d+)', structure)),
		    reverse=True)

	def first_parent_downlinks(self):
		structure = self.node.execute(
		    "SELECT orioledb_tbl_structure('tw'::regclass, 'n', 2);")[0][0]
		for page in structure.split("\nPage ")[1:]:
			if "level = 1," in page:
				return len(re.findall(r'\bItem \d+:', page))
		self.fail("no level 1 page in:\n%s" % structure)

	def sparse_leaves_sql(self):
		# Keeps one row of every leaf of the first level 1 page but its first
		# two, so evicting those leaves merges them and deletes their
		# downlinks from that page.
		return ("DELETE FROM tw WHERE k BETWEEN %s AND %s AND mod(v, 16) <> 0;" %
		        (KEY % 33, KEY % 288))

	def verify_sql(self):
		return "SELECT * FROM verify_orioledb('tw'::regclass);"

	def wait_for(self, what, predicate):
		deadline = time.time() + WAIT_TIMEOUT
		while not predicate():
			if time.time() > deadline:
				self.fail("timed out waiting for %s" % what)
			time.sleep(0.01)

	def parked(self, pid):
		return self.node.execute(
		    "SELECT EXISTS (SELECT 1 FROM pg_stopevents() "
		    "WHERE waiter_pids @> ARRAY[%d]);" % pid)[0][0]

	def join_or_crash(self, thread, what):
		result = thread.join(WAIT_TIMEOUT)
		if thread.is_alive():
			# A deadlocked backend ignores the fast shutdown of tearDown().
			self.node.stop(['-m', 'immediate'])
			self.fail("%s did not finish" % what)
		return result

	def test_downlink_io_wait(self):
		"""The walk must wait for a downlink IO with the page unlocked.

		A reader parked in load_page() holds the IO of a leaf and needs the
		lock on its parent to finish it.  The walk reaches that downlink and
		waits.  The leaf must be walked after the wait, not skipped, so the
		check has to come out clean.
		"""
		node = self.node
		self.start_with_tree()
		node.execute("SELECT orioledb_evict_pages('tw'::regclass, 0);")

		reader = node.connect()
		walker = node.connect()
		reader_pid = reader.pid
		walker_pid = walker.pid
		node.execute("SELECT pg_stopevent_set('load_page_refind', "
		             "'$pid == %d');" % reader_pid)
		t_reader = ThreadQueryExecutor(
		    reader, "SELECT v FROM tw WHERE k = %s;" % (KEY % 1500))
		t_reader.start()
		wait_stopevent(node, reader_pid)

		t_walker = ThreadQueryExecutor(walker, self.verify_sql())
		t_walker.start()
		self.wait_for(
		    "the walk to wait for the reader's IO", lambda: node.execute(
		        "SELECT wait_event FROM pg_stat_activity WHERE pid = %d;" %
		        walker_pid)[0][0] == 'orioledb_btree_io')

		node.execute("SELECT pg_stopevent_reset('load_page_refind');")
		self.assertEqual(self.join_or_crash(t_reader, "reader"), [(1500, )])
		self.assertEqual(self.join_or_crash(t_walker, "walk"), [])
		reader.close()
		walker.close()

	def test_parent_merged_during_load_page(self):
		"""The walk must resume where load_page() refound the parent.

		The walk is parked in load_page() of the first leaf of a page while
		merges of other leaves delete downlinks from that page, leaving its
		change count as it was.  load_page() refinds the page by key, and the
		walk has to go on from there rather than from its own locator.
		"""
		node = self.node
		self.start_with_tree()
		node.execute("SELECT orioledb_evict_pages('tw'::regclass, 0);")
		node.execute(self.sparse_leaves_sql())
		downlinks = self.first_parent_downlinks()

		walker = node.connect()
		walker_pid = walker.pid
		node.execute("SELECT pg_stopevent_set('load_page_refind', "
		             "'$pid == %d && $.level == 0');" % walker_pid)
		t_walker = ThreadQueryExecutor(walker, self.verify_sql())
		t_walker.start()
		wait_stopevent(node, walker_pid)

		node.execute("SELECT orioledb_evict_pages('tw'::regclass, 0);")
		self.assertLess(self.first_parent_downlinks(), downlinks)
		node.execute("SELECT pg_stopevent_reset('load_page_refind');")
		self.join_or_crash(t_walker, "walk")
		walker.close()

		node.execute("CHECKPOINT;")
		self.assertEqual(node.execute(self.verify_sql()), [])

	def test_parent_evicted_while_unlocked(self):
		"""The walk must refind a page that was evicted while unlocked.

		Every time the walk comes back from a leaf, its level 1 page and all
		the leaves are evicted and read back, mostly into other pool slots.
		That covers resuming in the middle of a page, after its last
		downlink by its hikey, and after the last downlink of the rightmost
		page.  All the pages are clean, so the extents do not change and the
		check has to come out clean: no subtree skipped or walked twice.
		"""
		node = self.node
		self.start_with_tree()

		walker = node.connect()
		walker_pid = walker.pid

		# Setting the next condition releases the walk parked on the
		# previous one, so no downlink slips through between the two.
		def park_after(row):
			key_cond = '$.key == null'
			if row is not None:
				key_cond += ' || $.key.k > "%s"' % str(row).zfill(400)
			node.execute(
			    "SELECT pg_stopevent_set('check_walk_refind', "
			    "'$pid == %d && $.treeName == \"tw_pkey\" && $.level == 1 "
			    "&& $.walked == true && (%s)');" % (walker_pid, key_cond))

		park_after(0)
		t_walker = ThreadQueryExecutor(walker, self.verify_sql())
		t_walker.start()

		cycles = 0
		for row in range(16, ROWS + 16, 16):
			self.wait_for(
			    "the walk to park or finish",
			    lambda: not t_walker.is_alive() or self.parked(walker_pid))
			if not t_walker.is_alive():
				break
			node.execute("SELECT orioledb_evict_pages('tw'::regclass, 1);")
			node.execute("SELECT count(*) FROM tw;")
			cycles += 1
			park_after(row if row < ROWS else None)

		node.execute("SELECT pg_stopevent_reset('check_walk_refind');")
		self.assertEqual(self.join_or_crash(t_walker, "walk"), [])
		walker.close()
		self.assertGreater(cycles, 150)

	def test_parent_merged_while_unlocked(self):
		"""The walk must refind its place after a merge into its page.

		The walk is parked on a level 1 page coming back from its first leaf
		while merges of the next leaves delete downlinks from that page,
		leaving its change count as it was.
		"""
		node = self.node
		self.start_with_tree()
		node.execute(self.sparse_leaves_sql())
		downlinks = self.first_parent_downlinks()

		walker = node.connect()
		walker_pid = walker.pid
		node.execute(
		    "SELECT pg_stopevent_set('check_walk_refind', "
		    "'$pid == %d && $.treeName == \"tw_pkey\" && $.level == 1');" %
		    walker_pid)
		t_walker = ThreadQueryExecutor(walker, self.verify_sql())
		t_walker.start()
		wait_stopevent(node, walker_pid)

		node.execute("SELECT orioledb_evict_pages('tw'::regclass, 0);")
		self.assertLess(self.first_parent_downlinks(), downlinks)
		node.execute("SELECT pg_stopevent_reset('check_walk_refind');")
		self.join_or_crash(t_walker, "walk")
		walker.close()

		node.execute("CHECKPOINT;")
		self.assertEqual(node.execute(self.verify_sql()), [])
