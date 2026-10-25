/*-------------------------------------------------------------------------
 *
 * check.c
 *		Routines for checking OrioleDB B-tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/check.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/check.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "catalog/free_extents.h"
#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "utils/compress.h"
#include "utils/memdebug.h"
#include "utils/page_pool.h"
#include "utils/seq_buf.h"
#include "utils/stopevent.h"
#include "utils/ucm.h"

#include "pgstat.h"
#include "access/transam.h"
#include "storage/latch.h"

/*
 * Dynamic array of file extents.
 */
typedef struct
{
	/* array of extents */
	FileExtent *extents;
	/* number of allocated extents */
	uint64		allocated;
	/* number of valid extents in the array */
	uint64		size;
	/* number of blocks in file containing by extents in the array */
	uint64		blocksCount;
} ExtentsArray;

typedef struct BTreeWalk BTreeWalk;

/*
 * A walk over every page of a B-tree, which loads the on-disk ones.  Each
 * page is visited after its subtree: visit_locked with the page locked, then
 * visit_unlocked (if set) for work too slow to do under the page lock.
 */
struct BTreeWalk
{
	BTreeDescr *desc;
	OBTreeFindPageContext context;
	void		(*visit_locked) (BTreeWalk *walk, OInMemoryBlkno blkno);
	void		(*visit_unlocked) (BTreeWalk *walk);
};

typedef struct
{
	BTreeWalk	walk;
	ExtentsArray busy;
	bool		hasError;
	bool		emit_transient_notices;
} BTreeCheckStatus;

typedef struct
{
	BTreeWalk	walk;
	BTreeCompressStats *stats;
	OCompress	lvl;
	char		img[ORIOLEDB_BLCKSZ];
} BTreeCompressWalk;

static int	file_extent_cmp(const void *p1, const void *p2);
static void walk_btree(BTreeWalk *walk, OInMemoryBlkno blkno,
					   OInMemoryBlkno parentPagenum);
static void check_visit_page(BTreeWalk *walk, OInMemoryBlkno blkno);
static void add_extent(ExtentsArray *arr, FileExtent extent);
static bool check_extents(ExtentsArray *busy, ExtentsArray *free);
static void get_free_extents(BTreeDescr *desc, ExtentsArray *free_extents,
							 bool force_file_check, uint32 chkp_num);
static void get_free_extents_from_file(SeqBufTag *tag, off_t offset,
									   ExtentsArray *free_extents,
									   bool compressed, bool should_exists);
static void get_free_extents_from_seqbuf_pending(SeqBufDescPrivate *seqBuf,
												 SeqBufTag *expected_tag,
												 ExtentsArray *free_extents,
												 bool compressed);
static void decode_free_extent_buf(const char *buf, Size len,
								   ExtentsArray *free_extents, bool compressed);
static bool is_sorted_by_off(ExtentsArray *array);
static bool is_sorted_by_len_off(ExtentsArray *array);

bool
check_btree(BTreeDescr *desc, bool force_file_check, bool wait_for_checkpoint)
{
	bool		is_sys_tree = IS_SYS_TREE_OIDS(desc->oids);
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	BTreeCheckStatus status;
	ExtentsArray free_extents;
	uint64		data_file_len = pg_atomic_read_u64(&metaPageBlkno->datafileLength[0]);	/* Fix for S3 mode */
	bool		is_compressed = OCompressIsValid(desc->compress);
	uint32		checkpoint_number = 0;
	bool		copy_blkno;

	memset(&status, 0, sizeof(BTreeCheckStatus));
	memset(&free_extents, 0, sizeof(ExtentsArray));

	/* get busy file extents */
	status.walk.desc = desc;
	status.walk.visit_locked = check_visit_page;
	status.walk.visit_unlocked = NULL;
	status.hasError = false;

	/*
	 * Legacy debug entry points (wait_for_checkpoint == false) keep the
	 * BROKEN_SPLIT NOTICE; the user-facing verify_orioledb path stays silent
	 * on that transient.
	 */
	status.emit_transient_notices = !wait_for_checkpoint;
	init_page_find_context(&status.walk.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	/*
	 * get_checkpoint_number() returns false while the checkpointer holds the
	 * per-tree state.  With wait_for_checkpoint, retry until it moves past;
	 * otherwise the caller is a debug entry point and gets the legacy NOTICE.
	 */
	while (!get_checkpoint_number(desc, desc->rootInfo.rootPageBlkno,
								  &checkpoint_number, &copy_blkno))
	{
		if (!wait_for_checkpoint)
		{
			elog(NOTICE, "Tree is under checkpoint now");
			return false;
		}

		/*
		 * Holding the per-tree lock here would block the checkpointer we are
		 * waiting on, so drop it across the latch wait and reacquire after.
		 * Sys trees serialize against the checkpointer via oSysTreesLock.
		 */
		if (is_sys_tree)
			LWLockRelease(&checkpoint_state->oSysTreesLock);
		else
			o_tables_rel_unlock_extended(&desc->oids, AccessExclusiveLock, true);
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (is_sys_tree)
			LWLockAcquire(&checkpoint_state->oSysTreesLock, LW_EXCLUSIVE);
		else
			o_tables_rel_lock_extended(&desc->oids, AccessExclusiveLock, true);
	}

	Assert(checkpoint_number > 0);

	walk_btree(&status.walk, desc->rootInfo.rootPageBlkno, OInvalidInMemoryBlkno);

	if (status.hasError)
		return false;

	if (desc->storageType != BTreeStoragePersistence)
		return true;

	/* get free file extents */
	get_free_extents(desc, &free_extents, force_file_check,
					 checkpoint_number - 1);

	if (status.hasError)
		return false;

	/* check extents */
	status.hasError = !check_extents(&status.busy, &free_extents);

	if (status.hasError)
		return false;

	if (data_file_len > status.busy.blocksCount + free_extents.blocksCount)
	{
		elog(NOTICE, "Not used file blocks from " UINT64_FORMAT " to " UINT64_FORMAT,
			 status.busy.blocksCount + free_extents.blocksCount,
			 data_file_len);
		status.hasError = true;
	}
	else if (data_file_len < status.busy.blocksCount + free_extents.blocksCount)
	{
		elog(NOTICE, "Excess file blocks from " UINT64_FORMAT " to " UINT64_FORMAT,
			 data_file_len,
			 status.busy.blocksCount + free_extents.blocksCount);
		status.hasError = true;
	}

	/* frees allocated bytes */
	if (status.busy.size > 0)
	{
		Assert(status.busy.extents != NULL);
		pfree(status.busy.extents);
	}

	if (free_extents.size > 0)
	{
		Assert(free_extents.extents != NULL);
		pfree(free_extents.extents);
	}

	if (checkpoint_number > 1)
	{
		/* file extents sort check */
		SeqBufTag	tag;
		ExtentsArray map_extents,
					tmp_extents;
		bool		found;

		memset(&map_extents, 0, sizeof(ExtentsArray));
		memset(&tmp_extents, 0, sizeof(ExtentsArray));

		tag.key.oids = desc->oids;
		tag.key.oids.spcoid = desc->oids.spcoid;
		tag.num = o_get_latest_chkp_num(desc->oids.datoid,
										desc->oids.relnode,
										desc->oids.spcoid,
										checkpoint_number - 1,
										&found);
		tag.type = 'm';


		if (seq_buf_file_exist(&tag))
		{
			get_free_extents_from_file(&tag, sizeof(CheckpointFileHeader),
									   &map_extents, is_compressed, false);
		}
		else if (found)
		{
			elog(NOTICE, "%s not exist", get_seq_buf_filename(&tag));
			status.hasError = true;
		}

		tag.type = 't';
		if (!is_compressed && seq_buf_file_exist(&tag))
		{
			get_free_extents_from_file(&tag, 0, &tmp_extents, false, false);
		}

		if (map_extents.size != 0)
		{
			bool		sorted = is_compressed ? is_sorted_by_len_off(&map_extents)
				: is_sorted_by_off(&map_extents);

			if (!sorted)
			{
				tag.type = 'm';
				elog(NOTICE, "%s file is not sorted", get_seq_buf_filename(&tag));
				status.hasError = true;
			}
			pfree(map_extents.extents);
		}

		if (tmp_extents.size != 0)
		{
			bool		sorted = is_compressed ? is_sorted_by_len_off(&tmp_extents)
				: is_sorted_by_off(&tmp_extents);

			if (!sorted)
			{
				tag.type = 't';
				elog(NOTICE, "%s file is not sorted", get_seq_buf_filename(&tag));
				status.hasError = true;
			}
			pfree(tmp_extents.extents);
		}
	}

	return !status.hasError;
}

/*
 * Appends extent into the extents array.
 */
static void
foreach_extent_append(BTreeDescr *desc, FileExtent extent, void *arg)
{
	ExtentsArray *arr = (ExtentsArray *) arg;

	add_extent(arr, extent);
}

/*
 * Gets free file extents for an index.
 */
static void
get_free_extents(BTreeDescr *desc, ExtentsArray *free_extents,
				 bool force_file_check, uint32 chkp_num)
{
	SeqBufTag	chkp_tag;
	bool		is_compressed = OCompressIsValid(desc->compress);

	chkp_tag.key.oids = desc->oids;
	chkp_tag.key.oids.spcoid = desc->oids.spcoid;

	if (force_file_check)
	{
		bool		found;
		uint32		num;
		uint32		map_num;

		/*
		 * Reads free blocks from map file.
		 */
		chkp_tag.type = 'm';
		chkp_tag.num = o_get_latest_chkp_num(desc->oids.datoid,
											 desc->oids.relnode,
											 desc->oids.spcoid,
											 chkp_num,
											 &found);

		get_free_extents_from_file(&chkp_tag, sizeof(CheckpointFileHeader),
								   free_extents, is_compressed, found);

		/*
		 * The map file only knows what was free when it was written.  Every
		 * extent freed since -- by the bgwriter rewriting a page, by a merge,
		 * by a copy-blkno rewrite -- is in a .tmp file or still buffered in
		 * the matching desc->tmpBuf[], and without them each of those shows
		 * up as an "Extent ... is neither free or busy" against a tree that
		 * is perfectly sound.  The branch below already reads both for the
		 * non-forced path; do the same here.
		 */
		map_num = chkp_tag.num;
		for (num = map_num; num <= chkp_num; num++)
		{
			SeqBufTag	tmp_tag = chkp_tag;

			tmp_tag.num = num + 1;
			tmp_tag.type = 't';
			get_free_extents_from_file(&tmp_tag, 0, free_extents,
									   is_compressed, false);
			get_free_extents_from_seqbuf_pending(&desc->tmpBuf[(num + 1) % 2],
												 &tmp_tag, free_extents,
												 is_compressed);
		}
	}
	else if (!is_compressed)
	{
		/*
		 * Reads free blocks as normal process for uncompressed index.
		 */
		off_t		freebuf_offset;
		uint32		num;

		chkp_tag = desc->freeBuf.shared->tag;
		freebuf_offset = seq_buf_get_offset(&desc->freeBuf);

		get_free_extents_from_file(&chkp_tag, freebuf_offset, free_extents, false, false);

		/*
		 * Read every .tmp file whose data could still feed the free-extent
		 * stream: anything written for the previous checkpoint (already
		 * flushed) up to chkp_num + 1 (the "next" checkpoint accepting fresh
		 * frees from in-flight merges and from copy-blkno page rewrites).
		 * Mirrors the compressed branch below, which already extends the
		 * range to chkp_num + 1.  Stopping at chkp_num leaves out frees that
		 * are sitting in chkp.{chkp_num+1}.tmp and produces a phantom "Extent
		 * ... is neither free or busy" report.
		 *
		 * For each .tmp file we also drain the matching desc->tmpBuf[] in
		 * shared memory.  free_extent_for_checkpoint() buffers small writes
		 * in an 8 KB seq_buf page; until the chunk fills or the checkpoint
		 * finalises, the bytes are not visible via the on-disk file read.
		 * Appending them here closes that read-side race.
		 */
		for (num = chkp_tag.num; num <= chkp_num; num++)
		{
			chkp_tag.num = num + 1;
			chkp_tag.type = 't';
			get_free_extents_from_file(&chkp_tag, 0, free_extents, false, false);
			get_free_extents_from_seqbuf_pending(&desc->tmpBuf[(num + 1) % 2],
												 &chkp_tag, free_extents,
												 false);
		}
	}
	else
	{
		/*
		 * Reads free blocks as normal process for compressed index.
		 * foreach_free_extent reads from in-memory free extent trees, which
		 * contain extents from consumed .tmp files.  We also need to read any
		 * unconsumed .tmp files.
		 */
		BTreeMetaPage *metaPage = BTREE_GET_META(desc);
		uint32		num;
		uint32		consumed_num = metaPage->freeBuf.tag.num;

		foreach_free_extent(desc, foreach_extent_append, (void *) free_extents);
		for (num = consumed_num + 1; num <= chkp_num + 1; num++)
		{
			chkp_tag.num = num;
			chkp_tag.type = 't';
			get_free_extents_from_file(&chkp_tag, 0, free_extents, true, false);
			/* Mirror the uncompressed-path append (see comment above). */
			get_free_extents_from_seqbuf_pending(&desc->tmpBuf[num % 2],
												 &chkp_tag, free_extents,
												 true);
		}
	}
}

/*
 * Decode a buffer of contiguous extent records into the free_extents array.
 * `len` is the number of bytes in `buf` to decode and must be a multiple of
 * the on-disk record size for this index.
 */
static void
decode_free_extent_buf(const char *buf, Size len, ExtentsArray *free_extents,
					   bool compressed)
{
	FileExtent	extent;
	uint32		off;
	Size		i = 0;

	while (i < len)
	{
		if (compressed || use_device)
		{
			memcpy(&extent, buf + i, sizeof(FileExtent));
			i += sizeof(FileExtent);
		}
		else
		{
			memcpy(&off, buf + i, sizeof(uint32));
			i += sizeof(uint32);
			extent.off = off;
			extent.len = 1;
		}
		add_extent(free_extents, extent);
	}
}

/*
 * Appends file extents from file to the free extents array.
 */
static void
get_free_extents_from_file(SeqBufTag *tag, off_t offset,
						   ExtentsArray *free_extents, bool compressed,
						   bool should_exists)
{
	char		buf[ORIOLEDB_BLCKSZ],
			   *filename;
	File		file;
	off_t		bytes_read;

	filename = get_seq_buf_filename(tag);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file == -1)
	{
		if (should_exists)
			ereport(NOTICE, (errcode_for_file_access(),
							 errmsg("could not open map file %s: %m", filename)));
		pfree(filename);
		return;
	}

	do
	{
		bytes_read = OFileRead(file, buf, ORIOLEDB_BLCKSZ, offset,
							   WAIT_EVENT_DATA_FILE_READ);
		offset += bytes_read;
		decode_free_extent_buf(buf, bytes_read, free_extents, compressed);
	} while (bytes_read == ORIOLEDB_BLCKSZ);

	FileClose(file);
	pfree(filename);
}

/*
 * Appends in-memory free-extent records that have been written to the given
 * write-mode seq_buf but are not yet flushed to its on-disk file.  Pair with
 * get_free_extents_from_file() to cover both halves of the stream when the
 * caller wants a snapshot that includes mid-checkpoint writes.
 */
static void
get_free_extents_from_seqbuf_pending(SeqBufDescPrivate *seqBuf,
									 SeqBufTag *expected_tag,
									 ExtentsArray *free_extents,
									 bool compressed)
{
	char	   *buf;
	Size		len;

	if (!SEQ_BUF_SHARED_EXIST(seqBuf->shared))
		return;
	if (!SeqBufTagEqual(&seqBuf->shared->tag, expected_tag))
		return;

	buf = palloc(seq_buf_max_pending_data_size());
	len = seq_buf_snapshot_pending_data(seqBuf, buf);
	if (len > 0)
		decode_free_extent_buf(buf, len, free_extents, compressed);
	pfree(buf);
}

/*
 * Returns true if the busy and free extents array do not intersect and have no
 * holes.
 */
static bool
check_extents(ExtentsArray *busy, ExtentsArray *free)
{
	FileExtent	cur;
	uint64		b,
				f,
				next_off;
	bool		result = true;

	qsort(busy->extents, busy->size, sizeof(FileExtent), file_extent_cmp);
	qsort(free->extents, free->size, sizeof(FileExtent), file_extent_cmp);

	b = 0;
	f = 0;
	cur.off = 0;
	cur.len = 0;
	while (true)
	{
		next_off = cur.off + cur.len;

		while (b < busy->size && next_off > busy->extents[b].off)
		{
			elog(NOTICE, "Excess busy extent %lu %u",
				 (unsigned long) busy->extents[b].off,
				 (unsigned) busy->extents[b].len);
			result = false;
			b++;
		}

		while (f < free->size && next_off > free->extents[f].off)
		{
			elog(NOTICE, "Excess free extent %lu %u",
				 (unsigned long) free->extents[f].off,
				 (unsigned) free->extents[f].len);
			result = false;
			f++;
		}

		if (f >= free->size && b >= busy->size)
			break;

		if (f >= free->size || (b < busy->size && file_extent_cmp(&free->extents[f], &busy->extents[b]) > 0))
		{
			if (next_off != busy->extents[b].off)
			{
				elog(NOTICE, "Extent %lu %u is neither free or busy",
					 (unsigned long) (next_off),
					 (unsigned) (busy->extents[b].off - next_off));
				result = false;
			}
			cur = busy->extents[b++];
		}
		else
		{
			if (next_off != free->extents[f].off)
			{
				elog(NOTICE, "Extent %lu %u is neither free or busy",
					 (unsigned long) (next_off),
					 (unsigned) (free->extents[f].off - next_off));
				result = false;
			}
			cur = free->extents[f++];
		}
	}

	return result;
}

/*
 * (off, len) sort comparator
 */
static int
file_extent_cmp(const void *p1, const void *p2)
{
	FileExtent	v1 = *((const FileExtent *) p1);
	FileExtent	v2 = *((const FileExtent *) p2);

	if (v1.off != v2.off)
		return v1.off > v2.off ? 1 : -1;
	if (v1.len != v2.len)
		return v1.len > v2.len ? 1 : -1;
	return 0;
}

/*
 * Returns false if the array is sorted by off order.
 */
static bool
is_sorted_by_off(ExtentsArray *array)
{
	uint64		i;
	bool		sorted = true;

	if (array->size > 1)
	{
		for (i = 1; i < array->size && sorted; i++)
		{
			sorted = array->extents[i - 1].off <= array->extents[i].off;
		}

		if (!sorted)
		{
			i--;
			elog(NOTICE, "Extents (%lu, %u), (%lu, %u) have wrong sort order",
				 (unsigned long) array->extents[i - 1].off,
				 (unsigned) array->extents[i - 1].len,
				 (unsigned long) array->extents[i].off,
				 (unsigned) array->extents[i].len);
		}
	}

	return sorted;
}

/*
 * Returns true if the array is sorted by (reverse len, off) order.
 */
static bool
is_sorted_by_len_off(ExtentsArray *array)
{
	uint64		i;
	bool		sorted = true;

	if (array->size > 1)
	{
		for (i = 1; i < array->size && sorted; i++)
		{
			if (array->extents[i - 1].len != array->extents[i].len)
				sorted = array->extents[i - 1].len > array->extents[i].len;
			else
				sorted = array->extents[i - 1].off <= array->extents[i].off;
		}

		if (!sorted)
		{
			i--;
			elog(NOTICE, "Extents (%lu, %u), (%lu, %u) have wrong sort order",
				 (unsigned long) array->extents[i - 1].off,
				 (unsigned) array->extents[i - 1].len,
				 (unsigned long) array->extents[i].off,
				 (unsigned) array->extents[i].len);
		}
	}

	return sorted;
}

/*
 * Appends the extent to the array.
 */
static void
add_extent(ExtentsArray *arr, FileExtent extent)
{
	if (arr->size >= arr->allocated)
	{
		if (arr->allocated == 0)
		{
			arr->allocated = 16;
			arr->extents = (FileExtent *) palloc(sizeof(FileExtent) * arr->allocated);
		}
		else
		{
			arr->allocated *= 2;
			arr->extents = (FileExtent *) repalloc(arr->extents,
												   sizeof(FileExtent) * arr->allocated);
		}
	}
	arr->extents[arr->size++] = extent;
	arr->blocksCount += extent.len;
}

/*
 * Lock again the non-leaf page this walk_btree() frame is on, and point loc at the
 * downlink whose subtree ends at the given key.
 *
 * The page was unlocked meanwhile, so its slot may hold another page by now,
 * and even when it does not, a merge may have deleted downlinks from it
 * without changing its change count.  Only a search by key finds the right
 * place in both cases.
 */
static void
walk_refind_page(BTreeWalk *walk, OFixedKey *key,
				 BTreeKeyType keyType, uint16 level, OInMemoryBlkno *blkno,
				 uint32 *pageChangeCount, BTreePageItemLocator *loc)
{
	OBTreeFindPageContext *context = &walk->context;
	OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;
	uint16		saved_flags = context->flags;

	BTREE_PAGE_FIND_SET(context, DOWNLINK_LOCATION);
	result = refind_page(context,
						 keyType == BTreeKeyRightmost ? NULL : (Pointer) &key->tuple,
						 keyType, level, *blkno, *pageChangeCount);
	context->flags = saved_flags;
	Assert(result == OFindPageResultSuccess);

	*blkno = context->items[context->index].blkno;
	*pageChangeCount = context->items[context->index].pageChangeCount;
	*loc = context->items[context->index].locator;
}

/*
 * Walk the B-tree down from the given page.  The caller holds the lock on
 * parentPagenum, which is released once this page is locked.  Returns with
 * no page locked, so the caller refinds its own page rather than relocking a
 * slot that may have been reused.
 *
 * Single-page eviction takes no relation lock, so whenever this page is
 * unlocked (to walk a child, to wait for a downlink IO, or in load_page())
 * it can be evicted or merged into.  The position is therefore kept as the
 * key the current downlink's subtree ends at.
 */
static void
walk_btree(BTreeWalk *walk, OInMemoryBlkno blkno, OInMemoryBlkno parentPagenum)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OBTreeFindPageContext *context = &walk->context;
	uint32		pageChangeCount;
	uint16		level;

	Assert(OInMemoryBlknoIsValid(blkno));

	lock_page(blkno);
	if (OInMemoryBlknoIsValid(parentPagenum))
		unlock_page(parentPagenum);

	context->index++;
	pageChangeCount = O_PAGE_GET_CHANGE_COUNT(p);
	context->items[context->index].blkno = blkno;
	context->items[context->index].pageChangeCount = pageChangeCount;
	level = PAGE_GET_LEVEL(p);

	if (!O_PAGE_IS(p, LEAF))
	{
		BTreePageItemLocator loc;
		BTreePageItemLocator nextLoc;
		OFixedKey	itemKey;
		BTreeKeyType itemKeyType;

		BTREE_PAGE_LOCATOR_FIRST(p, &loc);
		while (BTREE_PAGE_LOCATOR_IS_VALID(p, &loc))
		{
			BTreeNonLeafTuphdr *tuphdr;
			bool		walked;

			nextLoc = loc;
			BTREE_PAGE_LOCATOR_NEXT(p, &nextLoc);
			if (BTREE_PAGE_LOCATOR_IS_VALID(p, &nextLoc))
			{
				copy_fixed_page_key(walk->desc, &itemKey, p, &nextLoc);
				itemKeyType = BTreeKeyPageHiKey;
			}
			else if (!O_PAGE_IS(p, RIGHTMOST))
			{
				copy_fixed_hikey(walk->desc, &itemKey, p);
				itemKeyType = BTreeKeyPageHiKey;
			}
			else
			{
				clear_fixed_key(&itemKey);
				itemKeyType = BTreeKeyRightmost;
			}

			tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				walk_btree(walk, DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink),
						   blkno);
				walked = true;
			}
			else if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			{
				int			ionum = DOWNLINK_GET_IO_LOCKNUM(tuphdr->downlink);

				/*
				 * Whoever runs the IO locks this page to update the downlink
				 * before completing it, so waiting under the lock deadlocks.
				 */
				unlock_page(blkno);
				wait_for_io_completion(ionum);
				walked = false;
			}
			else if (DOWNLINK_IS_ON_DISK(tuphdr->downlink))
			{
				context->items[context->index].blkno = blkno;
				context->items[context->index].pageChangeCount = pageChangeCount;
				context->items[context->index].locator = loc;
				load_page(context);

				/* load_page() refinds this page by key, maybe at another slot. */
				blkno = context->items[context->index].blkno;
				pageChangeCount = context->items[context->index].pageChangeCount;
				loc = context->items[context->index].locator;
				p = O_GET_IN_MEMORY_PAGE(blkno);
				continue;
			}
			else
			{
				BTREE_PAGE_LOCATOR_NEXT(p, &loc);
				continue;
			}

			/*
			 * The page is unlocked here.  Tests park here to evict or merge
			 * into it before the refind.
			 */
			if (STOPEVENTS_ENABLED())
			{
				Jsonb	   *params;
				JsonbParseState *state = NULL;

				pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
				btree_desc_stopevent_params_internal(walk->desc, &state);
				jsonb_push_int8_key(&state, "level", level);
				jsonb_push_bool_key(&state, "walked", walked);
				if (itemKeyType == BTreeKeyRightmost)
					jsonb_push_null_key(&state, "key");
				else
				{
					jsonb_push_key(&state, "key");
					(void) o_btree_key_to_jsonb(walk->desc, itemKey.tuple, &state);
				}
				params = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));

				STOPEVENT(STOPEVENT_CHECK_WALK_REFIND, params);
			}

			walk_refind_page(walk, &itemKey, itemKeyType, level,
							 &blkno, &pageChangeCount, &loc);
			p = O_GET_IN_MEMORY_PAGE(blkno);
			if (walked)
				BTREE_PAGE_LOCATOR_NEXT(p, &loc);
		}
	}

	walk->visit_locked(walk, blkno);
	unlock_page(blkno);
	context->index--;

	if (walk->visit_unlocked)
		walk->visit_unlocked(walk);
}

static void
check_visit_page(BTreeWalk *walk, OInMemoryBlkno blkno)
{
	BTreeCheckStatus *status = (BTreeCheckStatus *) walk;
	BTreePageHeader *header = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	uint64		rightLink = header->rightLink;

	if (RightLinkIsValid(rightLink))
	{
		Page		rightP = O_GET_IN_MEMORY_PAGE(RIGHTLINK_GET_BLKNO(rightLink));

		/* An evicted local-pool slot is NULL. */
		if (rightP != NULL && O_PAGE_IS(rightP, BROKEN_SPLIT) &&
			status->emit_transient_notices)
		{
			elog(NOTICE, "BTree has a broken split.");
			status->hasError = true;
		}
	}

	if (FileExtentIsValid(page_desc->fileExtent))
		add_extent(&status->busy, page_desc->fileExtent);
}

static void
compress_visit_page(BTreeWalk *walk, OInMemoryBlkno blkno)
{
	BTreeCompressWalk *cw = (BTreeCompressWalk *) walk;

	ppool_ucm_inc_usage(walk->desc->ppool, blkno);
	memcpy(cw->img, O_GET_IN_MEMORY_PAGE(blkno), ORIOLEDB_BLCKSZ);
}

static void
compress_visit_unlocked(BTreeWalk *walk)
{
	BTreeCompressWalk *cw = (BTreeCompressWalk *) walk;
	BTreeCompressStats *stats = cw->stats;
	size_t		compressed_size;

	VALGRIND_MAKE_MEM_DEFINED(cw->img, ORIOLEDB_BLCKSZ);

	PG_TRY();
	{
		o_compress_page(cw->img, &compressed_size, cw->lvl);

		stats->totalSize += ORIOLEDB_BLCKSZ;
		stats->totalCompressedSize += compressed_size;

		if (compressed_size > ORIOLEDB_BLCKSZ)
		{
			stats->oversize++;
		}
		else
		{
			int			i;

			for (i = 0; i < stats->nranges; i++)
			{
				if (stats->ranges[i].from <= compressed_size
					&& compressed_size <= stats->ranges[i].to)
				{
					if (O_PAGE_IS(cw->img, LEAF))
						stats->ranges[i].leaf_count++;
					else
						stats->ranges[i].node_count++;
					break;
				}
			}
		}
	}
	PG_CATCH();
	{
		stats->errors++;
	}
	PG_END_TRY();
}

void
check_btree_compression(BTreeDescr *desc, BTreeCompressStats *stats, OCompress lvl)
{
	BTreeCompressWalk cw;
	bool		recovery = is_recovery_in_progress();

	o_tables_rel_lock_extended(&desc->oids, AccessShareLock, recovery);
	o_btree_load_shmem(desc);

	cw.walk.desc = desc;
	init_page_find_context(&cw.walk.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);
	cw.walk.visit_locked = compress_visit_page;
	cw.walk.visit_unlocked = compress_visit_unlocked;
	cw.stats = stats;
	cw.lvl = lvl;

	walk_btree(&cw.walk, desc->rootInfo.rootPageBlkno, OInvalidInMemoryBlkno);

	o_tables_rel_unlock_extended(&desc->oids, AccessShareLock, recovery);
}
