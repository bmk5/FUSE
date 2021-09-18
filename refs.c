
#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif
#include "refs.h"
#include <assert.h>
#include <strings.h>
#include <fuse.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

// The first thing we do in init is open the "disk file".
// We use this backing file as our "disk" for all I/O in ReFS.
static int disk_fd;

// Statically allocate the main data structures in our file system.
// Global variables are not ideal, but these are the main pillars
// of our file system state.
// These in-memory representations will be kept consistent with the on-disk
// representation by frequently writing portions of them to disk_fd

static union superblock super;

static union inode *inode_table;

static struct bitmap *inode_bitmap;

static struct bitmap *data_bitmap;


#define PATH_SEP "/"

/***********************/
/** Bitmap management **/
/***********************/

/**
 * Read a bitmap from disk into an in-memory representation.
 * @pre bmap points to an allocated struct bitmap, and bmap->n_bytes
 *      and bmap->n_valid_bytes are valid values
 * @return 0 on success, negative value on failure
 */
static int read_bitmap_from_disk(struct bitmap *bmap, lba_t bmap_start) {
	// Our bitmap's region of valid bits may not exactly align
	// with a"block boundary", but we must do block-aligned I/Os
	// when talking to our disk.  First "round up" our bitmap size
	// to the appropriate number of bytes, then make the I/O
	// request to read those blocks into our structure.
	size_t bmap_blocks = BYTES_TO_BLKS(bmap->n_bytes);
	int ret = read_blocks(bmap->bits, bmap_blocks, bmap_start);
	assert(ret == bmap_blocks);

	if (ret != bmap_blocks) {
		DEBUG_PRINT("Error reading bitmap (%d)", ret);
		return -EIO;
	}
	return 0;
}

/**
 * Write just the "bits" of our in-memory bitmap to disk
 * @pre bmap points to an allocated struct bitmap, and bmap->n_bytes
 *      and bmap->bits have valid values
 * @return 0 on success, negative value on failure
 */
static int write_bitmap_to_disk(struct bitmap *bmap, lba_t bmap_start) {
	size_t bmap_blocks = BYTES_TO_BLKS(bmap->n_bytes);
	int ret = write_blocks(bmap->bits, bmap_blocks, bmap_start);

	assert(ret == bmap_blocks);

	if (ret != bmap_blocks) {
		DEBUG_PRINT("Error writing bitmap (%d)", ret);
		return -EIO;
	}

	return 0;

}

/**
 * Utility function that writes the data region's bitmap in its entirety.
 */
static int sync_data_bitmap() {
	return write_bitmap_to_disk(data_bitmap, super.super.d_bitmap_start);
}

/**
 * Utility function that writes the inode table's bitmap in its entirety.
 */
static int sync_inode_bitmap() {
	return write_bitmap_to_disk(inode_bitmap, super.super.i_bitmap_start);
}

/****************************/
/** Superblock management **/
/****************************/

static int write_super() {
	int ret = write_block(&super, SUPER_LBA);
	// The assumption is that a disk writes sectors atomically, so if this
	// fails, we've violated a key assumption of our system.
	assert(ret == 1);
	return 0;
}


/** Inode / Inode Table management **/

/**
 * Write back the disk block that holds a given inode number.
 * Note that we write back more than just that inode, since we must
 * do I/O at the granularity of a 4096-byte disk block.
 */
static int write_inode(uint64_t inum) {
	// First, calcluate the offset of the block that contains this
	// inode. We know the start of the inode table (it's stored in
	// our super block) and we know the number of inodes per block.
	uint64_t itab_block = inum / INODES_PER_BLOCK;

	// Now, write the portion of the inode table contained in that block
	// to the corresponding lba
	int ret = write_block(((void *)inode_table)+BLKS_TO_BYTES(itab_block),
			     super.super.i_table_start + itab_block);
	return ret == 1 ? 0 : ret;
}

/**
 * Write data to 4096-byte-aligned set of consecutive blocks
 * @param buf The memory buffer that has the data to be written
 * @param lba_count The number of blocks to write is (lba_count * 4096)
 * @param starting_lba The byte offset on our "disk" that we start writing
 *        is (starting_lba * 4096)
 * @return the number of blocks successfully written, or negative on error.
 */
ssize_t write_blocks(void *buf, size_t lba_count, off_t starting_lba) {
	int ret = pwrite(disk_fd, buf, BLKS_TO_BYTES(lba_count),
			 BLKS_TO_BYTES(starting_lba));
	if (ret < 0) {
		// pass along the error
		return ret;
	}

	// pwrite may return a partial success. If we don't write
	// a multiple of 4096 bytes, we have create a confusing situation.
	// it's not clear how to best handle this until we've built the rest
	// of our system
	if (BLOCK_OFFSET(ret) != 0) {
		// possibly handle incomplete write with retry?
		DEBUG_PRINT("partial write: write_blocks\n");
	}

	return BYTES_TO_BLKS(ret);
}

/**
 * Read data from a 4096-byte-aligned set of consecutive blocks
 * @param buf The memory buffer where we will store the data read
 * @param lba_count The number of blocks to read is (lba_count * 4096)
 * @param starting_lba The byte offset on our "disk" that we start reading
 *        is (starting_lba * 4096)
 * @return the number of blocks successfully read, or negative on error.
 */
ssize_t read_blocks(void *buf, size_t lba_count, off_t starting_lba) {
	int ret = pread(disk_fd, buf, BLKS_TO_BYTES(lba_count),
			BLKS_TO_BYTES(starting_lba));

	if (ret < 0) {
		// pass along the error
		return ret;
	}

	// pread may return partial success.
	// for read, the user could just ignore the data that
	// isn't "acknowleged", but we may want to revisit this
	// depending on our implementation
	if (BLOCK_OFFSET(ret) != 0) {
		// possibly handle incomplete read with retry?
		DEBUG_PRINT("partial write: write_blocks\n");
	}

	return BYTES_TO_BLKS(ret);
}

/**
 * Allocates an unused data block, and returns the lba of that data block
 * Since this requires knowing the data region start, we access the global
 * reference to the superblock.
 */
static lba_t reserve_data_block() {
	uint64_t index = first_unset_bit(data_bitmap);
	set_bit(data_bitmap, index);
	// convert our bit number to the actual LBA by adding the data
	// region's starting offset
	return super.super.d_region_start + index;
}

/**
 * Returns an allocated data block, which is specified by the lba.
 * Since this requires knowing the data region start, we access the global
 * reference to the superblock.
 */
static void release_data_block(lba_t lba) {
	assert(lba >= super.super.d_region_start);
	clear_bit(data_bitmap, lba - super.super.d_region_start);
}


/**
 * Allocates an unused inode, and returns a pointer to that inode via an
 * index into the inode table (which is also the inode's number).
 */
static int reserve_inode() {
	uint64_t inum = first_unset_bit(inode_bitmap);
	set_bit(inode_bitmap, inum);

	// initialize basic inode fields to clear any stale state
	memset(&inode_table[inum], 0, sizeof(inode_table[0]));

	inode_table[inum].inode.flags = INODE_IN_USE;

	return inum;
}

/**
 * allocates a new directory inode by setting the bitmap bits and
 * initializing the inode's type field in the inode_table
 */
static uint64_t reserve_reg_inode(mode_t mode) {
	uint64_t inum = reserve_inode();
	inode_table[inum].inode.mode = S_IFREG | mode;
	// We assume that a parent dir entry will refer to this inode
	inode_table[inum].inode.n_links = 1;
	return inum;
}


/**
 * allocates a new directory inode by setting the bitmap bits and
 * initializing the inode's type field in the inode_table
 */
static uint64_t reserve_dir_inode(mode_t mode) {
	uint64_t inum = reserve_inode();
	inode_table[inum].inode.mode = S_IFDIR | mode;
	inode_table[inum].inode.n_links = 2; // . and parent dir
	return inum;
}

/**
 *   allocates inode for new directory
 *   creates the data block for the directory's contents of (. and ..)
 *   writes bitmaps, directory data block, directory inode
 *     - DOES NOT update parent directory data to contain new dir
 */
static int alloc_dir_inode(uint64_t parent_inum, uint64_t *dir_inum,
			   mode_t mode) {
	uint64_t inum = reserve_dir_inode(mode);

	lba_t data_lba = reserve_data_block();


	// The root dir is a special case, where '.' and '..' both refer to
	// itself. We also require / have inum 0 to simplify our FS layout

	union directory_block *dir_data = zalloc_blocks(1);
	if (dir_data == NULL) {
		return -ENOMEM;
	}

	// init "."
	dir_data->dirents[0].inum = inum;
	dir_data->dirents[0].is_valid = 1;
	strcpy(dir_data->dirents[0].path, ".");
	dir_data->dirents[0].path_len = strlen(dir_data->dirents[0].path);

	// init ".."
	dir_data->dirents[1].inum = parent_inum;
	dir_data->dirents[1].is_valid = 1;
	strcpy(dir_data->dirents[1].path, "..");
	dir_data->dirents[1].path_len = strlen(dir_data->dirents[1].path);

	// update the inode's first direct pointer to point to this data
	inode_table[inum].inode.size = 4096;
	inode_table[inum].inode.blocks = 1;
	inode_table[inum].inode.block_ptrs[0] = data_lba;

	inode_table[inum].inode.time = time(NULL);
	struct fuse_context *ctx = fuse_get_context();

	inode_table[inum].inode.uid = ctx->uid;
	inode_table[inum].inode.gid = ctx->gid;

	// NOTE: we should error check these writes

	int ret;

	// write the directory contents to its data block
	ret = write_block(dir_data, data_lba);
	assert(ret == 1);

	// free malloced resources
	free(dir_data);

	// write the data bitmap
	ret = sync_data_bitmap();
	assert(ret == 0);

	// write back the inode
	ret = write_inode(inum);
	assert(ret == 0);

	// write back the inode bitmap
	ret = sync_inode_bitmap();
	assert(ret == 0);

	*dir_inum = inum;
	return ret;
}

/**
 *   allocates inode for new regular file
 *   writes inode bitmap and new inode
 *     - DOES NOT update parent directory data to contain new inode
 */
static int alloc_reg_inode(uint64_t *inum_result, mode_t mode) {
	uint64_t inum = reserve_reg_inode(mode);

	// update the inode's first direct pointer to point to this data
	inode_table[inum].inode.size = 0;
	inode_table[inum].inode.blocks = 0;

	inode_table[inum].inode.time = time(NULL);
	struct fuse_context *ctx = fuse_get_context();

	inode_table[inum].inode.uid = ctx->uid;
	inode_table[inum].inode.gid = ctx->gid;

	// NOTE: we should error check these writes

	int ret;

	// write back the inode
	ret = write_inode(inum);
	assert(ret == 0);

	// write back the inode bitmap
	ret = sync_inode_bitmap();
	assert(ret == 0);

	*inum_result = inum;
	return ret;
}


static void release_inode(struct refs_inode *ino) {
	assert(ino->flags & INODE_IN_USE);

	ino->flags = INODE_FREE;
	ino->n_links = 0;
	ino->size = 0;
	ino->blocks = 0;
	clear_bit(inode_bitmap, ino->inum);
	// don't bother"clearing" the block pointers because this inode is
	// logically free; future code should never interpret their values
}



static void alloc_inode_table(int num_inodes) {
	size_t itable_bytes =
		BLKS_TO_BYTES((num_inodes + (INODES_PER_BLOCK - 1)) /
			      INODES_PER_BLOCK);

	inode_table = malloc(itable_bytes);
	assert(inode_table != NULL);

	bzero(inode_table, itable_bytes);

	// initialize each inode
	for (uint64_t i = 0; i < DEFAULT_NUM_INODES; i++) {
		inode_table[i].inode.inum = i;
		inode_table[i].inode.flags = INODE_FREE;
	}
}

static void dump_super(struct refs_superblock *sb) {
	printf("refs_superblock:\n");
	printf("\tblock_size: %"PRIu32"\n", sb->block_size);
	printf("\tnum_inodes: %"PRIu64"\n", sb->num_inodes);
	printf("\timap_start:%"LBA_PRINT"\n", sb->i_bitmap_start);
	printf("\titab_start:%"LBA_PRINT"\n", sb->i_table_start);
	printf("\tnum_d_blks:%"PRIu64"\n", sb->num_data_blocks);
	printf("\tdmap_start:%"LBA_PRINT"\n", sb->d_bitmap_start);
	printf("\tdreg_start:%"LBA_PRINT"\n", sb->d_region_start);
	printf("\tddev:%ld\n", sb->dev);
}

static void init_super(struct refs_superblock *sb) {
	// Disk Layout:
	// super | imap | ...inode table... | dmap | ...data blocks... |

	sb->dev = 0x33321;

	sb->block_size = BLOCK_SIZE;
	sb->num_inodes = DEFAULT_NUM_INODES;
	sb->num_data_blocks = DEFAULT_NUM_DATA_BLOCKS;

	size_t imap_bytes = BLOCK_ROUND_UP(sb->num_inodes);
	lba_t imap_blocks = BYTES_TO_BLKS(imap_bytes);

	lba_t itab_blocks = (DEFAULT_NUM_INODES) / (INODES_PER_BLOCK);

	size_t dmap_bytes = BLOCK_ROUND_UP(sb->num_data_blocks);
	lba_t dmap_blocks = BYTES_TO_BLKS(dmap_bytes);

	sb->i_bitmap_start = 1;
	sb->i_table_start = sb->i_bitmap_start + imap_blocks;
	sb->d_bitmap_start = sb->i_table_start + itab_blocks;
	sb->d_region_start = sb->d_bitmap_start + dmap_blocks;
}

static void* refs_init(struct fuse_conn_info *conn) {
	int ret = 0;

	// check whether we need to initialize an empty file system
	// or if we can populate our existing file system from "disk"
	if (access(DISK_PATH, F_OK) == -1) {
		// In this cond branch, we don't have an existing "file
		// system" to start from, so we initialize one from scratch.
		// Typically, a separate program would do this (e.g., mkfs)
		// but this is not a typical file system...

		printf("creating new disk\n");

		// First, create a new "disk" file from scratch
		disk_fd = open(DISK_PATH, O_CREAT | O_EXCL | O_SYNC | O_RDWR,
			       S_IRUSR | S_IWUSR);
		assert(disk_fd > 0);


		// extend our "disk" file to 10 MiB (per lab spec)
		ret = ftruncate(disk_fd, 10*1024*1024);
		assert(ret >= 0);

		// now initialize the "empty" state of all of our data
		// structures in memory, then write that state to disk

		init_super(&super.super);
		dump_super(&super.super);

		inode_bitmap = allocate_bitmap(super.super.num_inodes);
		assert(inode_bitmap != NULL);

		data_bitmap = allocate_bitmap(super.super.num_data_blocks);
		assert(inode_bitmap != NULL);

		// allocate our inode table memory and populate initial vals
		alloc_inode_table(DEFAULT_NUM_INODES);

		// allocate inode for `/`, create the directory with . and ..
		uint64_t inum;
		ret = alloc_dir_inode(ROOT_INUM, &inum, S_IFDIR | 0775);
		assert(inum == ROOT_INUM);
		assert(ret == 0);

		// write superblock
		write_super();

		// done! now we have all of our metadata initialized and
		// written, and we can reinitialize our file system from
		// this on-disk state in future runs.
	} else {
		// In this cond. branch, we have already created an instance
		// of ReFS. Based on the saved state of our FS, we initialize
		// the in-memory state so that we can pick up where we left
		// off.

		// Step 1: open disk and read the superblock

		// Since super is statically allocated, we don't need
		// to do any memory allocation with malloc; we just
		// need to populate its contents by reading them from
		// "disk".  And since we need to access fields in our
		// super in order to know the sizes of our other
		// metadata structures, we read super first.

		disk_fd = open(DISK_PATH, O_SYNC | O_RDWR);
		assert(disk_fd > 0);

		// read superblock
		ret = read_block(&super, 0);
		dump_super(&super.super);


		// Step 2: allocate our other data structures in memory
		// and read from "disk" to populate your data structures

		// bitmaps
		inode_bitmap = allocate_bitmap(super.super.num_inodes);
		assert(inode_bitmap != NULL);

		data_bitmap = allocate_bitmap(super.super.num_data_blocks);
		assert(inode_bitmap != NULL);

		read_bitmap_from_disk(inode_bitmap,
				      super.super.i_bitmap_start);
		dump_bitmap(inode_bitmap);

		read_bitmap_from_disk(data_bitmap,
				      super.super.d_bitmap_start);
		dump_bitmap(data_bitmap);

		//inode table
		alloc_inode_table(super.super.num_inodes);
		ret = read_blocks(inode_table,
				  super.super.num_inodes / INODES_PER_BLOCK,
				  super.super.i_table_start);
		assert(ret == super.super.num_inodes / INODES_PER_BLOCK);
	}

	// before returning you should have your in-memory data structures
	// initialized, and your file system should be able to handle any
	// implemented system call

	printf("done init\n");
	return NULL;
}

/**
 * helper function to get inode number of a file / directory given
 * a parent inode and a child path component (not an absolute path)
 *
 * @return status code (0 success)
 * @post address referred to by child_inum is updated with the resolved
 *       child's inode number
 */
int resolve_child_inum(struct refs_inode *parent_dir,
		       const char *child_comp,
		       uint64_t *child_inum) {

	int ret = 0;

	// General Strategy:
	// for each valid block in our parent directory's contents,
	// we read that block and scan the entries for an entry that matches
	// child_comp
	// if we find one, we update child_inum and return 0
	// if we don't find an entry, return -ENOENT

	if (!S_ISDIR(parent_dir->mode)) {
		// parent must be a directory!
		DEBUG_PRINT("non-directory inode when resolving %s\n",
			    child_comp);
		return -ENOTDIR;
	}

	union directory_block *cur_block = zalloc_blocks(1);
	if (cur_block == NULL) {
		return -ENOMEM;
	}

	for (int block = 0; block < parent_dir->blocks; block++) {
		// read the current directory data block
		ret = read_block(cur_block, parent_dir->block_ptrs[block]);
		if (ret != 1) {
			ret = -EIO;
			goto free_block_out;
		}

		// walk through the directory entries, looking for child_path
		for (int i = 0; i < DIRENTS_PER_BLOCK; i++) {
			if (cur_block->dirents[i].is_valid &&
			    (strcmp(child_comp, cur_block->dirents[i].path) == 0)) {
				// found it! signal success (ret=0)
				ret = 0;
				// update "return value"
				*child_inum = cur_block->dirents[i].inum;
				goto free_block_out;
			}
		}
	}

	// if we got here, we didn't find the child.
	ret = -ENOENT;

free_block_out:
	free(cur_block);
	return ret;
}

/**
 * helper function to remove an entry from a parent directory
 *
 * @pre parent is a dir
 * @pre basename of abspath exists as a valid entry in parent
 */
int remove_child_abspath(struct refs_inode *parent, struct refs_inode *child,
			 const char *abspath) {
	int ret = 0;

	// for each valid block in our parent directory's contents,
	// we read that block and scan the entries for an entry that matches
	// the basename component of abspath
	// if we find one, we:
	//  - invalidate the entry,
	//  - decrement the parent's link count (removing .. entry from child)
	//  - decrment the child's link count (removing parent's entry)

	char *path_copy = strdup(abspath);
	if (path_copy == NULL)
		return -ENOMEM;

	char *base = basename(path_copy);

	union directory_block *cur_block = zalloc_blocks(1);
	if (cur_block == NULL) {
		ret = -ENOMEM;
		goto free_path_out;
	}

	for (int block = 0; block < parent->blocks; block++) {
		// read the current directory data block
		ret = read_block(cur_block, parent->block_ptrs[block]);
		if (ret != 1) {
			ret = -EIO;
			goto free_block_out;
		}

		// walk through the directory entries, looking for child_path
		for (int i = 0; i < DIRENTS_PER_BLOCK; i++) {
			if (cur_block->dirents[i].is_valid &&
			    (strcmp(base, cur_block->dirents[i].path) == 0)) {
				// found it! now remove it
				ret = 0;
				cur_block->dirents[i].is_valid = 0;

				ret = write_block(cur_block, parent->block_ptrs[block]);
				if (ret != 1) {
					ret = -EIO;
					goto free_block_out;
				}

				parent->n_links--;
				ret = write_inode(parent->inum);
				assert(ret == 0);

				child->n_links--;
				ret = write_inode(child->inum);
				assert(ret == 0);
				goto free_block_out;
			}
		}
	}
	// if we got here, we didn't find the child.
	ret = -ENOENT;

free_block_out:
	free(cur_block);
free_path_out:
	free(path_copy);
	return ret;
}



int resolve_abspath(const char *abspath, uint64_t *target_inum) {

	int ret = 0;
	uint64_t inum = ROOT_INUM;

	// simpleset case: if the path is the root ('/')
	if (!strcmp(abspath, "/")) {
		DEBUG_PRINT("path is root: '%s'\n", abspath);
		*target_inum = inum;
		return ret;
	}

	// make a copy of the string that we can maniuplate without fear
	// (path parameter is const)
	char *path_copy = strdup(abspath);

	// path_copy is NOT root, so we need to traverse the dir hierarchy
	// First, separate the dirname (parent directory) from
	// basename (final path component).

	char *parent_dir = dirname(path_copy);

	// We know that each component of our "parent_dir" path must be a
	// directory.
	// So we walk through each directory, starting from '/',
	// one component at a time.

	struct refs_inode *parent_inode = &inode_table[inum].inode;
	char *child_component = strtok(parent_dir, PATH_SEP);
	while (child_component != NULL) {
		// look up the child inode within the parent directory
		ret = resolve_child_inum(parent_inode, child_component, &inum);
		if (ret < 0) {
			DEBUG_PRINT("failed to resolve path %s\n", abspath);
			goto free_strs_out;
		}

		// advance to the next component of our directory's path
		child_component = strtok(NULL, PATH_SEP);

		// advance parent_inode to associated inode
		parent_inode = &inode_table[inum].inode;
	}

	free(path_copy);

	// Now `inum` refers to the target path's parent directory.
	// Get the final inode (which may or may not be a directory).

	// make a new copy of our path because dirname may be destructive
	path_copy = strdup(abspath);

	char *base = basename(path_copy);
	ret = resolve_child_inum(parent_inode, base, target_inum);
	if (ret < 0) {
		DEBUG_PRINT("failed to resolve path %s\n", abspath);
		goto free_strs_out;
	}


free_strs_out:
	free(path_copy);
	return ret;
}


/**
 * Helper method for access.
 * Checks the permissions of the file specified by path
 */
static int do_access(const char *abspath, int mask) {
	int ret;

	uint64_t inum;

	// look up the path
	ret = resolve_abspath(abspath, &inum);

	if (ret < 0) {
		return ret;
	}

	// if mask == F_OK, just check existence
	if (mask == F_OK)
		return 0;

	// Strategy: cobble together all of the capailitie that
	// the user posses based on their idenitity
	// This means we compare the caller's uid/gid against the
	// uid/gid of the file (we look at "other" regardless)
	struct fuse_context *ctx = fuse_get_context();

	// assemble all allowed permissions into one trio of bits

	int allowed_mode = 0;

	// user matches
	if (ctx->uid == inode_table[inum].inode.uid) {
		allowed_mode |= inode_table[inum].inode.mode & S_IRUSR ?
			R_OK : 0;
		allowed_mode |= inode_table[inum].inode.mode & S_IWUSR ?
			W_OK : 0;
		allowed_mode |= inode_table[inum].inode.mode & S_IXUSR ?
			X_OK : 0;
	}

	// group matches
	if (ctx->gid == inode_table[inum].inode.gid) {
		allowed_mode |= inode_table[inum].inode.mode & S_IRGRP ?
			R_OK : 0;
		allowed_mode |= inode_table[inum].inode.mode & S_IWGRP ?
			W_OK : 0;
		allowed_mode |= inode_table[inum].inode.mode & S_IXGRP ?
			X_OK : 0;
	}

	// always check "other"
	allowed_mode |= inode_table[inum].inode.mode & S_IROTH ? R_OK : 0;
	allowed_mode |= inode_table[inum].inode.mode & S_IWOTH ? W_OK : 0;
	allowed_mode |= inode_table[inum].inode.mode & S_IXOTH ? X_OK : 0;


	// confirm that if they ask for read permissions,
	// they are allowed to read, ...
	if ((mask & R_OK) && !(allowed_mode & R_OK))
		    return -EACCES;
	if ((mask & W_OK) && !(allowed_mode & W_OK))
		    return -EACCES;
	if ((mask & X_OK) && !(allowed_mode & X_OK))
		    return -EACCES;

	return 0;
}

/**
 * This is the same as the access(2) system call.
 * Note that it can be called on files, directories, or any other object
 * that appears in the filesystem.
 * This call is not required but it is recommended.
 *
 * @return -ENOENT if the path doesn't exist, -EACCESS if the requested
 *         permission isn't available, or 0 for success.
 */
static int refs_access(const char *abspath, int mask) {
	return do_access(abspath, mask);
}

/**
 * Return file attributes. (called by getattr and fgetattr).
 * The "stat" structure is described in detail in the stat(2) manual page.
 * For the given pathname, this should fill in the elements of the "stat" 
 * structure. If a field is meaningless or semi-meaningless (e.g., st_rdev),
 * then it should be set to 0 or given a "reasonable" value.
 * This call is pretty much required for a usable filesystem.
 *
 * NOTE: st_dev and st_ino are often used by application programs to decide
 * whether two file names are aliases for the same physical storage,
 * so setting them to 0 isn't ideal.
 *
 * @return 0 on succes, negative on error (see man 2 stat for error codes)
 */
static int do_getattr(const char* abspath, struct stat* stbuf) {
	// look up inode associated with `path`
	int ret = 0;
	uint64_t inum = 0;

	ret = resolve_abspath(abspath, &inum);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve %s\n", __func__, abspath);
		return ret;
	}

	// make up a dev number
	stbuf->st_dev = super.super.dev;
	stbuf->st_uid = inode_table[inum].inode.uid;
	stbuf->st_gid = inode_table[inum].inode.gid;
	stbuf->st_mode = inode_table[inum].inode.mode;
	stbuf->st_nlink = inode_table[inum].inode.n_links;
	stbuf->st_size = inode_table[inum].inode.size;
	stbuf->st_blocks = inode_table[inum].inode.blocks;
	stbuf->st_atime = inode_table[inum].inode.time;
	stbuf->st_mtime = inode_table[inum].inode.time;
	stbuf->st_ctime = inode_table[inum].inode.time;
	stbuf->st_ino = inum;


	return ret;
}

/**
 * Helper method to allocate an entry inside a parent directory's
 * data block. This just updates the in-memory state of the block by
 * setting the valid flag and copying the path. It does not set any
 * associated data structures or fields.
 *
 * @return 0 on success
 */
static int alloc_free_dirent(union directory_block *dir_block,
			     const char *child_comp,
			     uint64_t child_inum) {
	// walk through the directory entries, looking for child_comp
	for (int i = 0; i < DIRENTS_PER_BLOCK; i++) {
		if (!dir_block->dirents[i].is_valid) {
			// found an empty slot!
			strcpy(dir_block->dirents[i].path, child_comp);
			dir_block->dirents[i].path_len =
				strlen(child_comp);
			dir_block->dirents[i].inum = child_inum;
			dir_block->dirents[i].is_valid = 1;
			return 0;
		}
	}
	return -ENOSPC;
}

/**
 * helper function to allocate entry in parent data block.
 * The FS only supports directories with *direct* blocks.
 * This limits the total number of subdirectories to:
 *    (DIRENTS_PER_BLOCK * NUM_DIRECT - 2)
 */
static int add_directory_entry(struct refs_inode *parent_dir,
			       const char *child_comp,
			       uint64_t child_inum,
			       char child_is_dir) {

	int ret = 0;

	// if child
	if (!S_ISDIR(parent_dir->mode)) {
		// parent must be a directory!
		DEBUG_PRINT("non-directory inode when resolving %s\n",
			    child_comp);
		return -ENOTDIR;
	}

	// for each valid block in our parent directory's contents,
	// we read that block and scan the entries for an unallocated slot

	// allocated block to cache directory data
	union directory_block *cur_block = zalloc_blocks(1);
	if (cur_block == NULL) {
		return -ENOMEM;
	}

	// read through each data block and look for a free entry
	int block = 0;
	while (block < parent_dir->blocks) {
		// read the current directory data block
		ret = read_block(cur_block, parent_dir->block_ptrs[block]);
		if (ret != 1) {
			ret = -EIO;
			goto free_block_out;
		}

		ret = alloc_free_dirent(cur_block, child_comp, child_inum);
		if (ret == 0) {
			goto write_block_out;
		}
		block++;
	}

	// if we got here, we need to allocate a new data block for our
	// directory so we can add the new entry
	if (block < NUM_DIRECT) {
		// allocate a new data block (bitmap)
		lba_t lba = reserve_data_block();

		// update inode
		parent_dir->block_ptrs[block] = lba;
		parent_dir->blocks++;
		if (child_is_dir)
			parent_dir->n_links++;

		// zero cur_block (we've already allocated it in memory,
		// so we'll just use it as our dir's new block)
		bzero(cur_block, BLOCK_SIZE);

		ret = alloc_free_dirent(cur_block, child_comp, child_inum);
		if (ret == 0) {
			// successfully allocated entry in parent dir
			ret = write_block(cur_block, parent_dir->block_ptrs[block]);
			if (ret == 1) {
				ret = 0;
			} else {
				// unset bit
				release_data_block(lba);
				// remove block from inode
				parent_dir->blocks--;
				goto free_block_out;
			}
			ret = sync_data_bitmap();
			assert(ret == 0);

			ret = write_inode(parent_dir->inum);
			assert(ret == 0);

			goto free_block_out;
		}
	} else {
		// we've run out of blocks, so we can't allocate a child
		ret = -ENOSPC;
		goto free_block_out;
	}

write_block_out:
	// successfully allocated entry in parent dir
	ret = write_block(cur_block, parent_dir->block_ptrs[block]);
	if (ret == 1)
		ret = 0;

	if (child_is_dir) {
		parent_dir->n_links++;
		write_inode(parent_dir->inum);
	}

free_block_out:
	free(cur_block);
	return ret;
}


/**
 * Create a directory with the given name.
 * The directory permissions are encoded in mode.
 * See mkdir(2) for details.
 * This function is needed for any reasonable read/write filesystem.
 */
static int refs_mkdir(const char* abspath, mode_t mode) {
	int ret = 0;

	/*
	 * to create a directory, we need to:
	 *  1) Resolve the path to find the parent directory's inode
	 *  2) scan the parent directory's data for an exising file
	 *  3) If one does not exist,
	 *      - allocate a new directory inode for the new dir
	 *      - allocate a new directory data block for the new dir
	 *             fill it with "." and ".."
	 *        (note that this means we need to increment the parent's
	 *         link count since the ".." entry refers to it)
	 *      - allocate a new entry in the parent's directory contents
	 */
	uint64_t parent_inum;

	char *path_copy = strdup(abspath);
	char *parent_dir = dirname(path_copy);


	// 1) get the parent directory's inode
	ret = resolve_abspath(parent_dir, &parent_inum);
	if (ret < 0) {
		goto free_strs_out;
	}
	struct refs_inode *parent_inode = &inode_table[parent_inum].inode;
	free(path_copy);

	// 2) scan the parent for the existence of the dir
	uint64_t dir_inum;
	path_copy = strdup(abspath);
	char *base = basename(path_copy);
	ret = resolve_child_inum(parent_inode, base, &dir_inum);
	if (ret == 0) {
		DEBUG_PRINT("file exists %s\n", abspath);
		ret = -EEXIST;
		goto free_strs_out;
	}

	if (ret != -ENOENT)
		goto free_strs_out;

	// 3) no existing dir exists. create one
	ret = alloc_dir_inode(parent_inum, &dir_inum, mode);
	if (ret != 0)
		goto free_strs_out;

	// allocate a new entry in the parent's directory contents
	ret = add_directory_entry(parent_inode, base, dir_inum, 1);

free_strs_out:
	free(path_copy);
	return ret;
}


/**
 * Helper function to find the inode of the *parent directory* of
 * the file specfiied by abspath.
 * @return 0 on success
 * @post the value at the address stored in inum is updated with the
 *       parent's inode number
 */
int resolve_parent_inum_abspath(const char *abspath, uint64_t *inum) {
	char *path_copy = strdup(abspath);
	if (path_copy == NULL) {
		return -ENOMEM;
	}

	char *parent = dirname(path_copy);

	int ret = resolve_abspath(parent, inum);
	free(path_copy);
	return ret;
}

/**
 * Helper function to find the inode of the file specified by the
 * absolute path abspath. However, instead of doing a component-by-component
 * traversal, this function looks for the last component of the absolute
 * path inside the directory specified by parent_inode.
 *
 * @return 0 on success
 * @post the value at the address stored in inum is updated with the
 *       inode number of the file specifiec by abspath
 */
int resolve_child_inum_abspath(const char *path, uint64_t *inum,
			       struct refs_inode *parent_inode) {
	char *path_copy = strdup(path);
	if (path_copy == NULL) {
		return -ENOMEM;
	}

	char *base = basename(path_copy);
	int ret = resolve_child_inum(parent_inode, base, inum);
	free(path_copy);
	return ret;
}

/**
 * Helper function that returns  if the directory is empty
 */
static int dir_isempty(struct refs_inode *dir) {
	union directory_block data;

	for (int b = 0; b < dir->blocks; b++) {

		int ret = read_block(&data, dir->block_ptrs[b]);
		assert(ret == 1);

		for (int d = 0; d < DIRENTS_PER_BLOCK; d++) {
			// if a valid entry is not . or .., not empty
			if (data.dirents[d].is_valid &&
			    !((strcmp(".", data.dirents[d].path) == 0) ||
			      (strcmp("..", data.dirents[d].path) == 0))) {
				return 0;
			}
		}
	}

	// if we got here, we have checked every entry in every block,
	// and the directyr is empty. return "true"
	return 1;
}

/**
 * Remove the directory specified by child_inum from the parent directory
 * specified by parent_inum.
 * This should succeed only if the child directory is empty
 * (except for the standard "." and ".." entries).
 * @pre parent and child are both directories
 */
static int do_rmdir(uint64_t parent_inum, uint64_t child_inum,
		    const char *abspath) {

	struct refs_inode *p_inode = &inode_table[parent_inum].inode;
	struct refs_inode *c_inode = &inode_table[child_inum].inode;

	// confirm that child directory is empty,
	// with the exception of "." and ".."
	if (!dir_isempty(c_inode)) {
		return -ENOTEMPTY;
	}

	// remove child from parent directory
	int ret = remove_child_abspath(p_inode, c_inode, abspath);
	if (ret)
		return ret;

	// return child's inode (unset bitmap entry)
	clear_bit(inode_bitmap, child_inum);

	// return path's blocks (unset bitmap entries for each)
	for (int b = 0; b < c_inode->blocks; b++)
		release_data_block(c_inode->block_ptrs[b]);

	// clear path's inode (mark as unused)
	release_inode(c_inode);
	return 0;
}

/**
 * Remove the given directory.
 * This should succeed only if the directory is empty
 * (except for "." and "..").
 */
static int refs_rmdir(const char* abspath) {
	int ret = 0;
	uint64_t parent_inum = 0;
	uint64_t child_inum = 0;

	// look up inode associated with `path`
	ret = resolve_parent_inum_abspath(abspath, &parent_inum);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve parent path %s\n",
			    __func__, abspath);
		return ret;
	}

	struct refs_inode *parent_inode = &inode_table[parent_inum].inode;

	// look up child inode
	ret = resolve_child_inum_abspath(abspath, &child_inum, parent_inode);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve parent path %s\n",
			    __func__, abspath);
		return ret;
	}

	// confirm the target file is actually a directory
	struct refs_inode *dir_inode = &inode_table[child_inum].inode;
	if (!S_ISDIR(dir_inode->mode)) {
		return -ENOTDIR;
	}

	// now we've collected all of the necessary components,
	// so we can attempt the actual rmdir work.
	return do_rmdir(parent_inum, child_inum, abspath);
}

/**
 * Return one or more directory entries (struct dirent) to the caller.
 * This is one of the most complex FUSE functions.
 * It is related to, but not identical to, the readdir(2) and getdents(2)
 * system calls, and the readdir(3) library function.
 */
static int refs_readdir(const char* path,
		 void* buf,
		 fuse_fill_dir_t filler,
		 off_t offset,
		 struct fuse_file_info* fi) {
	int ret;

	uint64_t dir_inum;

	ret = resolve_abspath(path, &dir_inum);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve path %s\n", __func__, path);
		return ret;
	}

	// offset is the "next dirent" to check.
	// each directory block has DIRENTS_PER_BLOCK dirents
	// So we figure out (1) which block the dirent lives in, and
	// (2) which offset within that block
	int blocknum = offset / DIRENTS_PER_BLOCK;
	int off = offset % DIRENTS_PER_BLOCK;

	struct refs_inode dir_inode = inode_table[dir_inum].inode;
	for(int b = blocknum; b < dir_inode.blocks; b++) {
		union directory_block dirblock;
		ret = read_block(&dirblock, dir_inode.block_ptrs[b]);
		if (ret != 1) {
			// This doesn't correspond to a return value in the
			// readdir manpage, but it is the most appropriate
			// value to return.
			return -EIO;
		}

		// int j = (b == blocknum) ? off : 0;
		int j = off;
		if (b != blocknum)
			j = 0;
		for(; j < DIRENTS_PER_BLOCK; j++) {
			struct dir_entry dirent = dirblock.dirents[j];
			if(dirent.is_valid) {
				struct stat stbuf;
				// FUSE only looks at the inum and mode fields
				stbuf.st_mode = inode_table[dirent.inum].inode.mode;
				stbuf.st_ino = dirent.inum;

				printf("dirent.path = %s\n", dirent.path);
				printf("reading %s\n", dirent.path);
				if (filler(buf, dirent.path, &stbuf, b*DIRENTS_PER_BLOCK + j + 1))
					return 0;
			}
		}
	}

	return 0;
}

/**
 * Free all state that we allocated when we initialized our file system.
 */
static void refs_destroy(void* private_data) {
	free(inode_table);
	free_bitmap(data_bitmap);
	free_bitmap(inode_bitmap);
	fsync(disk_fd);
	close(disk_fd);
}

/**
 * Change the ownership of the file specified by path.
 * If either of the specified ids are -1, they are not updated
 */
static int refs_chown(const char* path, uid_t uid, gid_t gid) {
	uint64_t inum;
	int ret =  resolve_abspath(path, &inum);
	if (ret != 0)
		return ret;

	if (uid != -1)
		inode_table[inum].inode.uid = uid;
	if (gid != -1)
		inode_table[inum].inode.gid = gid;

	ret = write_inode(inum);
	assert(ret == 0);

	return 0;
}

/**
 * Change the permissions (NOT file type) of the file specified by path.
 */
static int refs_chmod(const char* path, mode_t mode) {
	uint64_t inum;
	int ret =  resolve_abspath(path, &inum);
	if (ret != 0)
		return ret;

	// isolate the permisison bits of the argument
	mode &= (S_IRWXU | S_IRWXG | S_IRWXO);

	// clear the permission bits of the inode, but preserve all other
	// bits in the mode
	inode_table[inum].inode.mode &= ~(S_IRWXU | S_IRWXG | S_IRWXO);

	// set just the permission bits
	inode_table[inum].inode.mode |= mode;

	ret = write_inode(inum);
	assert(ret == 0);

	return 0;
}


/**
 * When a file is opened, we check for existence and permissions
 * and return either success or an error code.
 * Check the fuse_file_info->flags field in fuse_common.h for more
 * details.
 */
static int refs_open(const char* path, struct fuse_file_info* fi) {
	int req_mode = 0;
	if ((fi->flags & O_RDONLY) == O_RDONLY)
		req_mode |= R_OK;
	if ((fi->flags & O_WRONLY) == O_WRONLY)
		req_mode |= W_OK;
	if ((fi->flags & O_RDWR) == O_RDWR)
		req_mode |= R_OK | W_OK;

	return do_access(path, req_mode);
}

/**
 * Release is called when FUSE is completely done with a file; at that
 * point, you can free up any temporarily allocated data structures.
 * Since we don't have any file handles or other temporary state,
 * this is a no-op. Just return success.
 * @return 0 in all cases
 */
static int refs_release(const char* path, struct fuse_file_info *fi) {
	// we don't have any persistent state in our files,
	// so there is nothing to do here!
	return 0;
}

/**
 * Roughly equivalent to the stat system call
 */
static int refs_getattr(const char* path, struct stat* stbuf) {
	return do_getattr(path, stbuf);
}

/**
 * Roughly equivalent to the stat system call.
 * Since we aren't using file handles, we can resuse our getattr work.
 */
static int refs_fgetattr(const char* path, struct stat* stbuf, struct fuse_file_info *fi) {
	return do_getattr(path, stbuf);
}

/**
 * If the file does not already exist, make an empty inode for a regular
 * file.
 */
static int refs_mknod(const char* abspath, mode_t mode, dev_t rdev) {

	uint64_t parent_inum = 0;
	uint64_t child_inum = 0;

	int ret = resolve_parent_inum_abspath(abspath, &parent_inum);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve parent path %s\n",
			    __func__, abspath);
		return ret;
	}

	struct refs_inode *parent_inode = &inode_table[parent_inum].inode;

	ret = resolve_child_inum_abspath(abspath, &child_inum, parent_inode);

	// error if the file exists
	if (ret == 0)
		return -EEXIST;


	// create a new inode for the child
	ret = alloc_reg_inode(&child_inum, mode);

	// allocate a new entry in the parent's directory contents
	char *path_copy = strdup(abspath);
	if (path_copy == NULL)
		return -ENOMEM;

	ret = add_directory_entry(parent_inode,
				  basename(path_copy),
				  child_inum, 0);
	free(path_copy);
	return ret;
}

static int refs_create(const char* path, mode_t mode,
		       struct fuse_file_info *fi) {
	return refs_mknod(path, S_IFREG | mode, 0);
}

/**
 * Helper function that releases the valid direct blocks associated
 * with an inode.
 */
static int release_direct_blocks(struct refs_inode *inode) {

	int blocks = inode->blocks > NUM_DIRECT ? NUM_DIRECT : inode->blocks;

	for (int b = 0; b < blocks; b++)
		release_data_block(inode->block_ptrs[b]);

	return 0;
}

/**
 * Helper function that releases the valid indirect blocks associated
 * with an inode.
 */
static int release_indirect_blocks(struct refs_inode *inode) {

	//TODO: students, implement support for indirect blocks

	// for each valid indirect block in the inode:
	//   read the indirect block into memory
	//   for each valid direct block in the indirect block:
	//       release the direct block
	//   release the indirect block

	return 0;
}

/**
 * Remove the non-directory file specified by child_inum from the parent
 * directory specified by parent_inum.
 * This does check link counts, but code elsewhere may need to be updated
 * to properly support hard links.
 *
 * @pre parent is a directory by child is not
 */
static int do_unlink(uint64_t parent_inum, uint64_t child_inum,
		     const char *abspath) {

	struct refs_inode *p_inode = &inode_table[parent_inum].inode;
	struct refs_inode *c_inode = &inode_table[child_inum].inode;

	// remove child from parent directory
	int ret = remove_child_abspath(p_inode, c_inode, abspath);
	if (ret)
		return ret;

	if (c_inode->n_links > 0)
		return 0;

	// return child's inode (unset bitmap entry)
	clear_bit(inode_bitmap, child_inum);

	// return path's blocks (unset bitmap entries for each)
	release_direct_blocks(c_inode);
	release_indirect_blocks(c_inode);

	// clear path's inode (mark as unused)
	release_inode(c_inode);
	return 0;
}


/**
 * Remove (delete) the given non-directory file.  Note: if hard links
 * are supported, more careful checks must be done to ensure reference
 * counting is correct (only "delete" the inode when the reference count is 0).
 */
static int refs_unlink(const char *abspath) {
	int ret = 0;
	uint64_t parent_inum = 0;
	uint64_t child_inum = 0;

	// look up inode associated with `path`
	ret = resolve_parent_inum_abspath(abspath, &parent_inum);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve parent path %s\n",
			    __func__, abspath);
		return ret;
	}

	struct refs_inode *parent_inode = &inode_table[parent_inum].inode;

	// look up child inode
	ret = resolve_child_inum_abspath(abspath, &child_inum, parent_inode);
	if (ret != 0) {
		DEBUG_PRINT("%s failed to resolve parent path %s\n",
			    __func__, abspath);
		return ret;
	}

	// confirm the target file is not a directory
	struct refs_inode *dir_inode = &inode_table[child_inum].inode;
	if (S_ISDIR(dir_inode->mode))
		return -EISDIR;


	// now we've collected all of the necessary components,
	// so we can attempt the actual rmdir work.
	return do_unlink(parent_inum, child_inum, abspath);
}

/**
 * TODO: this is just implemented to handle the "truncate to zero" case
 * so that "touch" will work. Please replace this comment, and provide
 * a real implementation of truncate
 */
static int refs_truncate(const char* path, off_t size) {
	if (size == 0)
		return 0;

	return -EINVAL;
}

// You should implement the functions that you need, but do so in a
// way that lets you incrementally test.
static struct fuse_operations refs_operations = {
	.init		= refs_init,
	.destroy	= refs_destroy,
	.getattr	= refs_getattr,
	.access		= refs_access,
	.mkdir		= refs_mkdir,
	.readdir	= refs_readdir,
	.chown		= refs_chown,
	.chmod		= refs_chmod,
	.open		= refs_open,
	.release	= refs_release,
	.fgetattr	= refs_fgetattr,
	.rmdir		= refs_rmdir,
	.mknod		= refs_mknod,
	.truncate	= refs_truncate,
	.unlink		= refs_unlink,
	.create		= refs_create,
	/*
	.readlink	= NULL,
	.symlink	= NULL,
	.rename		= NULL,
	.link		= NULL,
	.utimens	= NULL,
	.read		= NULL,
	.write		= NULL,
	.statfs		= NULL,
	.fsync		= NULL,
#ifdef HAVE_SETXATTR
	.setxattr	= NULL,
	.getxattr	= NULL,
	.listxattr	= NULL,
	.removexattr	= NULL,
#endif
*/
};



int main(int argc, char *argv[]) {
	INODE_SIZE_CHECK;
	DIRENT_SIZE_CHECK;
	umask(0);
	return fuse_main(argc, argv, &refs_operations, NULL);
}
