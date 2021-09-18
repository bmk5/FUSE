#ifndef REFS_H

#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>

#define ENABLE_DEBUG

#ifdef ENABLE_DEBUG
 #define DEBUG_PRINT(...) fprintf(stderr, __VA_ARGS__)
#else
 #define DEBUG_PRINT(...) do { } while (false)
#endif


typedef uint64_t lba_t;

// This is used in printf, e.g.: printf("block idx: %"LBA_PRINT"\n", blk_idx);
#define LBA_PRINT PRIu64

#define SBLOCK_SIZE 4096 // feel free to change. 512 is also reasonable
#define BLOCK_SIZE 4096 // feel free to change, but 4096 is advisable
#define BLOCK_SHIFT 12  // 4096 = 2^12
#define BLOCK_ROUND_UP(n) ((((n) + (BLOCK_SIZE-1)) / BLOCK_SIZE) * BLOCK_SIZE)
#define NUM_DIRECT 10   // feel free to change
#define NUM_INDIRECT 2  // feel free to change

#define BYTES_TO_BLKS(x) ((x) >> (BLOCK_SHIFT))
#define BLKS_TO_BYTES(x) ((x) << (BLOCK_SHIFT))
#define BLOCK_OFFSET(x) ((x) & ((1 << BLOCK_SHIFT)-1))

#define DISK_PATH "./refs_disk"
#define ROOT_INUM 0
#define SUPER_LBA 0

struct refs_superblock {
	dev_t dev;
	uint32_t block_size; // in bytes
	uint64_t num_inodes; // number of slots in the inode table (and bitmap)
	lba_t i_bitmap_start; // starting LBA of bitmap
	lba_t i_table_start; // starting LBA of inode table
	uint64_t num_data_blocks; // total number of data blocks in "disk"
	lba_t d_bitmap_start; // starting LBA of bitmap
	lba_t d_region_start; // starting LBA of data blocks
};

union superblock {
	struct refs_superblock super;
	char		pad[SBLOCK_SIZE];
};

#define INODE_FREE 0
#define INODE_IN_USE 1

/*
 * The structure that defines our on-disk inode.
 * We may need to add more fields to support all of
 * the required functionality.
 */
struct refs_inode {
	char flags;       // inode status (currently allocated/free, file type)
	mode_t mode;
	uid_t uid;
	uid_t gid;
	time_t time; // just store one time for now
	uint16_t n_links; // number of links
	uint64_t inum;    // inode number (index within the inode table)
	uint64_t size;    // size (in bytes); not necessarily block-aligned
	uint64_t blocks;  // number of blocks (# valid entires in block_ptrs)
	lba_t block_ptrs[NUM_DIRECT + NUM_INDIRECT]; // lbas with file data
};


/* Our directory is a table of (inode number -> path) entries
   We make some simplifying design decisions here that deviate from the textbook.
   1. We treat each directory block as an array of fixed-size table entries
      (the textbook design supports variable-length entries).
   2. We impose a maximum path length. Note that this limits a single
      component of the path (i.e., /home/bill/path has 3 components,
      and any one of those components can be up to MAX_PATH_LEN-1 chars (+\0)).
      It does not limit the length of the absolute path.
   To make this work, we have an is_valid bit so we don't have to compact our
   directory array; when we delete, we just "shoot down" an entry by marking
   it invalid.
   This design is not required, but you may use it if you would like.
*/
# define MAX_PATH_LEN 54
struct dir_entry {
	uint64_t inum; // inode number (index within the inode table)
	char is_valid; // 0/1
	char path_len; // uses strlen, so this excludes the null terminator
	char path[MAX_PATH_LEN]; // MAX_PATH_LEN-1 is usable (we store `\0`)
};

#define DIRENT_SIZE 64
#define DIRENTS_PER_BLOCK ((BLOCK_SIZE) / (DIRENT_SIZE))
	


/*
 * (This is just a trick to let the system enforce our requirements.)
 * Runtime check to make sure our directory entries form a table that is
 * block-aligned. The check has 2 parts:
 *   (a) our "padded size" is at least as large as the C struct
 *   (b) our "padded size" evenly divides the BLOCK_SIZE
 */
#define DIRENT_SIZE_CHECK						\
	assert((sizeof(struct dir_entry) <= (DIRENT_SIZE))		\
	       && ((BLOCK_SIZE) % (DIRENT_SIZE) == 0))


/**
 * A directory's data block is a table of individual entries
 * that map inode numbers to path components.
 * We use a union to ensure our structure is exactly equal
 * to a block size.
 */
union directory_block {
	struct dir_entry dirents[DIRENTS_PER_BLOCK];
	char pad[BLOCK_SIZE];
};

#define INODE_SIZE 256
#define INODES_PER_BLOCK ((BLOCK_SIZE) / (INODE_SIZE))

// Pad our inodes to a size that divides our block size evenly
union inode {
	struct refs_inode inode;
	char pad[INODE_SIZE];
};

/* 
 * Runtime check to make sure our INODE_SIZE evenly divides our BLOCK_SIZE
 * so that our inode table is block-aligned.
 * 2 parts: (a) our "padded size" is at least as large as the C struct
 *          (b) our "padded size" evenly divides the BLOCK_SIZE
 */
#define INODE_SIZE_CHECK						\
	assert((sizeof(struct refs_inode) <= (INODE_SIZE))		\
	       && ((BLOCK_SIZE) % (INODE_SIZE) == 0))


/** 
 * Some default values. Ideally, these would not be hard-coded,
 *  but this simplifies our design. You can change these as you
 *  add functionality or resize your disk.
 *  You are also encouraged to calculate parameters dynamically.
 */
#define DEFAULT_NUM_INODES 1024
#define DEFAULT_NUM_DATA_BLOCKS 2492

/**
 * This structure is the in-memory representation of a bitmap.
 * In reality, the bitmap is stored in one-or-more blocks on disk.
 * We read and write the bitmap stored in the "bitmap" memory buffer,
 * and we have helper functions to query and modify the bitmap state
 * e.g., set_bit, check_bit.
 * We keep a bitmap for our inode table and for our data region
 */
struct bitmap {
	// the number of bytes in the bitmap (not bits!)
	// Note: the bitmap must be padded to a multiple of BLOCK_SIZE
	uint64_t n_bytes;
	
	// Due to padding, the number of valid bits may be significantly
	// less than the number of bits that the bitmap *COULD* represent
	uint64_t n_valid_bits;

	// The actual bitmap. This memory buffer must be malloc'ed and freed
	unsigned char *bits;
};

// see implementation of thse key bitmap functions (and tests) in bitmap.c
struct bitmap *allocate_bitmap(uint64_t num_bits);
void free_bitmap(struct bitmap *bmap);
char get_bit(struct bitmap *map, uint64_t i);
void set_bit(struct bitmap *map, uint64_t i);
void clear_bit(struct bitmap *map, uint64_t i);
uint64_t first_unset_bit(struct bitmap *map);
void dump_bitmap(struct bitmap *map); // debugging


/****
 Utility functions
 ***/

/**
 * Since we are pretending we are writing a real file system that
 * interacts with a disk, we do block-aligned I/O.  Use these
 * functions when you want to read/write from disk
 */
ssize_t write_blocks(void *buf, size_t lba_count, off_t starting_lba);
ssize_t read_blocks(void *buf, size_t lba_count, off_t starting_lba);

static inline ssize_t read_block(void *buf, off_t lba) {
	return read_blocks(buf, 1, lba);
}

static inline ssize_t write_block(void *buf, off_t lba) {
	return write_blocks(buf, 1, lba);
}

static inline void *malloc_blocks(int num_blocks) {
	return malloc(num_blocks * BLOCK_SIZE);
}

static inline void *zalloc_blocks(int num_blocks) {
	return calloc(1, num_blocks * BLOCK_SIZE);
}

// (no free_blocks function because we can just use free)

#endif // REFS_H
