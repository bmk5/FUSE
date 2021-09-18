
#include "refs.h"
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


// 8 bits per byte, so divide by 8
#define BIT_IDX_TO_BYTE(bit) ((bit) >> 3)

// Mod by 8 to get the index within a byte.
// Then shift to create a mask for that single bit.
#define BIT_IDX_TO_MASK(bit) (1 << ((bit) & 7))

/**
 * Allocates a struct bitmap and populates its fields. The bitmap
 * region is initialized to all 0s.
 *
 * This function allocates an in-memory represetnation of the bitmap
 * that is block-aligned, so the n_valid_bits represents the index of the
 * last meaningful bit; all bits after that value are undefined.
 * The total size of the bitmap, in bytes, is ->n_bytes.
 *
 * @param num_bits is the number of valid bits in the bitmap.
 * @return a pointer to an initialize bitmap, or NULL on error
 */
struct bitmap *allocate_bitmap(uint64_t num_bits) {
	struct bitmap *bmap = malloc(sizeof *bmap);
	if (bmap == NULL)
		return NULL;

	// 8 bits per byte, round up to nearest byte
	uint64_t min_bytes = (num_bits + 7) >> 3;

	// 4096 bytes per block, round up
	bmap->n_bytes = BLOCK_ROUND_UP(min_bytes);

	// no rounding here; we know the exact number of bits that are valid
	bmap->n_valid_bits = num_bits;

	bmap->bits = malloc(bmap->n_bytes);
	if (bmap->bits == NULL) {
		free(bmap);
		return NULL;
	}
	explicit_bzero(bmap->bits, bmap->n_bytes);

	return bmap;
}

/**
 * Free the memory for a malloc-ed bitmap structure:
 *  - free the bits field, since it is also malloced
 *  - free the @arg bmap pointer itself
 */
void free_bitmap(struct bitmap *bmap) {
	free(bmap->bits);
	free(bmap);
}

/**
 * Yield the logical value of a given bit (1 or 0).
 *
 * Note: this consistently counts from LSB to MSB within each byte.
 * So, for example, the first byte's bits represent bit indexes
 * `76543210` instead of `01234567`.
 *
 * @return 1 if the bit is set, or 0 if it is not set
 */
char get_bit(struct bitmap *map, uint64_t i) {
	uint64_t byte = BIT_IDX_TO_BYTE(i);
	unsigned char mask = BIT_IDX_TO_MASK(i);
	return map->bits[byte] & mask ? 1 : 0; // return 1 if bit was set
}

/**
 * Set the bit at position i (regardless of whether already set)
 * @post bit `i` is 1
 *
 * Note: this consistently counts from LSB to MSB within each byte
 */
void set_bit(struct bitmap *map, uint64_t i) {
	uint64_t byte = BIT_IDX_TO_BYTE(i);
	unsigned char mask = BIT_IDX_TO_MASK(i);
	map->bits[byte] |= mask; // set the new bit, leaving others unchanged
}

/**
 * unset (0) the bit at position i (regardless of whether already set)
 * post: bit `i` is 0
 * consistently count from LSB to MSB within each byte
 */
void clear_bit(struct bitmap *map, uint64_t i) {
	uint64_t byte = BIT_IDX_TO_BYTE(i);
	unsigned char mask = BIT_IDX_TO_MASK(i);
	map->bits[byte] &= ~mask; // preserve all other bit values
}


/**
 * walk through bitmap, scanning each byte to find the first unset bit
 * return the unset bit's global number (not byte number or byte offset)
 */
uint64_t first_unset_bit(struct bitmap *map) {
	// go through every byte in our bitmap, starting at the first,
	// and compare to the bit pattern `11111111`.
	// If we find a byte with an unset bit, we use it.
	// `b` is the index of our byte
	int b = 0;
	while (b < ((map->n_valid_bits + 7) / 8)) {

		// 255 has bit pattern `11111111`
		// if true: found a byte with at least one 0
		if (map->bits[b] != 255) {
			// find the first free bit in the target byte
			for (int bit = 0; bit < 8; bit++) {
				if (!(map->bits[b] & (1 << bit))) {
					// (byte * bits/byte) + offset in byte
					return b*8 + bit;
				}
			}
		}
		b++;
	}

	// signal error with an out of bounds bit
	return map->n_valid_bits;
}

/**
 * Print bitmap representation with 8 bytes per line and some indexes
 * to help visualize/count.
 * We print left to right, but evaluate bits from LSB to MSB within each byte
 * This displays bits in the bitmaps "expected" logical bit order,
 * even if it doesn't exactly match the byte values on disk.
 */
void dump_bitmap(struct bitmap *map) {
	printf("In-memory Bitmap Representation\n");
	printf("\tbitmap->n_bytes: %"PRIu64"\n", map->n_bytes);
	printf("\tbitmap->n_valid_bits: %"PRIu64"\n", map->n_valid_bits);
	printf("\tbitmap->bits (64 chars per column):\n");
	int line = 0;
	for (uint64_t i = 0; i < map->n_valid_bits; i++) {
		if (i % 8 == 0) {
			if (line % 64 == 0) {
				printf("\n");
				printf("%4"PRIu64":", i);
				line = 0;
			}

			printf(" ");
		}
		printf("%u", get_bit(map, i));
		line++;
	}
	printf("\n\n");
}

#ifdef TEST_BITMAP

int is_contained(int *arr, int val, int n) {
	for (int i = 0; i < n; i++) {
		if (arr[i] == val)
			return 1;
	}
	return 0;
}

int check_bits(struct bitmap *map, int *ones, int n) {
	for (int b = 0; b < map->n_valid_bits; b++) {
		int target = 0;
		if (is_contained(ones, b, n))
			target = 1;
			
		if (get_bit(map, b) != target) {
			printf("error! bit mismatch\n");
			printf("bit %d: expected %d, received %d\n", b, target, get_bit(map, b));
				return -1;
		}
	}
	return 0;
}

void test_bitmap_small() {
	struct bitmap *map = allocate_bitmap(10);

	dump_bitmap(map);

	printf("setting bit 0\n");
	set_bit(map, 0);
	dump_bitmap(map);

	printf("setting bit 3\n");
	set_bit(map, 3);
	dump_bitmap(map);

	printf("setting bit 8\n");
	set_bit(map, 8);
	dump_bitmap(map);

	printf("clearing bit 0\n");
	clear_bit(map, 0);
	dump_bitmap(map);

	printf("clearing bit 3\n");
	clear_bit(map, 3);
	dump_bitmap(map);

	printf("clearing bit 8\n");
	clear_bit(map, 8);
	dump_bitmap(map);
	
	free_bitmap(map);
}

void test_bitmap_large() {
	struct bitmap *map = allocate_bitmap(4096);
	int n = 7;
	int ones[7] = {0, 10, 100, 1000, 2000, 4000, 4095};

	printf("starting large bitmap test\n");
	
	for (int i = 0; i < n; i++) {
		check_bits(map, ones, i);
		set_bit(map, ones[i]);
		check_bits(map, ones, i+1);
	}

	for (int i = n; i > 0; i--) {
		check_bits(map, ones, i);
		clear_bit(map, ones[i-1]);
		check_bits(map, ones, i-1);
	}
	
	free_bitmap(map);
	printf("done large bitmap test\n");
}

int main(int argc, char *argv[]) {
	test_bitmap_small();
	test_bitmap_large();
}

#endif // TEST_BITMAP
