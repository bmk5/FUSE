# FUSE
The aim is to develop a “Reference FS”, which we will call ReFS. The code defines the “on-disk format” for ReFS
and implements support creating and navigating directories

## Repository Contents

 * __`bitmap.c`__: a straightforward implementation of a bitmap data structure that supports
   setting/clearing individual bits, as well as querying a given bit's value.
 * __`refs.h`__: struct and function declarations for the implementation of ReFS.
 * __`refs.c`__: definitions of the main functions that define ReFS behavior.
 * __`Makefile`__: includes rules to compile ReFS as well as a testable version of the bitmap code (`make bitmap`)
