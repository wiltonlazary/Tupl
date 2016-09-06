Changelog
=========

v1.3.6 (2016-09-05)
------
* Fix updating of large entries into crammed nodes, which caused entries to get lost.
* Fix database lock file retention issue.
* Refine Cursor lock method to ensure that the latest value is retrieved.
* Fix key order check when running verification.
* Added file preallocation option. Allows early handling of disk full exceptions to prevent
  crashes with SIGBUS in the case where the file is mmap'ed and a delayed block allocation fails
  due to no space left on device.

v1.3.5 (2016-08-28)
------
* Fix NullPointerException when too many nodes are unevictable. A CacheExhaustedException
  should be thrown instead.
* Fix deadlock between node split and checkpoint.
* Fix "Already in NodeMap" exception when loading fragmented nodes.
* Fix for a rare assertion error when deleting the root node of a tree.
* Added a Cursor lock method, for manual lock control.
* Added some default View and Cursor method implementations.

v1.3.3.1 (2016-08-02)
--------
* Fix subtraction error when load encounters a split node, causing wrong value to be loaded.

v1.3.3 (2016-07-30)
------
* Fixed transaction race condition which allowed shared locks to be prematurely released.
* Fixed load race conditions which caused an incorrect value to be returned.
* Fix for performing database compaction while old indexes are concurrently deleted. Some
  pages would get lost, preventing compaction from ever working again.
* Support temporary indexes.
* Don't close in-use indexes during verification.
* Redo decoder should be lenient if EOF is reached in the middle of an operation.
* Rewrite CommitLock to stripe shared lock requests, improving concurrency.
* Use Java 9 vectorized comparison method if available.
* Add full stats support for non-durable databases.

v1.3.2 (2016-06-04)
------
* Fix storage leak when database capacity is reached during fragmented value allocation.
* Fix deadlock when gathering stats while trees are concurrently closed.
* Optimize count method, utilizing stored internal node counts.
* Add file I/O support for ByteBuffers.

v1.3.1 (2016-05-07)
------
* Fix handling of invalidated transactions, and defined a new exception type for it.
* Fix root node initialization for new trees when using fully mapped mode.
* Fix various replication issues.
* Fix race conditions when closing database.
* Prevent improper use of bogus transaction.

v1.3.0.1 (2016-04-16)
--------
* Fix for Index.load during concurrent node splits. It caused the load to falsely return null.
* Fix undo log node creation when using direct page access mode. The reserved byte was not
  explicitly cleared, allowing fragmented values to corrupt the nodes as pages get recycled.
* Fix shared latch double release when using VARIANT_RETAIN, which would result in a
  deadlock. This affected the Cursor.findGe and Cursor.findLe methods.
* Fix handling of Index.evict when encountering empty nodes, and lock keys as required by the
  transaction.
* Fix handling of index delete and recovery. Deleted index must be closed just like they were
  before recovery, to allow recovery to complete.
* Fix for deleting empty indexes which caused an exception.
* Eliminate overhead of zero-length memory copy when using direct page access mode. This
  primarily affected the performance of values larger than the page size.
* Allow node merge to propagate upwards for empty nodes.
* Ensure that compaction and verification visit all nodes, even empty ones.

v1.3.0 (2016-04-02)
------
* Depends on Java 8.
* Several top-level classes are now interfaces.
* Tree search operations rely extensively on shared latches instead of exclusive latches,
  improving concurrency.
* Fix cursor race condition which allowed split nodes to be modified too soon, leading to
  database corruption.
* Fix deadlock when closing database.
* Fix handling of mapped file shrinkage on Windows.
* More fixes for random search and add improve safety of frame binding.
* Added method to analyze index size.
* Added capacity limit feature.
* Added fully mapped mode when using direct page access and MappedPageArray.
* Added method to evict records from an Index.
* File sync improvements for Linux and MacOS. Performs directory sync'ng and F_FULLSYNC.
* Use JNA to access native I/O functions, eliminating extra system calls.
* Make Latch class a public utility.

v1.2.7.1 (2015-12-22)
--------
* Fix defect in cursor skip which might operate against an unlatched node.

v1.2.7 (2015-10-04)
------
* Fixed reverse view range handling.
* Fixed compareKeyTo methods for transformed and trimmed views.
* Require that bounded views only operate on ordered views.
* Added method to count entries in a View.
* Exposed a few more utility methods.
* When using direct page access mode, page fields are no longer copied to Node instance
  fields. This reduces overall Java heap memory footprint.

v1.2.6.1 (2015-09-05)
--------
* Fix when using mapped files on Linux. Shrinking the database file would cause the process to
  crash when the file is accessed again.

v1.2.6 (2015-08-30)
------
* Counts stored in bottom internal nodes, for speeding up cursor skip operations.
* Added skip method which accepts a limit key.
* More failure handling improvements.

v1.2.5 (2015-07-19)
------
* Fix non-transactional delete race condition.
* Fixes for random cursor search, when encountering internal nodes with few entries or when
  the search range is empty.
* Bug fix for findNearby acting on a closed index.
* Improvements for handling temporary write failures.
* Allow replicas to create no-redo transactions.
* Added option to change transaction durability mode.
* Define an evict operation on the page array, for eliminating unnecessary copies.
* Introduce concurrent cache priming.

v1.2.4 (2015-06-14)
------
* Fix node delete race condition which triggered an assertion error.
* Fix case in which a cursor value was not set if it caused a node merge.
* Add basic AbstractCursor implementation.
* Merge custom redo and undo handler interfaces into one.
* Add checkpoint support to custom transaction handler.
* Provide access to transaction nesting level.
* Attempting to write into an unmodifiable transaction should not invalidate it.
* Introduce combined store and commit operation.

v1.2.3 (2015-05-16)
------
* Fix snapshot deadlock when reading from the cache.
* Add support for custom transaction operations.
* Improved key hash function used by lock manager.
* Expose the WrappedCursor class.
* Added experimental direct page implementation which relies on Unsafe features. Modify
  the source using PageAccessTransformer and then recompile.

v1.2.2 (2015-04-12)
------
* Fix root node deletion when deleting an index.
* Fix memory leak when closing an index.
* Fix to ensure that undo log recovery tracks ghosted values.
* Index drop uses same code as index delete, eliminating complex duplicate code and
  inconsistent behavior.
* Minor thread-safety fix when performing database compaction.
* Created a package for low-level extensions.

v1.2.1 (2015-03-21)
------
* Prevent search deadlock caused by heavy eviction.
* Handle rare NPE when a non-root node becomes the root node during a split.
* Snapshot can read from the cache.
* Allow checkpoint when doing page preallocation.

v1.2.0 (2015-03-01)
------
* Support large keys (up to 2GiB).
* Increased small key encoding format from 64 bytes to 128 bytes.

v1.1.12 (2015-03-01)
------
* Ensure that recovery deletes ghosts of large deleted values.
* Added support for filtering and transforming.
* Added direct lock control to views.
* Support large keys (up to 2GiB) as an experimental option.

v1.1.11 (2015-02-08)
------
* Ensure that any failure to push into the undo log is cleaned up.
* Modify replication interface to handle race condition during checkpoint.
* Ensure durability of index creation and alteration operations.
* Enhancements for safely handling checkpoint abort.
* Support weaker durability modes with replication.

v1.1.10 (2015-01-04)
------
* Fix race conditions in node allocation and recycling code.
* Minor performance optimizations for large values and for simple loads.

v1.1.9 (2014-12-15)
------
* Fix assertion failure caused by aggressive eviction before the very first checkpoint.
* Added simple verification and file compaction tools.
* Checkpoint and redo durability improvements.

v1.1.8 (2014-11-30)
------
* Fix for corruption caused by parent nodes being evicted before their child nodes.
* Stripe page allocation to improve concurrency.
* Merge extremity nodes less aggressively, optimizing for queue access patterns.

v1.1.7 (2014-10-19)
------
* Fix for temporary deadlock at the beginning of a checkpoint.

v1.1.6 (2014-10-11)
------
* Added support for a secondary cache. A secondary cache is slower than a primary cache, but a
  very large primary cache can cause high garbage collection overhead.
* Added method to delete a non-empty index.
* Closing an index no longer forces all the tree nodes to be evicted.

v1.1.5 (2014-09-07)
------
* New node cache implementation. It reduces memory overhead, garbage collection activity, and
  performs fewer memory copies when inserting records.

v1.1.4 (2014-07-13)
------
* Fix corruption when reopening database, caused by aggressive page recycling.

v1.1.3 (2014-06-22)
------
* Reduce the number of node compactions caused by leaf rebalancing, by requiring that an
  existing entry slot be re-used for the inserted entry. More aggressive rebalancing is
  possible, but it causes even more compaction overhead with little benefit.

v1.1.2 (2014-05-29)
------
* Fix rare node eviction bug which left it (partially) in the usage list.
* Cache growth is no longer aggressive, only doing so to avoid node eviction.
* Added cache size to stats.
* Defined new DatabaseFullException for non-durable databases.

v1.1.1 (2014-05-12)
------

* Target Java 7 only.
* Added support for cache priming.
* Various fixes.

v1.1.0 (2014-02-15)
------

* Improved performance of findNearby by encoding extra b-tree node metadata. Indexes can be
  rapidly filled with in-order records using a single cursor which advances to the next key
  using findNearby. This technique works with reverse ordered records and it allows concurrent
  access as usual. Indexes created in older versions will not have this metadata, and so they
  must be rebuilt to obtain it.
* Decode only 6-byte child pointers, preparing for future support of child entry counts.

v1.0.5 (2014-02-01)
------

* Reveal file compaction feature.
* Tolerate, but don't update, b-tree node metadata introduced in version 1.1.0.

v1.0.4 (2014-01-19)
------

* Fix defects when inserting largest possible keys.
* Added rebalancing for internal nodes (was only leaf nodes before).
* Added key suffix compression, which allows more keys to fit inside internal nodes.
* Enforce key size limits. 

v1.0.3 (2014-01-11)
------

* Fix root node corruption when inserting large keys.
* Allow database with non-default page size to be opened without requiring page size to be
  explicitly configured.
* Remove unimplemented node rebalancing for deletes and remove unnecessary node dirtying.
* Added experimental file compaction feature.

v1.0.2 (2013-12-22)
------

* Prevent corruption of recycle free list due to high recycle rate and checkpoints.
* Add equals and hashCode methods to Database.Stats object.
* Restrict page size to be even. Doesn't break compatibility because odd sized pages would lead
  to database corruption anyhow. Supporting odd sized pages isn't worth the effort.

v1.0.1 (2013-12-01)
------

* Index close allows active cursors now. The cursors will behave the same as if they were
  created after the index was closed. The index will appear to be empty and all modifications
  throw a ClosedIndexException.
* Added ability to rename an index.
* Added index rename and drop notifications.
* Database shutdown method works for non-durable databases.
* Added node rebalancing to reduce splits and reduce storage overhead.

v1.0.0 (2013-10-07)
------

* First released version.
