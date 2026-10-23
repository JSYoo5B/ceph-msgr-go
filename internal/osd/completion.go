package osd

// RequiresOnDisk reports the minimum completion bit for a successful reply.
// Besides write-mode operations, SET_CHUNK and the cache flush/evict family
// request native ONDISK while retaining READ encoding. The pinned cache
// no-op/read and transaction-commit paths both complete with ONDISK; the bit
// alone neither identifies a state change nor establishes crash persistence.
// Signed negative replies remain server results, not successful completion.
func RequiresOnDisk(ops []Operation, mode uint64) bool {
	if mode&FlagWrite != 0 {
		return true
	}
	for _, op := range ops {
		switch op.Code {
		case SetChunk, CacheFlush, CacheTryFlush, CacheEvict:
			return true
		}
	}
	return false
}
