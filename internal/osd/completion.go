package osd

// RequiresOnDisk reports the minimum completion bit for a successful reply.
// Besides write-mode operations, SET_CHUNK, the cache flush/evict family and
// the manifest TIER_FLUSH/TIER_EVICT request native ONDISK while retaining
// READ encoding. The pinned cache no-op/read and transaction-commit paths
// both complete with ONDISK, and the dedup TIER_FLUSH reply is sent right
// after its transaction is submitted; the bit alone neither identifies a state
// change nor establishes crash persistence.
// Signed negative replies remain server results, not successful completion.
func RequiresOnDisk(ops []Operation, mode uint64) bool {
	if mode&FlagWrite != 0 {
		return true
	}
	for _, op := range ops {
		switch op.Code {
		case SetChunk, CacheFlush, CacheTryFlush, CacheEvict, TierFlush, TierEvict:
			return true
		}
	}
	return false
}
