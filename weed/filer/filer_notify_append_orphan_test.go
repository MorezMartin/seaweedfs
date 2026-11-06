package filer

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommitAppendedChunkReleasesChunkWhenEntryCommitFails covers the
// orphan-chunk leak behind unbounded metadata-log growth: appendToFile uploads
// the chunk first and commits the entry afterwards, so a failed commit used to
// strand the chunk with no reference. Nothing else points at that fid, so no
// later delete can ever mark it — only volume.fsck could reclaim the space.
// That is how a cluster with a few hundred MB of live logs ends up carrying
// tens of GB of "live" volume data.
func TestCommitAppendedChunkReleasesChunkWhenEntryCommitFails(t *testing.T) {
	store := newStubFilerStore()
	store.insertErr = errors.New("store unavailable")
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)

	var released []string
	releaseOrphanChunkHook = func(fid string) {
		released = append(released, fid)
	}
	t.Cleanup(func() { releaseOrphanChunkHook = nil })

	uploadResult := &operation.UploadResult{Name: "chunk", Size: 5}
	err := f.commitAppendedChunk(context.Background(), "/topics/.system/log/2026-11-05", "7,0abc", uploadResult)

	require.Error(t, err, "the commit failure must be surfaced")
	assert.Equal(t, []string{"7,0abc"}, released,
		"the uploaded chunk must be released after a failed entry commit, otherwise it is orphaned on the volume server")
}

// TestCommitAppendedChunkKeepsChunkWhenEntrySurvivesFailedCommit guards the
// dangerous half of the cleanup: CreateEntry can return an error after the
// entry itself was stored (a parent-directory failure does exactly that). The
// chunk is then referenced, and deleting it would destroy live data. The
// stored entry must be read back before any release.
func TestCommitAppendedChunkKeepsChunkWhenEntrySurvivesFailedCommit(t *testing.T) {
	store := newStubFilerStore()
	store.insertErrAfterStore = errors.New("parent directory update failed")
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)

	var released []string
	releaseOrphanChunkHook = func(fid string) {
		released = append(released, fid)
	}
	t.Cleanup(func() { releaseOrphanChunkHook = nil })

	uploadResult := &operation.UploadResult{Name: "chunk", Size: 5}
	err := f.commitAppendedChunk(context.Background(), "/topics/.system/log/2026-11-05", "7,0abc", uploadResult)

	require.Error(t, err, "the commit failure must still be surfaced")
	assert.Empty(t, released,
		"the entry was stored despite the error, so its chunk is referenced and must not be released")
}

// TestCommitAppendedChunkKeepsChunkWhenEntryUnreadable covers the other
// ambiguous case: the commit failed and the entry cannot be read back. A leak
// is recoverable with volume.fsck; deleting a chunk a stored entry may point
// at is not. The chunk must be kept.
func TestCommitAppendedChunkKeepsChunkWhenEntryUnreadable(t *testing.T) {
	store := newStubFilerStore()
	store.insertErr = errors.New("store unavailable")
	store.findErr = errors.New("store unavailable")
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)

	var released []string
	releaseOrphanChunkHook = func(fid string) {
		released = append(released, fid)
	}
	t.Cleanup(func() { releaseOrphanChunkHook = nil })

	uploadResult := &operation.UploadResult{Name: "chunk", Size: 5}
	err := f.commitAppendedChunk(context.Background(), "/topics/.system/log/2026-11-05", "7,0abc", uploadResult)

	require.Error(t, err)
	assert.Empty(t, released,
		"an unreadable entry cannot prove the chunk is orphaned, so the chunk must be kept")
}

// TestCommitAppendedChunkKeepsChunkOnSuccess guards against over-correcting: a
// successful commit must never delete the chunk it just attached.
func TestCommitAppendedChunkKeepsChunkOnSuccess(t *testing.T) {
	store := newStubFilerStore()
	f := NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	f.Store = NewFilerStoreWrapper(store)

	var released []string
	releaseOrphanChunkHook = func(fid string) {
		released = append(released, fid)
	}
	t.Cleanup(func() { releaseOrphanChunkHook = nil })

	uploadResult := &operation.UploadResult{Name: "chunk", Size: 5}
	err := f.commitAppendedChunk(context.Background(), "/topics/.system/log/2026-11-05", "7,0abc", uploadResult)

	require.NoError(t, err)
	assert.Empty(t, released, "a committed chunk must not be released")

	stored, findErr := store.FindEntry(context.Background(), "/topics/.system/log/2026-11-05")
	require.NoError(t, findErr)
	require.Len(t, stored.GetChunks(), 1)
	assert.Equal(t, "7,0abc", stored.GetChunks()[0].GetFileIdString())
}

// TestCapEventChunksDropsOversizedChunkLists covers the quadratic metadata-log
// growth: an event embeds the whole entry, so an append-only file makes every
// append re-serialize every chunk it ever had. Past the cap the chunk list is
// dropped and the event is flagged, telling subscribers to read the entry.
func TestCapEventChunksDropsOversizedChunkLists(t *testing.T) {
	chunks := make([]*filer_pb.FileChunk, metadataEventChunkLimit+1)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{FileId: "7,0abc"}
	}
	entry := &filer_pb.Entry{Chunks: chunks}

	capped, truncated := capEventChunks(entry)

	assert.True(t, truncated, "an oversized chunk list must be reported as truncated")
	assert.Empty(t, capped.GetChunks(), "the chunk list must be dropped from the event")
	assert.Len(t, entry.GetChunks(), metadataEventChunkLimit+1,
		"the caller's entry must be left intact")
}

// TestCapEventChunksKeepsSmallChunkLists guards the common case: an entry under
// the cap must travel unchanged, with no copy and no flag.
func TestCapEventChunksKeepsSmallChunkLists(t *testing.T) {
	entry := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{{FileId: "7,0abc"}}}

	capped, truncated := capEventChunks(entry)

	assert.False(t, truncated)
	assert.Same(t, entry, capped, "an entry under the cap must not be copied")
	assert.Len(t, capped.GetChunks(), 1)
}
