package filer

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
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
