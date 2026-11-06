package filer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (f *Filer) appendToFile(ctx context.Context, targetFile string, data []byte) error {

	assignResult, uploadResult, err2 := f.assignAndUpload(ctx, targetFile, data)
	if err2 != nil {
		return err2
	}

	// The piece is already uploaded; commit it on a detached context so an
	// expired shutdown deadline does not strand the chunk.
	ctx = context.WithoutCancel(ctx)

	return f.commitAppendedChunk(ctx, targetFile, assignResult.Fid, uploadResult)
}

// commitAppendedChunk attaches an already-uploaded chunk to its filer entry.
//
// From here on the chunk is durable on a volume server. Any early return must
// therefore release it, otherwise it stays orphaned forever: nothing else
// references this fid, so no later delete can ever mark it, and volume.fsck is
// the only way to reclaim the space. This is especially visible for the system
// metadata log, which is appended to on every filer write while filers are
// being restarted — that is how a cluster with a few hundred MB of live logs
// ends up carrying tens of GB of "live" volume data.
func (f *Filer) commitAppendedChunk(ctx context.Context, targetFile, fid string, uploadResult *operation.UploadResult) error {
	fullpath := util.FullPath(targetFile)
	entry, err := f.FindEntry(ctx, fullpath)
	var offset int64 = 0
	if err == filer_pb.ErrNotFound {
		entry = &Entry{
			FullPath: fullpath,
			Attr: Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else if err != nil {
		// The chunk is uploaded but the lookup failed, so no commit will
		// happen and nothing will ever reference it. Release it here;
		// returning early used to strand one chunk per retry.
		f.releaseChunkIfUnreferenced(ctx, fullpath, uploadResult.ToPbFileChunk(fid, offset, time.Now().UnixNano()))
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		offset = int64(TotalSize(entry.GetChunks()))
	}

	// append to existing chunks
	chunk := uploadResult.ToPbFileChunk(fid, offset, time.Now().UnixNano())
	entry.Chunks = append(entry.GetChunks(), chunk)

	// update the entry
	err = f.CreateEntry(ctx, entry, nil, false, false, nil, false, f.MaxFilenameLength)
	if err != nil {
		f.releaseChunkIfUnreferenced(ctx, fullpath, chunk)
	}

	return err
}

// releaseOrphanChunkHook observes chunk releases in tests. Production leaves
// it nil.
var releaseOrphanChunkHook func(fid string)

// releaseChunkIfUnreferenced deletes an uploaded chunk only when the stored
// entry does not reference it. A missing entry means nothing points at the
// chunk, so it is released. When the entry exists but cannot be read the chunk
// is kept: deleting data a stored entry may point at is worse than leaking it.
func (f *Filer) releaseChunkIfUnreferenced(ctx context.Context, fullpath util.FullPath, chunk *filer_pb.FileChunk) {
	stored, err := f.FindEntry(ctx, fullpath)
	if err != nil && err != filer_pb.ErrNotFound {
		return
	}
	if stored != nil {
		for _, c := range stored.GetChunks() {
			if c.GetFileId() == chunk.GetFileId() {
				return
			}
		}
	}
	if releaseOrphanChunkHook != nil {
		releaseOrphanChunkHook(chunk.GetFileId())
	}
	f.DeleteChunks(ctx, fullpath, []*filer_pb.FileChunk{chunk})
}

// resolveMetadataLogAssignDiskType returns the disk type and matched path rule for
// metadata log volume assigns. Disk type uses the rule when set, otherwise
// Filer.DefaultDiskType (from -filer.disk), mirroring resolveAssignStorageOption.
func (f *Filer) resolveMetadataLogAssignDiskType(targetFile string) (string, *filer_pb.FilerConf_PathConf) {
	if f.FilerConf == nil {
		return f.DefaultDiskType, &filer_pb.FilerConf_PathConf{}
	}
	rule := f.FilerConf.MatchStorageRule(targetFile)
	return util.Nvl(rule.DiskType, f.DefaultDiskType), rule
}

// metaLogCollectionFor resolves the system metadata log's collection: the
// filer.options.metaLog.collection override, then the filer default, then the
// matched storage rule.
func (f *Filer) metaLogCollectionFor(ruleCollection string) string {
	return util.Nvl(f.metaLogTargetCollection, f.metaLogCollection, ruleCollection)
}

func (f *Filer) metaLogReplicationFor(ruleReplication string) string {
	return util.Nvl(f.metaLogTargetReplication, f.metaLogReplication, ruleReplication)
}

func (f *Filer) assignAndUpload(ctx context.Context, targetFile string, data []byte) (*operation.AssignResult, *operation.UploadResult, error) {
	// assign a volume location
	diskType, rule := f.resolveMetadataLogAssignDiskType(targetFile)
	assignRequest := &operation.VolumeAssignRequest{
		Count:               1,
		Collection:          f.metaLogCollectionFor(rule.Collection),
		Replication:         f.metaLogReplicationFor(rule.Replication),
		DiskType:            diskType,
		WritableVolumeCount: rule.VolumeGrowthCount,
		ExpectedDataSize:    uint64(len(data)),
	}

	assignResult, err := operation.Assign(ctx, f.GetMaster, f.masterGrpcDialOption(), assignRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("AssignVolume: %w", err)
	}
	if assignResult.Error != "" {
		return nil, nil, fmt.Errorf("AssignVolume error: %v", assignResult.Error)
	}

	// upload data
	targetUrl := "http://" + assignResult.Url + "/" + assignResult.Fid
	uploadOption := &operation.UploadOption{
		UploadUrl:         targetUrl,
		Filename:          "",
		Cipher:            f.Cipher,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               assignResult.Auth,
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}

	uploadResult, err := uploader.UploadData(ctx, data, uploadOption)
	if err != nil {
		return nil, nil, fmt.Errorf("upload data %s: %v", targetUrl, err)
	}
	// println("uploaded to", targetUrl)
	return assignResult, uploadResult, nil
}

