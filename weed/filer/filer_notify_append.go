package filer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
		f.deleteOrphanChunk(fid)
		return fmt.Errorf("find %s: %v", fullpath, err)
	} else {
		offset = int64(TotalSize(entry.GetChunks()))
	}

	// append to existing chunks
	entry.Chunks = append(entry.GetChunks(), uploadResult.ToPbFileChunk(fid, offset, time.Now().UnixNano()))

	// update the entry
	err = f.CreateEntry(ctx, entry, nil, false, false, nil, false, f.MaxFilenameLength)
	if err != nil {
		f.deleteOrphanChunk(fid)
	}

	return err
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

// deleteOrphanChunk releases a chunk that was uploaded but whose filer entry
// could not be committed. Without this the chunk stays on the volume server
// with no entry pointing at it: the garbage collector only reclaims chunks that
// were explicitly deleted, so the space is lost until an operator runs
// volume.fsck.
//
// Failures are logged, never returned: the caller is already handling the
// original error, and a failed cleanup must not mask it. A chunk that survives
// here is still recoverable through volume.fsck.
func (f *Filer) deleteOrphanChunk(fid string) {
	if fid == "" {
		return
	}
	if releaseOrphanChunkHook != nil {
		releaseOrphanChunkHook(fid)
		return
	}
	// Detach from the caller's context: it is often already cancelled (that is
	// frequently why the entry commit failed), and the cleanup must still run.
	ctx := context.WithoutCancel(context.Background())
	lookupFunc := func(vids []string) (map[string]*operation.LookupResult, error) {
		return operation.LookupVolumeIds(f.GetMaster, f.GrpcDialOption, vids)
	}
	results := operation.DeleteFileIdsWithLookupVolumeId(ctx, f.GrpcDialOption, []string{fid}, lookupFunc)
	for _, result := range results {
		if result.Error != "" {
			glog.Warningf("failed to release orphan chunk %s: %s", fid, result.Error)
		}
	}
}

// releaseOrphanChunkHook lets tests observe orphan-chunk cleanup without a
// master and volume server. Nil in production.
var releaseOrphanChunkHook func(fid string)
