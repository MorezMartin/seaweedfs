package weed_server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (fs *FilerServer) uploadRequestToChunks(ctx context.Context, w http.ResponseWriter, r *http.Request, reader io.Reader, chunkSize int32, fileName, contentType string, contentLength int64, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {
	query := r.URL.Query()

	isAppend := isAppend(r)
	if query.Has("offset") {
		offset := query.Get("offset")
		offsetInt, err := strconv.ParseInt(offset, 10, 64)
		if err != nil || offsetInt < 0 {
			err = fmt.Errorf("invalid 'offset': '%s'", offset)
			return nil, nil, 0, err, nil
		}
		if isAppend && offsetInt > 0 {
			err = fmt.Errorf("cannot set offset when op=append")
			return nil, nil, 0, err, nil
		}
		chunkOffset = offsetInt
	}

	return fs.uploadReaderToChunks(ctx, r, reader, chunkOffset, chunkSize, fileName, contentType, isAppend, so)
}

func (fs *FilerServer) uploadReaderToChunks(ctx context.Context, r *http.Request, reader io.Reader, startOffset int64, chunkSize int32, fileName, contentType string, isAppend bool, so *operation.StorageOption) (fileChunks []*filer_pb.FileChunk, md5Hash hash.Hash, chunkOffset int64, uploadErr error, smallContent []byte) {

	md5Hash = md5.New()
	chunkOffset = startOffset
	var partReader = io.NopCloser(io.TeeReader(reader, md5Hash))

	var wg sync.WaitGroup
	var bytesBufferCounter int64 = 4
	bytesBufferLimitChan := make(chan struct{}, bytesBufferCounter)
	var fileChunksLock sync.Mutex
	var uploadErrLock sync.Mutex
	for {

		// need to throttle used byte buffer
		bytesBufferLimitChan <- struct{}{}

		// As long as there is an error in the upload of one chunk, it can be terminated early
		// uploadErr may be modified in other go routines, lock is needed to avoid race condition
		uploadErrLock.Lock()
		if uploadErr != nil {
			<-bytesBufferLimitChan
			uploadErrLock.Unlock()
			break
		}
		uploadErrLock.Unlock()

		bytesBuffer := bufPool.Get().(*bytes.Buffer)

		limitedReader := io.LimitReader(partReader, int64(chunkSize))

		bytesBuffer.Reset()

		dataSize, err := bytesBuffer.ReadFrom(limitedReader)

		// data, err := io.ReadAll(limitedReader)
		if err != nil || dataSize == 0 {
			bufPool.Put(bytesBuffer)
			<-bytesBufferLimitChan
			if err != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = err
				}
				uploadErrLock.Unlock()
			}
			break
		}
		if chunkOffset == 0 && !isAppend {
			if dataSize < fs.option.SaveToFilerLimit {
				chunkOffset += dataSize
				smallContent = make([]byte, dataSize)
				bytesBuffer.Read(smallContent)
				bufPool.Put(bytesBuffer)
				<-bytesBufferLimitChan
				stats.FilerHandlerCounter.WithLabelValues(stats.ContentSaveToFiler).Inc()
				break
			}
		} else {
			stats.FilerHandlerCounter.WithLabelValues(stats.AutoChunk).Inc()
		}

		wg.Add(1)
		go func(offset int64, buf *bytes.Buffer) {
			defer func() {
				bufPool.Put(buf)
				<-bytesBufferLimitChan
				wg.Done()
			}()

			chunks, toChunkErr := fs.dataToChunkWithSSE(ctx, r, fileName, contentType, buf.Bytes(), offset, so)
			if toChunkErr != nil {
				uploadErrLock.Lock()
				if uploadErr == nil {
					uploadErr = toChunkErr
				}
				uploadErrLock.Unlock()
			}
			if chunks != nil {
				fileChunksLock.Lock()
				for _, chunk := range chunks {
					fileChunks = append(fileChunks, chunk)
				}
				fileChunksLock.Unlock()
			}
		}(chunkOffset, bytesBuffer)

		// reset variables for the next chunk
		glog.V(4).Infof("uploadReaderToChunks read chunk at offset %d, size %d", chunkOffset, dataSize)
		chunkOffset = chunkOffset + dataSize

		// if last chunk was not at full chunk size, but already exhausted the reader
		if dataSize < int64(chunkSize) {
			break
		}
	}

	wg.Wait()

	if uploadErr != nil {
		glog.V(0).InfofCtx(ctx, "upload file %s error: %v", fileName, uploadErr)
		for _, chunk := range fileChunks {
			glog.V(4).InfofCtx(ctx, "purging failed uploaded %s chunk %s [%d,%d)", fileName, chunk.FileId, chunk.Offset, chunk.Offset+int64(chunk.Size))
		}
		fs.filer.DeleteUncommittedChunks(ctx, fileChunks)
		return nil, md5Hash, 0, uploadErr, nil
	}
	slices.SortFunc(fileChunks, func(a, b *filer_pb.FileChunk) int {
		return int(a.Offset - b.Offset)
	})
	return fileChunks, md5Hash, chunkOffset, nil, smallContent
}

func (fs *FilerServer) doUpload(ctx context.Context, urlLocation string, limitedReader io.Reader, fileName string, contentType string, pairMap map[string]string, auth security.EncodedJwt, contentMd5 string) (*operation.UploadResult, error, []byte) {

	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUpload).Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues(stats.ChunkUpload).Observe(time.Since(start).Seconds())
	}()

	uploadOption := &operation.UploadOption{
		UploadUrl:         urlLocation,
		Filename:          fileName,
		Cipher:            fs.option.Cipher,
		IsInputCompressed: false,
		MimeType:          contentType,
		PairMap:           pairMap,
		Jwt:               auth,
		Md5:               contentMd5,
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return nil, err, []byte{}
	}

	// Use a context that ignores cancellation from the request context
	uploadCtx := context.WithoutCancel(ctx)

	uploadResult, err, data := uploader.Upload(uploadCtx, limitedReader, uploadOption)
	if uploadResult != nil && uploadResult.RetryCount > 0 {
		stats.FilerHandlerCounter.WithLabelValues(stats.ChunkUploadRetry).Add(float64(uploadResult.RetryCount))
	}
	return uploadResult, err, data
}

func (fs *FilerServer) dataToChunkWithSSE(ctx context.Context, r *http.Request, fileName, contentType string, data []byte, chunkOffset int64, so *operation.StorageOption) ([]*filer_pb.FileChunk, error) {
	dataReader := util.NewBytesReader(data)

	// retry to assign a different file id
	var fileId, urlLocation string
	var auth security.EncodedJwt
	var uploadErr error
	var uploadResult *operation.UploadResult
	var failedFileChunks []*filer_pb.FileChunk

	err := util.Retry("filerDataToChunk", func() error {
		// If a pre-assigned fileId is provided (e.g., from replication sink),
		// use it directly and look up the auth token instead of calling AssignVolume.
		if so.FileId != "" {
			fileId = so.FileId
			// Look up the volume server location and auth token from the master
			fileLocs, locErr := fs.filer.MasterClient.GetLookupFileIdFunction()(ctx, fileId)
			if locErr != nil || len(fileLocs) == 0 {
				glog.V(4).InfofCtx(ctx, "fileId %s not found, retrying: %v", fileId, locErr)
				return locErr
			}
			// Pick the first location
			urlLocation = fileLocs[0]
			// Get auth token for the fileId by calling master LookupVolume
			var token string
			if lerr := fs.filer.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
				resp, lerr := client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
					VolumeOrFileIds: []string{fileId},
				})
				if lerr != nil {
					return lerr
				}
				for _, vidLoc := range resp.VolumeIdLocations {
					if vidLoc.VolumeOrFileId == fileId && len(vidLoc.Auth) > 0 {
						token = string(vidLoc.Auth)
						break
					}
				}
				return nil
			}); lerr != nil {
				glog.V(4).InfofCtx(ctx, "failed to lookup auth for fileId %s: %v", fileId, lerr)
			}
			auth = security.EncodedJwt(token)
		} else {
			// assign one file id for one chunk
			fileId, urlLocation, auth, uploadErr = fs.assignNewFileInfo(ctx, so, uint64(len(data)))
		}
		if uploadErr != nil {
			glog.V(4).InfofCtx(ctx, "retry later due to assign error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkAssignRetry).Inc()
			return uploadErr
		}
		if fileId == "" {
			return fmt.Errorf("no fileId returned")
		}
		chunkMd5 := md5.Sum(data)
		chunkMd5B64 := base64.StdEncoding.EncodeToString(chunkMd5[:])
		// upload the chunk to the volume server
		uploadResult, uploadErr, _ = fs.doUpload(ctx, urlLocation, dataReader, fileName, contentType, nil, auth, chunkMd5B64)
		if uploadErr != nil {
			glog.V(4).InfofCtx(ctx, "retry later due to upload error: %v", uploadErr)
			stats.FilerHandlerCounter.WithLabelValues(stats.ChunkDoUploadRetry).Inc()
			fid, _ := filer_pb.ToFileIdObject(fileId)
			fileChunk := filer_pb.FileChunk{
				FileId: fileId,
				Offset: chunkOffset,
				Fid:    fid,
			}
			failedFileChunks = append(failedFileChunks, &fileChunk)
			return uploadErr
		}
		return nil
	})
	if err != nil {
		glog.ErrorfCtx(ctx, "upload error: %v", err)
		return failedFileChunks, err
	}

	// if last chunk exhausted the reader exactly at the border
	if uploadResult.Size == 0 {
		return nil, nil
	}

	// Extract SSE metadata from request headers if available
	var sseType filer_pb.SSEType = filer_pb.SSEType_NONE
	var sseMetadata []byte

	// Create chunk with SSE metadata if available
	var chunk *filer_pb.FileChunk
	if sseType != filer_pb.SSEType_NONE {
		chunk = uploadResult.ToPbFileChunkWithSSE(fileId, chunkOffset, time.Now().UnixNano(), sseType, sseMetadata)
	} else {
		chunk = uploadResult.ToPbFileChunk(fileId, chunkOffset, time.Now().UnixNano())
	}

	return []*filer_pb.FileChunk{chunk}, nil
}
