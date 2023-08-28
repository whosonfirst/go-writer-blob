package writer

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	wof_writer "github.com/whosonfirst/go-writer/v3"
	"gocloud.dev/blob"
)

type BlobWriterOptionsKey string

type BlobWriter struct {
	wof_writer.Writer
	bucket *blob.Bucket
	logger *log.Logger
}

// In principle this could also be done with a sync.OnceFunc call but that will
// require that everyone uses Go 1.21 (whose package import changes broke everything)
// which is literally days old as I write this. So maybe a few releases after 1.21.
//
// Also, _not_ using a sync.OnceFunc means we can call RegisterSchemes multiple times
// if and when multiple gomail-sender instances register themselves.

var register_mu = new(sync.RWMutex)
var register_map = map[string]bool{}

func init() {

	ctx := context.Background()
	err := RegisterSchemes(ctx)

	if err != nil {
		panic(err)
	}
}

// RegisterSchemes will explicitly register all the schemes associated with the `AccessTokensDeliveryAgent` interface.
func RegisterSchemes(ctx context.Context) error {

	register_mu.Lock()
	defer register_mu.Unlock()

	for _, scheme := range blob.DefaultURLMux().BucketSchemes() {

		_, exists := register_map[scheme]

		if exists {
			continue
		}

		err := wof_writer.RegisterWriter(ctx, scheme, NewBlobWriter)

		if err != nil {
			return fmt.Errorf("Failed to register blob writer for '%s', %w", scheme, err)
		}

		register_map[scheme] = true
	}

	return nil
}

func NewBlobWriter(ctx context.Context, uri string) (wof_writer.Writer, error) {

	bucket, err := blob.OpenBucket(ctx, uri)

	if err != nil {
		return nil, err
	}

	logger := log.New(io.Discard, "", 0)

	wr := &BlobWriter{
		bucket: bucket,
		logger: logger,
	}

	return wr, nil
}

func (wr *BlobWriter) Write(ctx context.Context, uri string, fh io.ReadSeeker) (int64, error) {

	var wr_opts *blob.WriterOptions

	v := ctx.Value(BlobWriterOptionsKey("options"))

	if v != nil {
		wr_opts = v.(*blob.WriterOptions)
	}

	wr_fh, err := wr.bucket.NewWriter(ctx, uri, wr_opts)

	if err != nil {
		return 0, err
	}

	b, err := io.Copy(wr_fh, fh)

	if err != nil {
		return b, err
	}

	err = wr_fh.Close()

	if err != nil {
		return b, err
	}

	return b, nil
}

func (wr *BlobWriter) Flush(ctx context.Context) error {
	return nil
}

func (wr *BlobWriter) Close(ctx context.Context) error {
	return nil
}

func (wr *BlobWriter) SetLogger(ctx context.Context, logger *log.Logger) error {
	wr.logger = logger
	return nil
}

func (wr *BlobWriter) WriterURI(ctx context.Context, uri string) string {
	return uri
}
