package writer

import (
	"context"
	wof_writer "github.com/whosonfirst/go-writer/v3"
	"gocloud.dev/blob"
	"io"
	"log"
)

type BlobWriterOptionsKey string

type BlobWriter struct {
	wof_writer.Writer
	bucket *blob.Bucket
	logger *log.Logger
}

func init() {

	ctx := context.Background()

	for _, scheme := range blob.DefaultURLMux().BucketSchemes() {

		err := wof_writer.RegisterWriter(ctx, scheme, NewBlobWriter)

		if err != nil {
			panic(err)
		}
	}
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
