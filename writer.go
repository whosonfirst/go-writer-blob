package writer

import (
	"context"
	wof_writer "github.com/whosonfirst/go-writer"
	"gocloud.dev/blob"
	"io"
)

func init() {

	ctx := context.Background()

	for _, scheme := range blob.DefaultURLMux().BucketSchemes() {

		err := wof_writer.RegisterWriter(ctx, scheme, initializeBlobWriter)

		if err != nil {
			panic(err)
		}
	}
}

func initializeBlobWriter(ctx context.Context, uri string) (wof_writer.Writer, error) {

	wr := NewBlobWriter()
	err := wr.Open(ctx, uri)

	if err != nil {
		return nil, err
	}

	return wr, nil
}

type BlobWriterOptionsKey string

type BlobWriter struct {
	wof_writer.Writer
	bucket *blob.Bucket
}

func NewBlobWriter() wof_writer.Writer {

	wr := BlobWriter{}
	return &wr
}

func (wr *BlobWriter) Open(ctx context.Context, uri string) error {

	bucket, err := blob.OpenBucket(ctx, uri)

	if err != nil {
		return err
	}

	wr.bucket = bucket
	return nil
}

func (wr *BlobWriter) Write(ctx context.Context, uri string, fh io.ReadCloser) error {

	var wr_opts *blob.WriterOptions

	v := ctx.Value(BlobWriterOptionsKey("options"))

	if v != nil {
		wr_opts = v.(*blob.WriterOptions)
	}

	wr_fh, err := wr.bucket.NewWriter(ctx, uri, wr_opts)

	if err != nil {
		return err
	}

	_, err = io.Copy(wr_fh, fh)

	if err != nil {
		return err
	}

	return wr_fh.Close()
}
