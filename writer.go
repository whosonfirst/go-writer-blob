package writer

import (
	"context"
	wof_writer "github.com/whosonfirst/go-writer"
	"gocloud.dev/blob"
	"io"
)

type BlobWriterOptionsKey string

type BlobWriter struct {
	wof_writer.Writer
	bucket *blob.Bucket
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

	wr := &BlobWriter{
		bucket: bucket,
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

func (wr *BlobWriter) WriterURI(ctx context.Context, uri string) string {
	return uri
}
