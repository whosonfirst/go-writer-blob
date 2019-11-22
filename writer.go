package writer

import (
	"context"
	"github.com/aaronland/gocloud-blob-bucket"
	wof_writer "github.com/whosonfirst/go-writer"
	"gocloud.dev/blob"
	"io"
	"net/url"
)

func init() {
	r := NewBlobWriter()
	wof_writer.Register("blob", r)
}

type BlobWriter struct {
	wof_writer.Writer
	scheme string
	bucket *blob.Bucket
}

func NewBlobWriter() wof_writer.Writer {

	wr := BlobWriter{}
	return &wr
}

func (wr *BlobWriter) Open(ctx context.Context, uri string) error {

	u, err := url.Parse(uri)

	if err != nil {
		return err
	}

	scheme := u.Host

	u.Scheme = scheme
	u.Host = ""

	blob_uri := u.String()

	blob_bucket, err := bucket.OpenBucket(ctx, blob_uri)

	if err != nil {
		return err
	}

	wr.bucket = blob_bucket
	wr.scheme = scheme
	return nil
}

func (wr *BlobWriter) Write(ctx context.Context, uri string, fh io.ReadCloser) error {

	var wr_opts *blob.WriterOptions

	/*

		if wr.scheme == "s3" && wr.acl != "" {

			before := func(asFunc func(interface{}) bool) error {

				req := &s3manager.UploadInput{}
				ok := asFunc(&req)

				if !ok {
					return errors.New("invalid s3 type")
				}

				req.ACL = aws.String(bc.acl)
				return nil
			}

			wr_opts = &blob.WriterOptions{
				BeforeWrite: before,
			}
		}

	*/

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
