package writer

import (
	_ "gocloud.dev/blob/fileblob"
)

import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-writer/v3"
	"os"
	"path/filepath"
	"testing"
)

func TestFileWriter(t *testing.T) {

	ctx := context.Background()

	cwd, err := os.Getwd()

	if err != nil {
		t.Fatal(err)
	}

	tmpdir := os.TempDir()
	data_root := filepath.Join(tmpdir, "data")

	_, err = os.Stat(data_root)

	if err != nil {

		err := os.MkdirAll(data_root, 0755)

		if err != nil {
			t.Fatal(err)
		}
	}

	target_root := fmt.Sprintf("file://%s", data_root)

	source_root := filepath.Join(cwd, "fixtures")
	feature_path := filepath.Join(source_root, "101736545.geojson")

	target_path := "101/736/545/101736545.geojson"

	wr, err := writer.NewWriter(ctx, target_root)

	if err != nil {
		t.Fatal(err)
	}

	feature_fh, err := os.Open(feature_path)

	if err != nil {
		t.Fatal(err)
	}

	defer feature_fh.Close()

	_, err = wr.Write(ctx, target_path, feature_fh)

	if err != nil {
		t.Fatal(err)
	}

	test_path := filepath.Join(data_root, target_path)

	_, err = os.Stat(test_path)

	if err != nil {
		t.Fatal(err)
	}

	err = os.RemoveAll(data_root)

	if err != nil {
		t.Fatal(err)
	}

}
