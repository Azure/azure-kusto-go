package filesystem

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestFormatDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  properties.DataFormat
	}{
		{".avro.zip", properties.AVRO},
		{".AVRO.GZ", properties.AVRO},
		{".csv", properties.CSV},
		{".json", properties.JSON},
		{".orc", properties.ORC},
		{".parquet", properties.Parquet},
		{".psv", properties.PSV},
		{".raw", properties.Raw},
		{".scsv", properties.SCSV},
		{".sohsv", properties.SOHSV},
		{".tsv", properties.TSV},
		{".txt", properties.TXT},
		{".whatever", properties.DFUnknown},
	}

	for _, test := range tests {
		got := properties.DataFormatDiscovery(test.input)
		if got != test.want {
			t.Errorf("TestFormatDiscovery(%s): got '%s', want '%s'", test.input, got, test.want)
		}
	}
}

func TestCompressionDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  properties.CompressionType
	}{
		{"https://somehost.somedomain.com:8080/v1/somestuff/file.gz", properties.GZIP},
		{"https://somehost.somedomain.com:8080/v1/somestuff/file.zip", properties.ZIP},
		{"/path/to/a/file.gz", properties.GZIP},
		{"/path/to/a/file.zip", properties.ZIP},
		{"/path/to/a/file", properties.CTNone},
	}

	for _, test := range tests {
		got := CompressionDiscovery(test.input)
		if got != test.want {
			t.Errorf("TestCompressionDiscoveryy(%s): got '%s', want '%s'", test.input, got, test.want)
		}
	}
}

type fakeBlobstore struct {
	out       *bytes.Buffer
	shouldErr bool
}

func (f *fakeBlobstore) uploadBlobStream(ctx context.Context, reader io.Reader, url azblob.BlockBlobURL, o azblob.UploadStreamToBlockBlobOptions) (azblob.CommonResponse, error) {
	if f.shouldErr {
		return nil, fmt.Errorf("error")
	}
	_, err := io.Copy(f.out, reader)
	return nil, err
}

func (f *fakeBlobstore) uploadBlobFile(ctx context.Context, fi *os.File, url azblob.BlockBlobURL, o azblob.UploadToBlockBlobOptions) (azblob.CommonResponse, error) {
	if f.shouldErr {
		return nil, fmt.Errorf("error")
	}
	_, err := io.Copy(f.out, fi)
	return nil, err
}

func TestLocalToBlob(t *testing.T) {
	t.Parallel()

	content := "hello world"
	u, err := url.Parse("https://account.windows.net")
	if err != nil {
		panic(err)
	}
	to := azblob.NewContainerURL(*u, nil)

	f, err := os.OpenFile("test_file", os.O_CREATE+os.O_RDWR, 0770)
	if err != nil {
		panic(err)
	}
	defer os.Remove(f.Name())
	f.Write([]byte(content))
	f.Close()

	fgzip, err := os.OpenFile("test_file.gz", os.O_CREATE+os.O_RDWR, 0770)
	if err != nil {
		panic(err)
	}
	defer os.Remove(fgzip.Name())

	zw := gzip.NewWriter(fgzip)

	_, err = zw.Write([]byte(content))
	if err != nil {
		panic(err)
	}
	zw.Close()

	_, err = ioutil.ReadFile(f.Name())
	if err != nil {
		panic(err)
	}

	tests := []struct {
		desc      string
		from      string
		props     *properties.All
		err       bool
		uploadErr bool
		errOp     errors.Op
		errKind   errors.Kind
	}{
		{
			desc:    "Can't open file",
			err:     true,
			from:    "/path/does/not/exist",
			errOp:   errors.OpFileIngest,
			errKind: errors.KLocalFileSystem,
		},
		{
			desc:    "Can't stat the file",
			err:     true,
			errOp:   errors.OpFileIngest,
			errKind: errors.KLocalFileSystem,
		},
		{
			desc:      "Upload Stream fails",
			from:      f.Name(),
			err:       true,
			uploadErr: true,
			errOp:     errors.OpFileIngest,
			errKind:   errors.KBlobstore,
		},
		{
			desc:      "Upload file fails",
			from:      f.Name(),
			err:       true,
			uploadErr: true,
			errOp:     errors.OpFileIngest,
			errKind:   errors.KBlobstore,
		},
		{
			desc: "Stream success",
			from: f.Name(),
		},
		{
			desc: "File success",
			from: fgzip.Name(),
		},
	}

	for _, test := range tests {
		fbs := &fakeBlobstore{shouldErr: test.uploadErr, out: &bytes.Buffer{}}

		in := &Ingestion{
			db:               "database",
			table:            "table",
			uploadBlobStream: fbs.uploadBlobStream,
			uploadBlobFile:   fbs.uploadBlobFile,
		}

		_, _, err := in.localToBlob(context.Background(), test.from, to, &properties.All{})
		switch {
		case err == nil && test.err:
			t.Errorf("TestLocalToBlob(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestLocalToBlob(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		gotBuf := &bytes.Buffer{}
		zr, err := gzip.NewReader(fbs.out)
		if err != nil {
			panic(err)
		}
		if _, err := io.Copy(gotBuf, zr); err != nil {
			t.Errorf("TestLocalToBlob(%s): on gzip decompress: err == %s", test.desc, err)
			continue
		}

		if gotBuf.String() != content {
			t.Errorf("TestLocalToBlob(%s): got %q, want %q", test.desc, gotBuf.String(), content)
		}
	}
}

func TestIsLocalFileSystem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"c:\\dir\\file", true},
		{"\\drive\\dir\\file", true},
		{"dir\\dir\\file", true},
		{"/mnt/dir/file", true},
		{"/dir/dir/file", true},
		{"./dir/file", true},
		{"./file", true},
		{"file", true},
		{"file:///drive/dir/file", true},
		{"https://server/resource", false},
		{"ftp://server/resource", false},
	}

	for _, test := range tests {
		got := IsFileSystem(test.input)
		if got != test.want {
			t.Errorf("TestIsLocalFileSystem(%s): got %v, want %v", test.input, got, test.want)
		}
	}
}
