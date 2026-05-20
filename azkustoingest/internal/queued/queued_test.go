package queued

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustoingest/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/properties"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/utils"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{".w3clogfile", properties.W3CLogFile},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.input, func(t *testing.T) {
			t.Parallel()

			got := properties.DataFormatDiscovery(test.input)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestCompressionDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  ingestoptions.CompressionType
	}{
		{"https://somehost.somedomain.com:8080/v1/somestuff/file.gz", ingestoptions.GZIP},
		{"https://somehost.somedomain.com:8080/v1/somestuff/file.zip", ingestoptions.ZIP},
		{"/path/to/a/file.gz", ingestoptions.GZIP},
		{"/path/to/a/file.zip", ingestoptions.ZIP},
		{"/path/to/a/file", ingestoptions.CTNone},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.input, func(t *testing.T) {
			t.Parallel()

			got := utils.CompressionDiscovery(test.input)
			assert.Equal(t, test.want, got)
		})
	}

}

type fakeBlobstore struct {
	out       *bytes.Buffer
	shouldErr bool
	blobName  string
}

func (f *fakeBlobstore) uploadBlobStream(_ context.Context, reader io.Reader, _ *azblob.Client, _ string, blob string, _ *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
	if f.shouldErr {
		return azblob.UploadStreamResponse{}, fmt.Errorf("error")
	}
	f.blobName = blob
	_, err := io.Copy(f.out, reader)
	return azblob.UploadStreamResponse{}, err
}

func (f *fakeBlobstore) uploadBlobFile(_ context.Context, fi *os.File, _ *azblob.Client, _ string, _ string, _ *azblob.UploadFileOptions) (azblob.UploadFileResponse, error) {
	if f.shouldErr {
		return azblob.UploadFileResponse{}, fmt.Errorf("error")
	}
	_, err := io.Copy(f.out, fi)
	return azblob.UploadFileResponse{}, err
}

func TestLocalToBlob(t *testing.T) {
	t.Parallel()

	content := "hello world"
	u := "https://account.windows.net"
	to, err := azblob.NewClientWithNoCredential(u, nil)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile("test_file", os.O_CREATE+os.O_RDWR, 0770)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = os.Remove(f.Name())
	})
	_, _ = f.Write([]byte(content))
	_ = f.Close()

	fgzip, err := os.OpenFile("test_file.gz", os.O_CREATE+os.O_RDWR, 0770)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		_ = os.Remove(fgzip.Name())
	})

	zw := gzip.NewWriter(fgzip)

	_, err = zw.Write([]byte(content))
	if err != nil {
		panic(err)
	}
	_ = zw.Close()

	_, err = os.ReadFile(f.Name())
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
			db:           "database",
			table:        "table",
			uploadStream: fbs.uploadBlobStream,
			uploadBlob:   fbs.uploadBlobFile,
		}

		_, _, err := in.localToBlob(context.Background(), test.from, to, "test", &properties.All{})
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

type fileInfo struct {
	os.FileInfo
	isDir bool
}

func (f fileInfo) IsDir() bool {
	return f.isDir
}

func fakeStat(name string) (os.FileInfo, error) {
	switch name {
	case "c:\\dir\\file":
		return fileInfo{}, nil
	case "/mnt/dir/":
		return fileInfo{isDir: true}, nil
	}
	return nil, fmt.Errorf("error")
}

func TestIsLocalPath(t *testing.T) {
	statFunc = fakeStat
	t.Cleanup(func() {
		statFunc = os.Stat
	})

	tests := []struct {
		desc string
		path string
		err  bool
		want bool
	}{
		{
			desc: "error: valid path to local dir",
			path: "/mnt/dir",
			err:  true,
		},
		{
			desc: "error: invalid remote path ftp",
			path: "ftp://some.ftp.com",
			err:  true,
		},
		{
			desc: "success: valid http path",
			path: "http://some.http.com/path",
			want: false,
		},
		{
			desc: "success: valid https path",
			path: "https://some.https.com/path",
			want: false,
		},
		{
			desc: "success: valid path to local file",
			path: "c:\\dir\\file",
			want: true,
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			got, err := IsLocalPath(test.path)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestShouldCompress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		props *properties.All
		want  bool
	}{
		{
			name: "Some file",
			props: &properties.All{Source: properties.SourceOptions{CompressionType: ingestoptions.CTUnknown,
				OriginalSource: "https://somehost.somedomain.com:8080/v1/somestuff/file"}},
			want: true,
		},
		{
			name: "Some file2",
			props: &properties.All{Source: properties.SourceOptions{CompressionType: ingestoptions.CTNone,
				OriginalSource: "https://somehost.somedomain.com:8080/v1/somestuff/file"}},
			want: true,
		},
		{
			name: "Provided compression type is GZIP",
			props: &properties.All{Source: properties.SourceOptions{CompressionType: ingestoptions.GZIP,
				OriginalSource: "https://somehost.somedomain.com:8080/v1/somestuff/file"}},
			want: false,
		},
		{
			name: "Guess by name is GZIP",
			props: &properties.All{Source: properties.SourceOptions{CompressionType: ingestoptions.CTUnknown,
				OriginalSource: "https://somehost.somedomain.com:8080/v1/somestuff/file.gz"}},
			want: false,
		},
		{
			name: "DontCompress is true",
			props: &properties.All{Source: properties.SourceOptions{CompressionType: ingestoptions.CTNone,
				DontCompress:   true,
				OriginalSource: "https://somehost.somedomain.com:8080/v1/somestuff/file"}},
			want: false,
		},
		{
			name: "Binary format",
			props: &properties.All{Source: properties.SourceOptions{CompressionType: ingestoptions.CTNone,
				OriginalSource: "https://somehost.somedomain.com:8080/v1/somestuff/file.avro"}},
			want: false,
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := CompleteFormatFromFileName(test.props, test.props.Source.OriginalSource)
			assert.NoError(t, err)

			got := ShouldCompress(test.props,
				utils.CompressionDiscovery(test.props.Source.OriginalSource))
			assert.Equal(t, test.want, got)
		})
	}
}

func TestGenBlobName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		fileName              string
		compressionFromSource ingestoptions.CompressionType
		shouldCompress        bool
		dataFormat            string
		expectedSuffix        string
	}{
		{
			name:                  "should compress always yields gz",
			fileName:              "file",
			compressionFromSource: ingestoptions.CTNone,
			shouldCompress:        true,
			dataFormat:            "csv",
			expectedSuffix:        ".gz",
		},
		{
			name:                  "no compression and no source compression uses csv",
			fileName:              "file",
			compressionFromSource: ingestoptions.CTNone,
			shouldCompress:        false,
			dataFormat:            "csv",
			expectedSuffix:        ".csv",
		},
		{
			name:                  "no compression and no source compression uses json",
			fileName:              "file",
			compressionFromSource: ingestoptions.CTNone,
			shouldCompress:        false,
			dataFormat:            "json",
			expectedSuffix:        ".json",
		},
		{
			name:                  "unknown source compression falls back to format",
			fileName:              "file",
			compressionFromSource: ingestoptions.CTUnknown,
			shouldCompress:        false,
			dataFormat:            "csv",
			expectedSuffix:        ".csv",
		},
		{
			name:                  "explicit gzip source compression keeps gz suffix",
			fileName:              "file",
			compressionFromSource: ingestoptions.GZIP,
			shouldCompress:        false,
			dataFormat:            "csv",
			expectedSuffix:        ".gz",
		},
		{
			name:                  "explicit zip source compression keeps zip suffix",
			fileName:              "file",
			compressionFromSource: ingestoptions.ZIP,
			shouldCompress:        false,
			dataFormat:            "csv",
			expectedSuffix:        ".zip",
		},
		{
			name:                  "double extension prevention - file.json.gz with GZIP",
			fileName:              "data.json.gz",
			compressionFromSource: ingestoptions.GZIP,
			shouldCompress:        false,
			dataFormat:            "json",
			expectedSuffix:        ".gz",
		},
		{
			name:                  "double extension prevention - file.csv.zip with ZIP",
			fileName:              "data.csv.zip",
			compressionFromSource: ingestoptions.ZIP,
			shouldCompress:        false,
			dataFormat:            "csv",
			expectedSuffix:        ".zip",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			blobName := GenBlobName("db", "table", nower(), "guid", tt.fileName, tt.compressionFromSource, tt.shouldCompress, tt.dataFormat)
			assert.True(t, strings.HasSuffix(blobName, tt.expectedSuffix), "expected %q to have suffix %q", blobName, tt.expectedSuffix)

			// Verify no double compression extensions
			assert.False(t, strings.HasSuffix(blobName, ".gz.gz"), "should not have double .gz extension in %q", blobName)
			assert.False(t, strings.HasSuffix(blobName, ".zip.zip"), "should not have double .zip extension in %q", blobName)
		})
	}
}

func TestUploadReaderToBlobRespectsExplicitCompressionTypeForBlobName(t *testing.T) {
	t.Parallel()

	const content = "The quick brown fox jumps over the lazy dog"

	var compressed bytes.Buffer
	gzw := gzip.NewWriter(&compressed)
	_, err := gzw.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, gzw.Close())

	fbs := &fakeBlobstore{out: &bytes.Buffer{}}
	i := &Ingestion{
		db:           "database",
		table:        "table",
		uploadStream: fbs.uploadBlobStream,
		mgr: newFakeResourceManager(
			[]string{"https://account.blob.core.windows.net/container"},
			[]string{"https://account.queue.core.windows.net/queue"},
			nil,
		),
	}

	_, _, err = i.UploadReaderToBlob(t.Context(), bytes.NewReader(compressed.Bytes()), properties.All{
		Source: properties.SourceOptions{
			CompressionType: ingestoptions.GZIP,
		},
		Ingestion: properties.Ingestion{
			Additional: properties.Additional{Format: properties.CSV},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, compressed.Bytes(), fbs.out.Bytes(), "reader payload should not be recompressed when source is already gzip")
	assert.True(t, strings.HasSuffix(fbs.blobName, ".gz"), "expected blob name to retain gzip extension, got %q", fbs.blobName)
}

type retryingBlobstore struct {
	out            *bytes.Buffer
	remainingFails atomic.Int32 // remaining failures before success
}

func newRetryingBlobstore(out *bytes.Buffer, failCount int) *retryingBlobstore {
	r := &retryingBlobstore{out: out}
	r.remainingFails.Store(int32(failCount))
	return r
}

func (r *retryingBlobstore) uploadBlobStream(_ context.Context, reader io.Reader, _ *azblob.Client, _ string, _ string, _ *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
	remaining := r.remainingFails.Add(-1)

	if remaining >= 0 {
		_, _ = io.Copy(io.Discard, reader) // Consume some content to simulate failed upload
		return azblob.UploadStreamResponse{}, fmt.Errorf("simulated upload failure, %d remaining", remaining)
	}

	_, err := io.Copy(r.out, reader)
	return azblob.UploadStreamResponse{}, err
}

type fakeResourceManager struct {
	storageContainers []*resources.URI
	storageQueues     []*resources.URI
	tables            []*resources.URI
}

var _ resources.ResourcesManager = (*fakeResourceManager)(nil)

func mustParseURIs(uris ...string) []*resources.URI {
	parsed := make([]*resources.URI, len(uris))
	for i, uri := range uris {
		uriParsed, err := resources.Parse(uri)
		if err != nil {
			panic(err)
		}
		parsed[i] = uriParsed
	}
	return parsed
}

func newFakeResourceManager(storageContainers []string, storageQueues []string, tables []string) *fakeResourceManager {
	return &fakeResourceManager{
		storageContainers: mustParseURIs(storageContainers...),
		storageQueues:     mustParseURIs(storageQueues...),
		tables:            mustParseURIs(tables...),
	}
}

func (f fakeResourceManager) ReportStorageResourceResult(_ string, _ bool) {

}

func (f fakeResourceManager) GetRankedStorageContainers() ([]*resources.URI, error) {
	return f.storageContainers, nil
}

func (f fakeResourceManager) GetRankedStorageQueues() ([]*resources.URI, error) {
	return f.storageQueues, nil
}

func (f fakeResourceManager) GetTables() ([]*resources.URI, error) {
	return f.tables, nil
}

func (f fakeResourceManager) Close() {
}

type nonSeekableReader struct {
	r io.Reader
}

func newNonSeekableReader(r io.Reader) *nonSeekableReader {
	return &nonSeekableReader{r: r}
}

func (n *nonSeekableReader) Read(p []byte) (int, error) {
	return n.r.Read(p)
}

func TestReaderRetry(t *testing.T) {
	t.Parallel()

	const content = "The quick brown fox jumps over the lazy dog"

	tests := []struct {
		name           string
		shouldCompress bool
		failCount      int
		nonSeekable    bool
		failError      string
	}{
		{
			name:           "success on first attempt without compression",
			shouldCompress: false,
			failCount:      0,
		},
		{
			name:           "success after 2 retries without compression",
			shouldCompress: false,
			failCount:      2,
		},
		{
			name:           "success on first attempt with compression",
			shouldCompress: true,
			failCount:      0,
		},
		{
			name:           "success after 2 retries with compression",
			shouldCompress: true,
			failCount:      2,
		},
		{
			name:           "success on first attempt without compression non-seekable reader",
			shouldCompress: false,
			failCount:      0,
			nonSeekable:    true,
		},
		{
			name:           "fail without compression non-seekable reader",
			shouldCompress: false,
			failCount:      2,
			nonSeekable:    true,
			failError:      "reader does not support seeking, cannot retry: simulated upload failure",
		},
		{
			name:           "success on first attempt with compression non-seekable reader",
			shouldCompress: true,
			failCount:      0,
			nonSeekable:    true,
		},
		{
			name:           "fail with compression non-seekable reader",
			shouldCompress: true,
			failCount:      2,
			nonSeekable:    true,
			failError:      "reader does not support seeking, cannot retry: simulated upload failure",
		},
	}

	compressedFormat := properties.TXT
	assert.True(t, compressedFormat.ShouldCompress())

	mgr := newFakeResourceManager(
		[]string{
			"https://account.blob.core.windows.net/container",
			"https://account.blob.core.windows.net/container2",
			"https://account.blob.core.windows.net/container3",
			"https://account.blob.core.windows.net/container4",
			"https://account.blob.core.windows.net/container5",
			"https://account.blob.core.windows.net/container6",
		},
		[]string{
			"https://account.queue.core.windows.net/queue",
		},
		nil,
	)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			if len(mgr.storageContainers) <= tc.failCount {
				r.FailNow("not enough storage containers to test retry logic")
			}

			finalOutput := new(bytes.Buffer)
			i := &Ingestion{
				uploadStream: newRetryingBlobstore(finalOutput, tc.failCount).uploadBlobStream,
				mgr:          mgr,
			}

			var reader io.Reader = bytes.NewReader([]byte(content))
			if tc.nonSeekable {
				reader = newNonSeekableReader(reader)
			}

			_, _, err := i.UploadReaderToBlob(t.Context(), reader, properties.All{
				Source: properties.SourceOptions{
					DontCompress: !tc.shouldCompress,
				},
				Ingestion: properties.Ingestion{
					DatabaseName: "", // empty string to fail Ingestion.Blob
					Additional: properties.Additional{
						Format:      compressedFormat,
						AuthContext: "token",
					},
				},
			})

			if tc.failError != "" {
				r.ErrorContains(err, tc.failError)
				return
			} else {
				r.NoError(err)
			}

			var output string
			if tc.shouldCompress {
				gr, err := gzip.NewReader(finalOutput)
				r.NoError(err)
				decompressed, err := io.ReadAll(gr)
				r.NoError(err)
				output = string(decompressed)
			} else {
				output = finalOutput.String()
			}
			r.Equal(content, output)
		})
	}
}
