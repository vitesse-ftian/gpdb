package testUtils

import (
	"os"
	"time"
)

type FakeFileInfo struct {
	name    string      // base name of the file
	size    int64       // length in bytes for regular files; system-dependent for others
	mode    os.FileMode // file mode bits
	modTime time.Time   // modification time
	isDir   bool        // abbreviation for Mode().IsDir()
	sys     interface{}
}

func (f FakeFileInfo) Name() string {
	return f.name
}

func (f FakeFileInfo) Size() int64 {
	return f.size
}
func (f FakeFileInfo) Mode() os.FileMode {
	return f.mode
}
func (f FakeFileInfo) ModTime() time.Time {
	return f.modTime
}
func (f FakeFileInfo) IsDir() bool {
	return f.isDir
}

func (f FakeFileInfo) Sys() interface{} {
	return nil
}
