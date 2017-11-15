package configutils

import "os"

type FileWriter interface {
	Write(*os.File, []byte) error
}
