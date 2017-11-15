package configutils

type Formatter interface {
	Format([]byte) ([]byte, error)
}
