package logger

type LogEntry struct { // TODO: Naming? Is this interface (or something similar) already described in another library?
	Info  chan string
	Error chan string
	Done  chan bool // TODO: not sure where this fits
}
