package main

import (
	"bufio"
	"io"
	"strconv"
)

var (
	arrayPrefixSlice      = []byte{'*'}
	bulkStringPrefixSlice = []byte{'$'}
	lineEndingSlice       = []byte{'\r', '\n'}
)

// RESPWriter is helper to write redis commmands.
type RESPWriter struct {
	*bufio.Writer
}

// NewRESPWriter creates a RESPWriter.
func NewRESPWriter(writer io.Writer) *RESPWriter {
	return &RESPWriter{
		Writer: bufio.NewWriter(writer),
	}
}

// WriteCommand writes the command and args in bytes.
func (w *RESPWriter) WriteCommand(args ...[]byte) (err error) {
	w.Write(arrayPrefixSlice)
	w.WriteString(strconv.Itoa(len(args)))
	w.Write(lineEndingSlice)

	for _, arg := range args {
		w.Write(bulkStringPrefixSlice)
		w.WriteString(strconv.Itoa(len(arg)))
		w.Write(lineEndingSlice)
		w.Write(arg)
		w.Write(lineEndingSlice)
	}

	return w.Flush()
}

// WriteStrCommand writes the command and args in string.
func (w *RESPWriter) WriteStrCommand(args ...string) (err error) {
	w.Write(arrayPrefixSlice)
	w.WriteString(strconv.Itoa(len(args)))
	w.Write(lineEndingSlice)

	for _, arg := range args {
		w.Write(bulkStringPrefixSlice)
		w.WriteString(strconv.Itoa(len(arg)))
		w.Write(lineEndingSlice)
		w.WriteString(arg)
		w.Write(lineEndingSlice)
	}

	return w.Flush()
}
