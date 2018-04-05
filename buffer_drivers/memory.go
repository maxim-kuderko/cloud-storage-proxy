package buffer_drivers

import (
	"bytes"
	"errors"
)

type MemBuffer struct {
	buff []bytes.Buffer
}


func (mb *MemBuffer) Read(p []byte) (n int, err error){
	return 0, errors.New("")
}
func (mb *MemBuffer) Write(p []byte) (n int, err error){
	return 0, errors.New("")
}


func (mb *MemBuffer) Close() error{
	return errors.New("")
}