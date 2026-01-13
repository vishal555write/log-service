package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const(
	lenWidth=8 //no of bytes useed to store the records length
)

type store struct{
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store,error){
fi,err:=os.Stat(f.Name()) //to get the file current size
if err!=nil{
	return nil,err
}
size:=uint64(fi.Size())
return &store{
	File: f,
	size: size,
	buf: bufio.NewWriter(f),
},nil
}

func(s *store) Append(p []byte) (n uint64,pos uint64,err error){
	s.mu.Lock()
	defer s.mu.Unlock()
	pos=s.size //segment will use this position while creating an associate index entry for this record
	if err:=binary.Write(s.buf,enc,uint64(len(p)));err!=nil{
		return 0,0,err
	}
	w,err:=s.buf.Write(p)
	if err!=nil{
		return 0,0,err
	}
	w+=lenWidth
	s.size+=uint64(w)
	return uint64(w),pos,nil
}

func(s *store) Read(pos uint64) ([]byte, error) {
	// Ensure data is written before reading
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Read the length prefix
	sizeBuf := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(sizeBuf, int64(pos)); err != nil {
		return nil, err
	}

	// Convert sizeBuf to uint64 safely
	size := enc.Uint64(sizeBuf)
	if size == 0 {
		return nil, fmt.Errorf("invalid entry size at pos %d", pos)
	}



	// Read the actual data
	data := make([]byte, size)
	if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *store) ReadAt(p []byte, off int64) (int,error){
	if err:=s.buf.Flush();err!=nil{
		return 0,err
	}
s.mu.Lock()
defer s.mu.Unlock()
return s.File.ReadAt(p,off)
}

func (s *store) Close() error{
	s.mu.Lock()
	defer s.mu.Unlock()
	err:=s.buf.Flush()
	if err!=nil{
		return err
	}
	return s.File.Close()
}




