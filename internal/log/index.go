// package log

// import (
// 	"io"
// 	"os"

// 	"github.com/tysonmote/gommap"
// )

// var (
// 	offWidth  uint64 = 4
// 	posWidth uint64 = 8
// 	entWidth  uint64 = offWidth + posWidth
// )

// type index struct {
// 	file *os.File
// 	mmap gommap.MMap
//     size uint64
// }

// func newIndex(f *os.File,c Config) (*index,error){
// 	idx:=&index{
// 		file:f,
// 	}

// 	fi,err:=os.Stat(f.Name())
// 	if err!=nil{
// 		return nil,err
// 	}
// 	idx.size=uint64(fi.Size())
// 	if err=os.Truncate(f.Name(),int64(c.Segment.MaxIndexBytes));err!=nil{
// 		return nil,err
// 	}

// 	if idx.mmap,err=gommap.Map(idx.file.Fd(),gommap.PROT_READ|gommap.PROT_WRITE,gommap.MAP_SHARED,);err!=nil{
// 			return nil,err
// 		}

// 	return idx,nil

// }

// func (i *index) Close()error{
// 	if err:=i.mmap.Sync(gommap.MS_SYNC);err!=nil{
// 		return err
// 	}
// 	if err:=i.file.Sync();err!=nil{
// 		return err
// 	}
// 	if err:=i.file.Truncate(int64(i.size));err!=nil{
// 		return err
// 	}
// 	if err := i.mmap.UnsafeUnmap(); err != nil {
// 		return err
// 	}
// 	return i.file.Close()
// }

// func(i *index) Read(in int64) (out uint32,pos uint64, err error){
// 	if i.size == 0{      //check if file empty
// 		return 0,0,io.EOF
// 	}
// 	if in == -1{             //read from end of line
// 		out = uint32((i.size/entWidth)-1)
// 	}else{
//          out=uint32(in)
// 	}
// 	pos=uint64(out)*entWidth   //absolute postition of index start
// 	if i.size<pos+entWidth{
// 		return 0,0,io.EOF
// 	}
// 	out=enc.Uint32(i.mmap[pos : pos+offWidth])   //first 4B offset byte
// 	pos = enc.Uint64(i.mmap[pos+offWidth:pos+entWidth])  // next 8B offset byte
// 	return out,pos,nil
// }

// func (i *index) Write(off uint32,pos uint64) error{
// 	if uint64(len(i.mmap))<i.size+entWidth{
// 		return io.EOF
// 	}
// 	enc.PutUint32(i.mmap[i.size:i.size+offWidth],off)
// 	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth],pos)
// 	i.size+=entWidth
// 	return nil
// }

// func (i *index) Name()string{
// 	return i.file.Name()
// }

package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

/*
 Index record format:
 ------------------------------------------------
 | offset (uint32 - 4 bytes) | position (uint64 - 8 bytes) |
 ------------------------------------------------
 Total: 12 bytes per index entry.
*/

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth uint64 = offWidth + posWidth
	//enc= binary.BigEndian
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // how many bytes are actually written (0..mmapSize)
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// Get current file size
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Grow file to maximum allowed index bytes
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// mmap entire file (correct gommap signature)
	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		// last entry index
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// absolute byte position in mmap
	entryPos := uint64(out) * entWidth

	if i.size < entryPos+entWidth {
		return 0, 0, io.EOF
	}

	// decode: first 4 bytes offset
	offset := enc.Uint32(i.mmap[entryPos : entryPos+offWidth])
	// decode: next 8 bytes position
	position := enc.Uint64(i.mmap[entryPos+offWidth : entryPos+entWidth])

	return offset, position, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// write offset
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// write position
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
