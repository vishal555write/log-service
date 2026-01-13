// package log

// import (
// 	"fmt"
// 	"os"
// 	"path"
// 	"time"

// 	api "github.com/vishal555write/proglog/api/v1"
// 	"google.golang.org/protobuf/proto"
// )

// type segment struct {
// 	store                  *store
// 	index                  *index
// 	baseOffset, nextOffset uint64
// 	config                 Config
// }

// func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
// 	s := &segment{
// 		baseOffset: baseOffset,
// 		config:     c,
// 	}
// 	var err error

// 	storeFile, err := os.OpenFile(path.Join(dir,fmt.Sprintf("%d%s",baseOffset,".store")),os.O_RDWR|os.O_CREATE|os.O_APPEND,0644,)
//        if err!=nil{
// 		return nil,err
// 	   }
// 	   if s.store,err=newStore(storeFile);err!=nil{
// 		return nil,err
// 	   }
//         indexFile,err:=os.OpenFile(path.Join(dir,fmt.Sprintf("%d%s",baseOffset,".index")),os.O_RDWR|os.O_CREATE,0644,)
// 		if err!=nil{
// 			return nil,err
// 		}
// 		if s.index,err=newIndex(indexFile,c);err!=nil{
// 			return nil,err
// 		}
// 		if off,_,err:=s.index.Read(-1);err!=nil{
// 			s.nextOffset=baseOffset
// 		}else{
// 			s.nextOffset=baseOffset+uint64(off)+1
// 		}
// 		return s,nil
// 		}

// func (s *segment) Append(record *api.Record)(offset uint64,err error){
// 	cur:=s.nextOffset
// 	record.Offset=cur
// 	p,err:=proto.Marshal(record)
// 	if err!=nil{
// 		return 0,err
// 	}
// 	_,pos,err:=s.store.Append(p)
// 	if err!=nil{
// 		return 0,err
// 	}
// 	if err=s.index.Write(uint32(s.nextOffset-uint64(s.baseOffset)),
// pos,);err!=nil{
// 	return 0,err
// }
// s.nextOffset++
// return cur,nil
// }

// func (s *segment) Read(off uint64) (*api.Record,error){
// 	_,pos,err:=s.index.Read(int64(off-s.baseOffset))
// 	if err!=nil{
// 		return nil,err
// 	}
// 	p,err:=s.store.Read(pos)
// 	if err!=nil{
// 		return nil,err
// 	}
// 	record:=&api.Record{}
// 	err=proto.Unmarshal(p,record)
// 	return record,nil
// }

// func (s *segment) IsMaxed() bool{
// 	return s.store.size>=s.config.Segment.MaxStoreBytes||s.index.size>=s.config.Segment.MaxIndexBytes
// }

// func (s *segment) Remove() error{
// 	if err:=s.Close();err!=nil{
// 		return err
// 	}
// 	if err:= os.Remove(s.index.Name());err != nil {
// 		time.Sleep(100 * time.Millisecond) // Give time for file release
// 		err = os.Remove(s.index.Name()) // Retry
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	if err:= os.Remove(s.store.Name());err!=nil{
// 		return err
// 	}
// 	return nil
// }

// func (s *segment) Close() error {
// 	if err:=s.index.Close();err!=nil{
// 		return err
// 	}
// 	if err:=s.store.Close();err!=nil{
// 		return err
// 	}
//    return nil
// }

// func nearMultiple(j,k uint64) uint64{
// 	if j>=0{
// 		return (j/k)*k
// 	}
// 	return ((j-k+1)/k)*k
// }

package log

import (
	"fmt"
	"os"
	"path"
	"time"

	api "github.com/vishal555write/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// open or create store file (append mode)
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	// open or create index file
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	// set nextOffset based on index
	// index.Read(-1) is expected to return last entry (offset, pos, nil) or an error when index empty
	off, _, err := s.index.Read(-1)
	if err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (uint64, error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	rel := uint32(s.nextOffset - s.baseOffset)
	if err := s.index.Write(rel, pos); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	rel := int64(off - s.baseOffset)
	_, pos, err := s.index.Read(rel)
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	if err := proto.Unmarshal(p, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	// attempt removal with a brief retry if the file is still in use
	if err := os.Remove(s.index.Name()); err != nil {
		time.Sleep(100 * time.Millisecond)
		if err = os.Remove(s.index.Name()); err != nil {
			return err
		}
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearMultiple simplified for unsigned arithmetic
func nearMultiple(j, k uint64) uint64 {
	return (j / k) * k
}
