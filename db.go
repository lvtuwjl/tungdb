// Copyright 2023 The tung Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tung

//const maxMmapStep = 1 << 30 // 1GB
//
//// maxMapSize represents the largest mmap size supported by Bolt.
//const maxMapSize = 0xFFFFFFFFFFFF // 256TB
//
//// maxAllocSize is the size used when creating array pointers.
//const maxAllocSize = 0x7FFFFFFF
//
//// Default values if not set in a DB instance.
//const (
//	DefaultMaxBatchSize  int = 1000
//	DefaultMaxBatchDelay     = 10 * time.Millisecond
//	DefaultAllocSize         = 16 * 1024 * 1024
//)
//
//type DB struct {
//	StrictMode    int
//	NoSync        bool
//	NoGrowSync    bool
//	MmapFlags     int
//	MaxBatchSize  int
//	MaxBatchDelay time.Duration
//	AllocSize     int
//	path          string
//	file          *os.File
//	lockfile      *os.File
//	dataref       []byte
//	data          *[maxMapSize]byte
//	datasz        int
//	filesz        int
//	meta0         *meta
//	meta1         *meta
//	pageSize      int
//	opened        bool
//	rwtx          *Tx
//	txs           []*Tx
//	freelist      *freelist
//	stats         Stats
//	pagePool      sync.Pool
//	batchMu       sync.Mutex
//	batch         *batch
//	rwlock        sync.Mutex
//	metalock      sync.Mutex
//	mmaplock      sync.RWMutex
//	statlock      sync.RWMutex
//
//	ops struct {
//		writeAt func(b []byte, off int64) (n int, err error)
//	}
//	readOnly bool
//}
//
//type Stats struct {
//	FreePageN     int
//	PendingPageN  int
//	FreeAlloc     int
//	FreelistInuse int
//	TxN           int
//	OpenTxN       int
//	TxStats       TxStats
//}
//
//type batch struct {
//	db    *DB
//	timer *time.Timer
//	start sync.Once
//	calls []call
//}
//
//type call struct {
//	fn  func(*Tx) error
//	err chan<- error
//}
//
//type Options struct {
//	Timeout         time.Duration
//	NoGrowSync      bool
//	ReadOnly        bool
//	MmapFlags       int
//	InitialMmapSize int
//}
//
//var DefaultOptions = &Options{
//	Timeout:    0,
//	NoGrowSync: false,
//}
//
//func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
//	var db = &DB{opened: true}
//
//	if options == nil {
//		options = DefaultOptions
//	}
//
//	db.NoGrowSync = options.NoGrowSync
//	db.MmapFlags = options.MmapFlags
//
//	db.MaxBatchSize = DefaultMaxBatchSize
//	db.MaxBatchDelay = DefaultMaxBatchDelay
//	db.AllocSize = DefaultAllocSize
//
//	flag := os.O_RDWR
//	if options.ReadOnly {
//		flag = os.O_RDONLY
//		db.readOnly = true
//	}
//
//	db.path = path
//	var err error
//	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
//		_ = db.file.Close()
//		return nil, err
//	}
//
//	// lock
//	if err := syscall.Flock(int(db.file.Fd()), 0); err != nil {
//		_ = db.file.Close()
//		return nil, err
//	}
//
//	db.ops.writeAt = db.file.WriteAt
//
//	if info, err := db.file.Stat(); err != nil {
//		return nil, err
//	} else if info.Size() == 0 {
//		//if err:= db.init();err!=nil{
//		//	return nil,err
//		//}
//	} else {
//		var buf [0x1000]byte
//		if _, err := db.file.WriteAt(buf[:], 0); err == nil {
//			//m:=
//			fmt.Println()
//		}
//	}
//
//	db.pagePool = sync.Pool{
//		New: func() interface{} {
//			return make([]byte, db.pageSize)
//		},
//	}
//
//	if _, err := syscall.Mmap(int(db.file.Fd()), 0, 0, 0, 0); err != nil {
//		_ = db.file.Close()
//		return nil, err
//	}
//
//	db.freelist = nil
//	//db.freelist.read(db.pa)
//
//	return db, err
//}
//
//func (db *DB) mmap(minsz int) error {
//	db.mmaplock.Lock()
//	defer db.mmaplock.Unlock()
//
//	info, err := db.file.Stat()
//	if err != nil {
//		return fmt.Errorf("mmap stst error: %s", err)
//	} else if int(info.Size()) < db.pageSize*2 {
//		return fmt.Errorf("file size too small")
//	}
//
//	return nil
//}
//
//func (db *DB) munmap() error {
//	if err := munmap(db); err != nil {
//		return fmt.Errorf("unmap error: %s", err.Error())
//	}
//	return nil
//}
//
//// munmap unmaps a DB's data file from memory.
//func munmap(db *DB) error {
//	// Ignore the unmap if we have no mapped data.
//	if db.dataref == nil {
//		return nil
//	}
//
//	// Unmap using the original byte slice.
//	err := syscall.Munmap(db.dataref)
//	db.dataref = nil
//	db.data = nil
//	db.datasz = 0
//	return err
//}
//
//func (db *DB) mmapSize(size int) (int, error) {
//	// Double the size from 32KB until 1GB
//	for i := uint(15); i <= 30; i++ {
//		if size <= 1<<i {
//			return 1 << i, nil
//		}
//	}
//
//	if size > maxMapSize {
//		return 0, fmt.Errorf("mmap too large")
//	}
//
//	// If larger than 1GB then grow by 1GB at a time.
//	sz := int64(size)
//	if remainder := sz % int64(maxMmapStep); remainder > 0 {
//		sz += int64(maxMmapStep) - remainder
//	}
//
//	pageSize := int64(db.pageSize)
//	if (sz % pageSize) != 0 {
//		sz = ((sz / pageSize) + 1) * pageSize
//	}
//
//	if sz > maxMapSize {
//		sz = maxMapSize
//	}
//
//	return int(sz), nil
//}
//
//func (db *DB) Close() error {
//	db.rwlock.Lock()
//	defer db.rwlock.Unlock()
//
//	db.metalock.Lock()
//	defer db.metalock.Unlock()
//
//	db.mmaplock.RLock()
//	defer db.mmaplock.RUnlock()
//	return db.close()
//}
//
//func (db *DB) close() error {
//	// todo handle
//	return nil
//}
//
//func (db *DB) pageInBuffer(b []byte, id pgid) *page {
//	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
//}
