package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/lvtuwjl/tungdb/tung/config"
	"github.com/lvtuwjl/tungdb/tung/kv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// TableTree 树
type TableTree struct {
	levels []*tableNode
	// 用于避免进行插入或压缩，删除SSTable时发生冲突
	mu sync.RWMutex
}

// 链表，表示每一层的SSTable
type tableNode struct {
	index int
	table *SSTable
	next  *tableNode
}

func (t *TableTree) insert(table *SSTable, level int) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 每次插入的 都出现在最后
	node := t.levels[level]
	newNode := &tableNode{
		table: table,
		next:  nil,
		index: 0,
	}

	if node == nil {
		t.levels[level] = newNode
	} else {
		for node != nil {
			if node.next == nil {
				newNode.index = node.index + 1
				node.next = newNode
				break
			} else {
				node = node.next
			}
		}
	}
	return newNode.index
}

func (t *TableTree) Search(key string) (kv.KV, kv.Status) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// 遍历每一层的SSTable
	for _, node := range t.levels {
		// 整理SSTable列表
		tables := make([]*SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}

		// 查找的时候要从最后一个SSTable开始查找
		for i := len(tables) - 1; i >= 0; i-- {
			value, searchResult := tables[i].Search(key)
			// 未找到 则查找下一个SSTable表
			if searchResult == kv.StatusNone {
				continue
			} else {
				// 如果找到或已被删除 则返回结果
				return value, searchResult
			}
		}
	}

	return kv.KV{}, kv.StatusNone
}

// 获取一层中的 SSTable的最大序号
func (t *TableTree) getMaxIndex(level int) int {
	node := t.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}

// 获取该层有多少个SSTable
func (t *TableTree) getCount(level int) int {
	node := t.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}

// 获取一个db文件所代表的SSTable的所在层数和索引
func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}

// 创建新的SSTable
func (t *TableTree) CreateNewTable(values []kv.KV) {
	t.createTable(values, 0)
}

// 创建新的SSTable，插入到合适的层
func (t *TableTree) createTable(values []kv.KV, level int) *SSTable {
	// 生成数据区
	keys := make([]string, 0, len(values))
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, value := range values {
		data, err := kv.Encode(value)
		if err != nil {
			log.Println("Failed to insert Key: ", value.Key, err)
			continue
		}

		keys = append(keys, value.Key)
		// 文件定位记录
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Status == kv.StatusDeleted,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	// map[string]Position to json
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("An SSTable file cannot be created,", err)
	}

	// 生成MetaInfo
	meta := MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}

	table := &SSTable{
		tableMetaInfo: meta,
		sparseIndex:   positions,
		sortIndex:     keys,
		mu:            sync.Mutex{},
	}

	index := t.insert(table, level)
	log.Printf("Create a new SSTable,level: %d ,index: %d\r\n", level, index)
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filePath = filePath

	writeDataToFile(filePath, dataArea, indexArea, meta)
	// 以只读的形式打开文件
	f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicln("error open file", table.filePath)
	}
	table.file = f

	return table
}

// 获取指定层的SSTable总大小
func (t *TableTree) GetLevelSize(level int) int64 {
	var size int64
	node := t.levels[level]
	for node != nil {
		size += node.table.GetDbSize()
		node = node.next
	}
	return size
}

func writeDataToFile(filePath string, dataArea []byte, indexArea []byte, meta MetaInfo) {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("error create file,", err)
	}
	_, err = f.Write(dataArea)
	if err != nil {
		log.Fatal("error write file,", err)
	}

	_, err = f.Write(indexArea)
	if err != nil {
		log.Fatal("error write file,", err)
	}

	// 写入元数据到文件末尾
	// 注意 右侧必须能够识别字节长度的类型 不能使用int这种类型 只能使用int32 int64等
	_ = binary.Write(f, binary.LittleEndian, &meta.version)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexLen)

	err = f.Sync()
	if err != nil {
		log.Fatal("err write file,", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal("error close file,", err)
	}
}

// 加载一个 db 文件到 TableTree 中
func (tree *TableTree) loadDbFile(path string) {
	log.Println("Loading the ", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loading the ", path, ",Consumption of time : ", elapse)
	}()

	level, index, err := getLevel(filepath.Base(path))
	if err != nil {
		return
	}
	table := &SSTable{}
	table.Init(path)
	newNode := &tableNode{
		index: index,
		table: table,
	}

	currentNode := tree.levels[level]

	if currentNode == nil {
		tree.levels[level] = newNode
		return
	}
	if newNode.index < currentNode.index {
		newNode.next = currentNode
		tree.levels[level] = newNode
		return
	}

	// 将 SSTable 插入到合适的位置
	for currentNode != nil {
		if currentNode.next == nil || newNode.index < currentNode.next.index {
			newNode.next = currentNode.next
			currentNode.next = newNode
			break
		} else {
			currentNode = currentNode.next
		}
	}
}

// 加载文件句柄
func (table *SSTable) loadFileHandle() {
	if table.file == nil {
		// 以只读的形式打开文件
		f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(" error open file ", table.filePath)
			panic(err)
		}

		table.file = f
	}
	// 加载文件句柄的同时，加载表的元数据
	table.loadMetaInfo()
	table.loadSparseIndex()
}

// 加载 SSTable 文件的元数据，从 SSTable 磁盘文件中读取出 TableMetaInfo
func (table *SSTable) loadMetaInfo() {
	f := table.file
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	info, _ := f.Stat()
	_, err = f.Seek(info.Size()-8*5, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.version)

	_, err = f.Seek(info.Size()-8*4, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataStart)

	_, err = f.Seek(info.Size()-8*3, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataLen)

	_, err = f.Seek(info.Size()-8*2, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexStart)

	_, err = f.Seek(info.Size()-8*1, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexLen)
}

// 加载稀疏索引区到内存
func (table *SSTable) loadSparseIndex() {
	// 加载稀疏索引区
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	if _, err := table.file.Seek(table.tableMetaInfo.indexStart, 0); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	if _, err := table.file.Read(bytes); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}

	// 反序列化到内存
	table.sparseIndex = make(map[string]Position)
	err := json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	_, _ = table.file.Seek(0, 0)

	// 先排序
	keys := make([]string, 0, len(table.sparseIndex))
	for k := range table.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	table.sortIndex = keys
}

var levelMaxSize []int

// Init 初始化 TableTree
func (tree *TableTree) Init(dir string) {
	log.Println("The SSTable list are being loaded")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("The SSTable list are being loaded,consumption of time : ", elapse)
	}()

	// 初始化每一层 SSTable 的文件总最大值
	con := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = con.Level0Size
	levelMaxSize[1] = levelMaxSize[0] * 10
	levelMaxSize[2] = levelMaxSize[1] * 10
	levelMaxSize[3] = levelMaxSize[2] * 10
	levelMaxSize[4] = levelMaxSize[3] * 10
	levelMaxSize[5] = levelMaxSize[4] * 10
	levelMaxSize[6] = levelMaxSize[5] * 10
	levelMaxSize[7] = levelMaxSize[6] * 10
	levelMaxSize[8] = levelMaxSize[7] * 10
	levelMaxSize[9] = levelMaxSize[8] * 10

	tree.levels = make([]*tableNode, 10)
	//tree.lock = &sync.RWMutex{}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	for _, info := range infos {
		// 如果是 SSTable 文件
		if path.Ext(info.Name()) == ".db" {
			tree.loadDbFile(path.Join(dir, info.Name()))
		}
	}
}
