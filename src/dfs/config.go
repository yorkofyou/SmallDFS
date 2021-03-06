package dfs

import (
	"hash/fnv"
	"sync"
)

const ClientNum int = 4
const SplitUnit int = 1048576
const Redundance int = 2

type ChunkUnit []byte
type Namespace map[string]File
type File struct {
	Info   string
	Size   int
	Chunks []Chunk
	Offset int
}
type Chunk struct {
	Info     string
	Replicas []ReplicaLocation
}
type ReplicaLocation struct {
	Location   string
	ReplicaNum int
}
type Master struct {
	mu             sync.Mutex
	Node           Node
	Clients        []Client
	Redundance     int
	MapAlready     bool
	ReduceAlready  bool
	MapFinished    []bool
	ReduceFinished []bool
}
type Client struct {
	Id             int64
	Node           Node
	MasterLocation string
	Files          []string
}
type Node struct {
	Namespace    Namespace
	Port         int
	Directory    string
	Location     string
	StorageSum   int
	StorageAvail int
	ChunkAvail   []int
	LastEdit     int64
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
