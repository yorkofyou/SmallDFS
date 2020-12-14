package dfs

const ClientNum int = 4
const SplitUnit int = 100000
const Redundance int = 2
const ChunkNum int = 100000

type ChunkUnit []byte
type Namespace map[string]File
type File struct {
	Info   string
	Size   int
	Chunks [ChunkNum]Chunk
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
	Node       Node
	Clients    []Client
	Redundance int
}
type Client struct {
	Node           Node
	MasterLocation string
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
