package main

import (
	"dfs"
)

func main() {
	var master dfs.Master
	master.Node.Directory = "SmallDFS/Master"
	master.Reset()
	master.Init()
	master.Run()
}
