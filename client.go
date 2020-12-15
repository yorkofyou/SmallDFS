package main

import (
	"dfs"
	"flag"
	"strconv"
)

func main() {
	var client dfs.Client
	numFlag := flag.Int("which", 0, "the number of clients")
	flag.Parse()
	if *numFlag != 0 {
		client.Id = int64(*numFlag)
		client.Node.Directory = "SmallDFS/Client" + strconv.Itoa(*numFlag)
		client.Reset(*numFlag)
		client.Init(*numFlag)
		client.Run(*numFlag)
	}
}
