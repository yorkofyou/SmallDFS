package main

import (
	"dfs"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
)

func main() {
	numFlag := flag.Int("which", 0, "the number of clients")
	putFlag := flag.String("put", "unknown", "input file name")
	getFlag := flag.String("get", "unknown", "input file name")
	deleteFlag := flag.String("delete", "unknown", "input file name")
	taskFlag := flag.Int("task", 0, "the number of tasks")
	flag.Parse()
	var client dfs.Client
	var master dfs.Master
	if *numFlag != 0 {
		if *numFlag == 1 {
			response, err := http.Get("http://localhost:11091" + "/getmeta")
			if err != nil {
				fmt.Println(err.Error())
			}
			defer response.Body.Close()
			err = json.NewDecoder(response.Body).Decode(&client)
			if err != nil {
				fmt.Println(err.Error())
			}
		} else if *numFlag == 2 {
			response, err := http.Get("http://localhost:11092" + "/getmeta")
			if err != nil {
				fmt.Println(err.Error())
			}
			defer response.Body.Close()
			err = json.NewDecoder(response.Body).Decode(&client)
			if err != nil {
				fmt.Println(err.Error())
			}
		} else if *numFlag == 3 {
			response, err := http.Get("http://localhost:11093" + "/getmeta")
			if err != nil {
				fmt.Println(err.Error())
			}
			defer response.Body.Close()
			err = json.NewDecoder(response.Body).Decode(&client)
			if err != nil {
				fmt.Println(err.Error())
			}
		} else if *numFlag == 4 {
			response, err := http.Get("http://localhost:11094" + "/getmeta")
			if err != nil {
				fmt.Println(err.Error())
			}
			defer response.Body.Close()
			err = json.NewDecoder(response.Body).Decode(&client)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
	if *putFlag != "unknown" {
		fmt.Println("Request put file " + *putFlag)
		client.PutFile(*putFlag)
	}
	if *getFlag != "unknown" {
		fmt.Println("Request get file " + *getFlag)
		client.GetFile(*getFlag)
	}
	if *deleteFlag != "unknown" {
		fmt.Println("Request delete file " + *deleteFlag)
		client.DeleteFile(*deleteFlag)
	}
	if *taskFlag != 0 {
		fmt.Println("Do task " + strconv.Itoa(*taskFlag))
		response, err := http.Get("http://localhost:11090" + "/getmeta")
		if err != nil {
			fmt.Println(err.Error())
		}
		defer response.Body.Close()
		err = json.NewDecoder(response.Body).Decode(&master)
		if err != nil {
			fmt.Println(err.Error())
		}
		master.DoTask(*taskFlag)
	}
}
