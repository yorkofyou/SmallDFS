package dfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

func (master *Master) Reset() {
	err := os.RemoveAll(master.Node.Directory + "/")
	if err != nil {
		fmt.Println(err.Error())
	}
	err = os.MkdirAll(master.Node.Directory, 0777)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (master *Master) Init() {
	location := "http://localhost:11090"
	temp := strings.Split(location, ":")
	result, err := strconv.Atoi(temp[2])
	if err != nil {
		fmt.Println(err.Error())
	}
	master.Node.Namespace = Namespace{}
	master.Node.Port = result
	master.Node.Directory = "SmallDFS/Master"
	master.Node.Location = location
	master.Node.StorageSum = 100000
	master.Node.StorageAvail = 100000
	master.Node.ChunkAvail = append(master.Node.ChunkAvail, 0)
	for i := 1; i < master.Node.StorageAvail; i++ {
		master.Node.ChunkAvail = append(master.Node.ChunkAvail, 100000-i)
	}
	for num := 0; num < master.Node.StorageSum; num++ {
		createFile(master.Node.Directory + "/chunk-" + strconv.Itoa(num))
	}
	master.Node.LastEdit = time.Now().Unix()
	master.Redundance = 2
	master.MapFinished = make([]bool, ClientNum)
	master.ReduceFinished = make([]bool, ClientNum)
	master.GetClients()
	master.server()
}

func (m *Master) ReadyForReduce(Id int, _ *bool) error {
	m.MapAlready = false
	m.ReduceAlready = false
	m.MapFinished[Id-1] = false
	m.ReduceFinished[Id-1] = false
	return nil
}

func (m *Master) MapFinish(Id int, ready *bool) error {
	if m.MapAlready == true {
		*ready = true
		return nil
	}
	m.MapFinished[Id-1] = true
	for i := 0; i < ClientNum; i++ {
		if m.MapFinished[i] == false {
			return nil
		}
	}
	m.MapAlready = true
	return nil
}

func (m *Master) ReduceFinish(Id int, ready *bool) error {
	if m.ReduceAlready == true {
		*ready = true
		return nil
	}
	m.ReduceFinished[Id-1] = true
	for i := 0; i < ClientNum; i++ {
		if m.MapFinished[i] == false {
			return nil
		}
	}
	m.ReduceAlready = true
	return nil
}

func (m *Master) AskForFiles(Id int, Files *[]string) error {
	filename := "tb_call_201202_random.txt"
	filename = strings.Split(filename, ".")[0]
	file := m.Node.Namespace[filename]
	if file.Info == "" {
		err := errors.New("no such file or directory")
		return err
	}
	for i := 0; i < len(file.Chunks); i++ {
		if file.Chunks[i].Replicas[0].Location == "http://localhost:1109"+strconv.Itoa(Id) {
			*Files = append(*Files, "chunk-"+strconv.Itoa(file.Chunks[i].Replicas[0].ReplicaNum))
		}
	}
	return nil
}

func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (master *Master) GetClients() {
	locations := []string{"http://localhost:11091", "http://localhost:11092", "http://localhost:11093", "http://localhost:11094"}
	for i := 0; i < ClientNum; i++ {
		response, err := http.Get(locations[i] + "/getmeta")
		if err != nil {
			fmt.Println(err.Error())
		}
		defer response.Body.Close()
		var client Client
		err = json.NewDecoder(response.Body).Decode(&client)
		if err != nil {
			fmt.Println(err.Error())
		}
		master.Clients = append(master.Clients, client)
	}
}

func (master *Master) Run() {
	router := gin.Default()

	router.POST("/put", func(c *gin.Context) {
		file := master.ReceiveFrom(c)
		filename := strings.Split(file, ".")[0]
		exist, err := pathExist(master.Node.Directory + "/" + filename)
		if err != nil {
			fmt.Println("Master error when getting directory", err.Error())
		}
		if !exist {
			err = os.MkdirAll(master.Node.Directory+"/"+filename, os.ModePerm)
			if err != nil {
				fmt.Println("Master error when making directory", err.Error())
			}
		}
		chunkLen, offset, size := store(master.Node.Directory+"/"+file, master.Node.Directory+"/"+filename+"/chunk-")
		f := File{Info: "{name:" + filename + "}"}
		f.Chunks = make([]Chunk, chunkLen)
		for i := 0; i < chunkLen; i++ {
			replicaLocationList := master.Allocate()
			f.Chunks[i].Replicas = replicaLocationList
			PutChunk(master.Node.Directory+"/"+filename+"/chunk-"+strconv.Itoa(i), i, replicaLocationList)
		}
		f.Offset = offset
		f.Size = size
		master.mu.Lock()
		master.Node.Namespace[filename] = f
		master.mu.Unlock()
		fmt.Printf("File %s generated: %d chunks with last chunk offset %d\n", filename, chunkLen, offset)
		fmt.Println("File info: ", f.Info)
		err = os.Remove(master.Node.Directory + "/" + file)
		if err != nil {
			fmt.Println("Master error when removing temporary files", err.Error())
		}
		for i := 0; i < chunkLen; i++ {
			err = os.Remove(master.Node.Directory + "/" + filename + "/chunk-" + strconv.Itoa(i))
			if err != nil {
				fmt.Println("Master error when removing chunk", i, err.Error())
			}
		}
	})

	router.GET("/get/:filename", func(c *gin.Context) {
		filename := c.Param("filename")
		filename = strings.Split(filename, ".")[0]
		master.mu.Lock()
		file := master.Node.Namespace[filename]
		master.mu.Unlock()
		if file.Info == "" {
			c.String(404, "no such file or directory")
		}
		for i := 0; i < len(file.Chunks); i++ {
			master.GetChunk(file, filename, i)
		}
		data := master.Merge(file, filename)
		c.String(http.StatusOK, string(data))
	})

	router.DELETE("/delete/:filename", func(c *gin.Context) {
		filename := c.Param("filename")
		file := master.Node.Namespace[filename]
		for i := 0; i < len(file.Chunks); i++ {
			master.DelChunk(file, filename, i)
		}
		c.String(http.StatusOK, "Delete "+filename+"success\n")
	})

	router.POST("/putchunk", func(c *gin.Context) {
		ReplicaNum := c.PostForm("ReplicaNum")
		file, header, err := c.Request.FormFile("putchunk")
		if err != nil {
			c.String(http.StatusBadRequest, "Bad request")
			return
		}
		filename := header.Filename
		fmt.Println(file, err, filename)
		chunkout, err := os.Create(master.Node.Directory + "/chunk-" + ReplicaNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer chunkout.Close()
		io.Copy(chunkout, file)
		chunkdata := readByBytes(master.Node.Directory + "/chunk-" + ReplicaNum)
		hash := sha256.New()
		hash.Write(chunkdata)
		hashStr := hex.EncodeToString(hash.Sum(nil))
		fmt.Println("Chunk hash", ReplicaNum, ": %s", hashStr)
		write(master.Node.Directory+"/chunkhashs/chunkhash-"+ReplicaNum, []byte(hashStr))
		n := master.Node.StorageAvail
		master.Node.ChunkAvail[0] = master.Node.ChunkAvail[n-1]
		master.Node.ChunkAvail = master.Node.ChunkAvail[0 : n-1]
		master.Node.StorageAvail--
		c.String(http.StatusCreated, "Put chunk success\n")
	})

	router.GET("/getchunk/:chunknum", func(c *gin.Context) {
		chunkNum := c.Param("chunknum")
		num, err := strconv.Atoi(chunkNum)
		if err != nil {
			fmt.Println("Master get chunk number error", err.Error())
		}
		data := readByBytes(master.Node.Directory + "/chunk-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(data))
	})

	router.DELETE("/delchunk/:chunknum", func(c *gin.Context) {
		chunkNum := c.Param("chunknum")
		num, err := strconv.Atoi(chunkNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		deleteFile(master.Node.Directory + "/chunk-" + strconv.Itoa(num))
		createFile(master.Node.Directory + "/chunk-" + strconv.Itoa(num))
		deleteFile(master.Node.Directory + "/chunkhashs/chunkhash-" + strconv.Itoa(num))
		c.String(http.StatusOK, "Delete chunk-"+strconv.Itoa(num)+" success")
	})

	router.GET("/getchunkhash/:chunknum", func(c *gin.Context) {
		chunkNum := c.Param("chunknum")
		num, err := strconv.Atoi(chunkNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		data := readByBytes(master.Node.Directory + "/chunkhashs/chunkhash-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(data))
	})

	router.GET("/getmeta", func(c *gin.Context) {
		c.JSON(http.StatusOK, master)
	})

	router.Run(":" + strconv.Itoa(master.Node.Port))
}

func (master *Master) ReceiveFrom(c *gin.Context) string {
	file, header, err := c.Request.FormFile("put")
	if err != nil {
		c.String(http.StatusBadRequest, "Bad request")
		fmt.Println(err.Error())
		return "null"
	}
	filename := header.Filename
	fmt.Println(file, err, filename)
	out, err := os.Create(master.Node.Directory + "/" + filename)
	defer out.Close()
	if err != nil {
		c.String(http.StatusBadRequest, "Master create file error")
		return "null"
	}
	io.Copy(out, file)
	c.String(http.StatusCreated, "Put file to master success\n")
	return filename
}

func (master *Master) Allocate() (replicas []ReplicaLocation) {
	replicaLocationList := make([]ReplicaLocation, Redundance)
	max := make([]int, Redundance)
	for i := 0; i < Redundance; i++ {
		for j := 0; j < ClientNum; j++ {
			if master.Clients[j].Node.StorageAvail > master.Clients[max[i]].Node.StorageAvail {
				max[i] = j
			}
		}
		replicaLocationList[i].Location = master.Clients[max[i]].Node.Location
		replicaLocationList[i].ReplicaNum = master.Clients[max[i]].Node.ChunkAvail[0]
		n := master.Clients[max[i]].Node.StorageAvail
		master.Clients[max[i]].Node.ChunkAvail[0] = master.Clients[max[i]].Node.ChunkAvail[n-1]
		master.Clients[max[i]].Node.ChunkAvail = master.Clients[max[i]].Node.ChunkAvail[0 : n-1]
		master.Clients[max[i]].Node.StorageAvail--
	}
	return replicaLocationList
}

func (master *Master) Merge(file File, filename string) []byte {
	fileData := make([][]byte, len(file.Chunks))
	for i := 0; i < len(file.Chunks); i++ {
		d := readByBytes(master.Node.Directory + "/" + filename + "/chunk-" + strconv.Itoa(i))
		fileData[i] = make([]byte, SplitUnit)
		fileData[i] = d
	}
	data := bytes.Join(fileData, nil)
	return data
}

func (master *Master) GetChunk(file File, filename string, num int) {
	fmt.Println("Get ", filename, "chunk-", num)
	for i := 0; i < Redundance; i++ {
		replicaLocation := file.Chunks[num].Replicas[i].Location
		replicaNum := file.Chunks[num].Replicas[i].ReplicaNum
		url := replicaLocation + "/getchunk/" + strconv.Itoa(replicaNum)
		result, err := http.Get(url)
		if err != nil {
			fmt.Println("Master get chunk error", err.Error())
			return
		}
		defer result.Body.Close()
		chunks, err := ioutil.ReadAll(result.Body)
		if err != nil {
			fmt.Println("Master read chunk error", err.Error())
			return
		}
		write(master.Node.Directory+"/"+filename+"/chunk-"+strconv.Itoa(num), chunks)
		hashRes, err := http.Get(replicaLocation + "/getchunkhash/" + strconv.Itoa(replicaNum))
		if err != nil {
			fmt.Println("Master get chunk hash error", err.Error())
			return
		}
		defer hashRes.Body.Close()
		chunkHash, err := ioutil.ReadAll(hashRes.Body)
		if err != nil {
			fmt.Println("Master read chunk hash error", err.Error())
			return
		}
		hash := sha256.New()
		hash.Write(chunks)
		hashStr := hex.EncodeToString(hash.Sum(nil))
		if hashStr == string(chunkHash) {
			break
		} else {
			fmt.Println("Chunk-", num, "hash wrong, continue to request another replica...")
			continue
		}
	}
}

func (master *Master) DelChunk(file File, filename string, num int) {
	for i := 0; i < Redundance; i++ {
		chunkLocation := file.Chunks[num].Replicas[i].Location
		chunkNum := file.Chunks[num].Replicas[i].ReplicaNum
		url := chunkLocation + "/delchunk/" + strconv.Itoa(chunkNum)
		c := &http.Client{}
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		response, err := c.Do(req)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		defer response.Body.Close()
		delRes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println("Delete chunk-", num, "replica-", i, ": ", string(delRes))
	}
}

func (master *Master) DoTask(Id int) {
	if Id < 1 || Id > 3 {
		return
	}
	var wg sync.WaitGroup
	if Id == 1 {
		master.MapAlready = false
		master.ReduceAlready = false
		for i := 0; i < ClientNum; i++ {
			wg.Add(1)
			url := master.Clients[i].Node.Location + "/work?" + "id=" + strconv.Itoa(Id) + "&option=" + strconv.Itoa(0)
			go sendRequest(url, &wg)
		}
		wg.Wait()
		for i := 0; i < ClientNum; i++ {
			master.MapFinished[i] = false
			master.ReduceFinished[i] = false
		}
		url := master.Clients[0].Node.Location + "/task?" + "id=" + strconv.Itoa(Id) + "&option=" + strconv.Itoa(0)
		response, err := http.Get(url)
		if err != nil {
			fmt.Println("Master send request error", err.Error())
		}
		defer response.Body.Close()
		content, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println("Client post error", err.Error())
			return
		}
		fmt.Println("Response: ", string(content))
		fmt.Println("Clients finish task" + strconv.Itoa(Id))
	} else if Id == 2 {
		for i := 1; i <= 3; i++ {
			master.MapAlready = false
			master.ReduceAlready = false
			for j := 0; j < ClientNum; j++ {
				wg.Add(1)
				url := master.Clients[j].Node.Location + "/work?" + "id=" + strconv.Itoa(Id) + "&option=" + strconv.Itoa(i)
				go sendRequest(url, &wg)
			}
			wg.Wait()
			for j := 0; j < ClientNum; j++ {
				master.MapFinished[j] = false
				master.ReduceFinished[j] = false
			}
			url := master.Clients[0].Node.Location + "/task?" + "id=" + strconv.Itoa(Id) + "&option=" + strconv.Itoa(i)
			response, err := http.Get(url)
			if err != nil {
				fmt.Println("Master send request error", err.Error())
			}
			defer response.Body.Close()
			content, err := ioutil.ReadAll(response.Body)
			if err != nil {
				fmt.Println("Client post error", err.Error())
				return
			}
			fmt.Println("Response: ", string(content))
		}
		fmt.Println("Clients finish task" + strconv.Itoa(Id))
	} else if Id == 3 {
		master.MapAlready = false
		master.ReduceAlready = false
		for i := 0; i < ClientNum; i++ {
			wg.Add(1)
			url := master.Clients[i].Node.Location + "/work?" + "id=" + strconv.Itoa(Id) + "&option=" + strconv.Itoa(0)
			go sendRequest(url, &wg)
		}
		wg.Wait()
		for i := 0; i < ClientNum; i++ {
			master.MapFinished[i] = false
			master.ReduceFinished[i] = false
		}
		url := master.Clients[0].Node.Location + "/task?" + "id=" + strconv.Itoa(Id) + "&option=" + strconv.Itoa(0)
		response, err := http.Get(url)
		if err != nil {
			fmt.Println("Master send request error", err.Error())
		}
		defer response.Body.Close()
		content, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println("Client post error", err.Error())
			return
		}
		fmt.Println("Response: ", string(content))
		fmt.Println("Clients finish task" + strconv.Itoa(Id))
	}
}

func sendRequest(url string, wg *sync.WaitGroup) {
	defer wg.Done()
	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Master send request error", err.Error())
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Client post error", err.Error())
		return
	}
	fmt.Println("Response: ", string(content))
}
