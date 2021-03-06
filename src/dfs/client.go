package dfs

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (client *Client) Reset(num int) {
	for j := 0; j < client.Node.StorageSum; j++ {
		deleteFile("SmallDFS/Client" + strconv.Itoa(num) + "/chunk-" + strconv.Itoa(j))
		createFile("SmallDFS/Client" + strconv.Itoa(num) + "/chunk-" + strconv.Itoa(j))
	}
	err := os.RemoveAll(client.Node.Directory)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	err = os.MkdirAll(client.Node.Directory+"/", os.ModePerm)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	err = os.RemoveAll(client.Node.Directory + "/chunkhashs")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	err = os.MkdirAll(client.Node.Directory+"/chunkhashs", os.ModePerm)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func (client *Client) Init(num int) {
	location := "http://localhost:1109" + strconv.Itoa(num)
	temp := strings.Split(location, ":")
	result, err := strconv.Atoi(temp[2])
	if err != nil {
		fmt.Println(err.Error())
	}
	client.Node.Namespace = Namespace{}
	client.Node.Port = result
	client.Node.Location = location
	client.Node.StorageSum = 100000
	client.Node.StorageAvail = 100000
	client.Node.ChunkAvail = append(client.Node.ChunkAvail, 0)
	client.MasterLocation = "http://localhost:11090"
	for i := 1; i < client.Node.StorageAvail; i++ {
		client.Node.ChunkAvail = append(client.Node.ChunkAvail, 100000-i)
	}
	for num := 0; num < client.Node.StorageSum; num++ {
		createFile(client.Node.Directory + "/chunk-" + strconv.Itoa(num))
	}
	client.Node.LastEdit = time.Now().Unix()
}

func (client *Client) Run(num int) {
	router := gin.Default()

	router.POST("/putchunk", func(c *gin.Context) {
		ReplicaNum := c.PostForm("ReplicaNum")
		file, header, err := c.Request.FormFile("putchunk")
		if err != nil {
			c.String(http.StatusBadRequest, "Bad request")
		}
		filename := header.Filename
		fmt.Println(file, err, filename)
		checkout, err := os.Create(client.Node.Directory + "/chunk-" + ReplicaNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer checkout.Close()
		io.Copy(checkout, file)
		chunkdata := readByBytes(client.Node.Directory + "/chunk-" + ReplicaNum)
		hash := sha256.New()
		hash.Write(chunkdata)
		hashStr := hex.EncodeToString(hash.Sum(nil))
		fmt.Println("Chunk hash", ReplicaNum, ": %s", hashStr)
		write(client.Node.Directory+"/chunkhashs/chunkhash-"+ReplicaNum, []byte(hashStr))
		n := client.Node.StorageAvail
		client.Node.ChunkAvail[0] = client.Node.ChunkAvail[n-1]
		client.Node.ChunkAvail = client.Node.ChunkAvail[0 : n-1]
		client.Node.StorageAvail--
		c.String(http.StatusCreated, "Put chunk success\n")

	})

	router.GET("/getchunk/:chunknum", func(c *gin.Context) {
		chunkNum := c.Param("chunknum")
		num, err := strconv.Atoi(chunkNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		data := readByBytes(client.Node.Directory + "/chunk-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(data))
	})

	router.DELETE("/delchunk/:chunknum", func(c *gin.Context) {
		chunkNum := c.Param("chunknum")
		num, err := strconv.Atoi(chunkNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		deleteFile(client.Node.Directory + "/chunk-" + strconv.Itoa(num))
		createFile(client.Node.Directory + "/chunk-" + strconv.Itoa(num))
		deleteFile(client.Node.Directory + "/chunkhashs/chunkhash-" + strconv.Itoa(num))
		c.String(http.StatusOK, "Delete chunk-"+strconv.Itoa(num)+" success")
	})

	router.GET("/getchunkhash/:chunknum", func(c *gin.Context) {
		chunkNum := c.Param("chunknum")
		num, err := strconv.Atoi(chunkNum)
		if err != nil {
			fmt.Println(err.Error())
		}
		data := readByBytes(client.Node.Directory + "/chunkhashs/chunkhash-" + strconv.Itoa(num))
		c.String(http.StatusOK, string(data))
	})

	router.GET("/work", func(c *gin.Context) {
		fmt.Println("Client start working")
		id, _ := strconv.Atoi(c.Query("id"))
		option, _ := strconv.Atoi(c.Query("option"))
		files := []string{}
		if len(client.Files) == 0 {
			call("Master.AskForFiles", client.Id, &files)
			client.Files = files
		}
		if id < 1 || id > 3 {
			return
		}
		if id == 1 {
			client.Work(id, 0)
		} else if id == 2 {
			client.Work(id, option)
		} else if id == 3 {
			client.Work(id, 0)
		}
		c.String(http.StatusOK, "Client "+strconv.FormatInt(client.Id, 10)+" finish task "+strconv.Itoa(id))
	})

	router.GET("/task", func(c *gin.Context) {
		id, _ := strconv.Atoi(c.Query("id"))
		option, _ := strconv.Atoi(c.Query("option"))
		kva := ByKey{}
		if id == 1 {
			output := "task-" + strconv.Itoa(id) + ".txt"
			ofile, _ := os.Create(client.Node.Directory + "/" + output)
			for i := 1; i <= ClientNum; i++ {
				filename := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + "-out-" + strconv.Itoa(i) + ".txt"
				if int64(i) != client.Id {
					client.GetFile(filename)
				}
				file, _ := os.Open(client.Node.Directory + "/" + filename)
				var kv KeyValue
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					scanner.Text()
					s := make([]string, 2)
					fmt.Sscanf(scanner.Text(), "%v %v %v", &kv.Key, &s[0], &s[1])
					kv.Value = strings.Join(s[:], " ")
					kva = append(kva, kv)
				}
			}
			for i := 0; i < len(kva); i++ {
				s := strings.Fields(kva[i].Value)
				num, _ := strconv.ParseFloat(s[0], 32)
				count, _ := strconv.ParseFloat(s[1], 32)
				fmt.Fprintf(ofile, "<%v, %v>\n", kva[i].Key, fmt.Sprintf("%f", num/count))
			}
		} else if id == 2 {
			output := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + ".txt"
			ofile, _ := os.Create(client.Node.Directory + "/" + output)
			sum := 0
			for i := 1; i <= ClientNum; i++ {
				filename := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + "-out-" + strconv.Itoa(i) + ".txt"
				if int64(i) != client.Id {
					client.GetFile(filename)
				}
				file, _ := os.Open(client.Node.Directory + "/" + filename)
				var kv KeyValue
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					scanner.Text()
					fmt.Sscanf(scanner.Text(), "%v %v", &kv.Key, &kv.Value)
					v, _ := strconv.Atoi(kv.Value)
					sum += v
					kva = append(kva, kv)
				}
			}
			for i := 0; i < len(kva); i++ {
				num, _ := strconv.ParseFloat(kva[i].Value, 32)
				fmt.Fprintf(ofile, "<%v, %v>\n", kva[i].Key, fmt.Sprintf("%f", num/float64(sum)))
			}
		} else if id == 3 {
			output := "task-" + strconv.Itoa(id) + ".txt"
			// os.Create(client.Node.Directory + "/" + output)
			ofile, _ := os.Create(client.Node.Directory + "/" + output)
			for i := 1; i <= ClientNum; i++ {
				filename := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + "-out-" + strconv.Itoa(i) + ".txt"
				if int64(i) != client.Id {
					client.GetFile(filename)
				}
				file, _ := os.Open(client.Node.Directory + "/" + filename)
				var kv KeyValue
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					scanner.Text()
					s := make([]string, 8)
					fmt.Sscanf(scanner.Text(), "%v %v %v %v %v %v %v %v %v", &kv.Key, &s[0], &s[1], &s[2], &s[3], &s[4], &s[5], &s[6], &s[7])
					kv.Value = strings.Join(s[:], " ")
					kva = append(kva, kv)
				}
			}
			for i := 0; i < len(kva); i++ {
				duration := make([]float64, 8)
				sum := float64(0)
				for j, s := range strings.Fields(kva[i].Value) {
					d, _ := strconv.ParseFloat(s, 32)
					duration[j] = float64(d)
					sum += duration[j]
				}
				for i := range duration {
					duration[i] = duration[i] / sum
				}
				fmt.Fprintf(ofile, "<%v, %v, %v, %v, %v, %v, %v, %v, %v>\n", kva[i].Key, fmt.Sprintf("%f", duration[0]), fmt.Sprintf("%f", duration[1]), fmt.Sprintf("%f", duration[2]), fmt.Sprintf("%f", duration[3]), fmt.Sprintf("%f", duration[4]), fmt.Sprintf("%f", duration[5]), fmt.Sprintf("%f", duration[6]), fmt.Sprintf("%f", duration[7]))
			}
		}
		c.String(http.StatusOK, "Client has merged")
	})

	router.GET("/getmeta", func(c *gin.Context) {
		c.JSON(http.StatusOK, client)
	})

	router.Run(":" + strconv.Itoa(client.Node.Port))
}

func (client *Client) PutFile(path string) {
	fmt.Printf("Start putting %s to master: %s\n", path, client.MasterLocation)
	file, err := os.Open(client.Node.Directory + "/" + path)
	if err != nil {
		fmt.Println("Client open file error", err.Error())
		return
	}
	defer file.Close()
	buffer := new(bytes.Buffer)
	writer := multipart.NewWriter(buffer)
	formFile, err := writer.CreateFormFile("put", path)
	if err != nil {
		fmt.Println("Client create form file error", err.Error())
		return
	}
	_, err = io.Copy(formFile, file)
	if err != nil {
		fmt.Println("Client copy file to formfile error", err.Error())
		return
	}
	contentType := writer.FormDataContentType()
	writer.Close()
	result, err := http.Post(client.MasterLocation+"/put", contentType, buffer)
	if err != nil {
		fmt.Println("Client request http post error", err.Error())
		return
	}
	defer result.Body.Close()
	_, err = ioutil.ReadAll(result.Body)
	// content, err := ioutil.ReadAll(result.Body)
	if err != nil {
		fmt.Println("Client post error", err.Error())
		return
	}
	// fmt.Println("Response: ", string(content))
}

func (client *Client) GetFile(filename string) {
	fmt.Printf("Start getting %s to master: %s\n", filename, client.MasterLocation)
	response, err := http.Get(client.MasterLocation + "/get/" + filename)
	if response.StatusCode == 404 || err != nil {
		fmt.Printf("Master get file error")
		if err != nil {
			fmt.Printf(err.Error())
		}
		fmt.Printf("\n")
		return
	}
	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Client read file error", err.Error())
		return
	}
	err = ioutil.WriteFile(client.Node.Directory+"/"+filename, bytes, 0666)
	if err != nil {
		fmt.Println("Client write ", err.Error())
		return
	}
	fmt.Println("Response: " + string(bytes))
}

func (client *Client) DeleteFile(filename string) {
	c := http.Client{}
	req, err := http.NewRequest("DELETE", client.MasterLocation+"/delete/"+filename, nil)
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
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Print("Master response: ", string(bytes))
}

func (client *Client) Work(id int, option int) {
	var kva ByKey
	intermediate := ByKey{}
	var ready bool
	call("Master.ReadyForReduce", client.Id, &ready)
	for _, filename := range client.Files {
		file, err := os.Open(client.Node.Directory + "/" + filename)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return
		}
		if id == 1 {
			kva = Map(client.Node.Directory+"/"+filename, string(content), id, 0)
		} else if id == 2 {
			kva = Map(client.Node.Directory+"/"+filename, string(content), id, option)
		} else if id == 3 {
			kva = Map(client.Node.Directory+"/"+filename, string(content), id, option)
		}
		intermediate = append(intermediate, kva...)
	}
	if len(intermediate) != 0 {
		sort.Sort(ByKey(intermediate))
		tasks := make([][]KeyValue, ClientNum)
		for _, kv := range intermediate {
			taskNo := ihash(kv.Key) % ClientNum
			tasks[taskNo] = append(tasks[taskNo], kv)
		}
		for idx, task := range tasks {
			fname := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + "-" + strconv.FormatInt(client.Id, 10) + "-" + strconv.Itoa(idx+1) + ".txt"
			file, err := os.Create(client.Node.Directory + "/" + fname)
			if err != nil {
				log.Fatal("error :%v", err)
			}
			enc := json.NewEncoder(file)
			for _, kv := range task {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("error :%v", err)
				}
			}
			file.Close()
			client.PutFile(fname)
		}
	}
	fmt.Println("Map finished")
	ready = false
	for ready == false {
		fmt.Println("Wait for reduce")
		call("Master.MapFinish", client.Id, &ready)
		time.Sleep(1000)
	}
	fmt.Println("Start to reduce")
	kva = []KeyValue{}
	for mId := 1; mId < 5; mId++ {
		filename := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + "-" + strconv.Itoa(mId) + "-" + strconv.FormatInt(client.Id, 10) + ".txt"
		if int64(mId) != client.Id {
			client.GetFile(filename)
		}
		if _, err := os.Stat(client.Node.Directory + "/" + filename); os.IsNotExist(err) {
			continue
		}
		file, err := os.Open(client.Node.Directory + "/" + filename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	oname := "task-" + strconv.Itoa(id) + "-option-" + strconv.Itoa(option) + "-out-" + strconv.FormatInt(client.Id, 10) + ".txt"
	ofile, _ := os.Create(client.Node.Directory + "/" + oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := Reduce(kva[i].Key, values, id)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	client.PutFile(oname)
	fmt.Println("Reduce finished")
	ready = false
	for ready == false {
		call("Master.ReduceFinish", client.Id, &ready)
		time.Sleep(1000)
	}
}
