package dfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
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
	content, err := ioutil.ReadAll(result.Body)
	if err != nil {
		fmt.Println("Client post error", err.Error())
		return
	}
	fmt.Println("Response: ", string(content))
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
