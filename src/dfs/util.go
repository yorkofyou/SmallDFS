package dfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

func readByBytes(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Open file error", err.Error())
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("Read file error", err.Error())
	}
	return data
}

func write(filename string, data []byte) {
	err := ioutil.WriteFile(filename, data, 0666)
	if err != nil {
		fmt.Println("Write error", err.Error())
	}
}

func store(filename string, path string) (chunkLen int, offset int) {
	data := readByBytes(filename)
	i := 0
	for i < len(data)/SplitUnit {
		write(path+strconv.Itoa(i), data[i*SplitUnit:(i+1)*SplitUnit])
		i++
	}
	write(path+strconv.Itoa(i), data[i*SplitUnit:len(data)])
	return i + 1, len(data) - i*SplitUnit
}

func moveFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}

func putChunk(path string, chunkNum int, replicaLocationList []ReplicaLocation) {
	for i := 0; i < Redundance; i++ {
		fmt.Printf("Put Chunk %d to DFS Client %s at Replica %d.\n", chunkNum, replicaLocationList[i].Location, replicaLocationList[i].ReplicaNum)
		buffer := new(bytes.Buffer)
		writer := multipart.NewWriter(buffer)
		formFile, err := writer.CreateFormFile("putchunk", path)
		if err != nil {
			fmt.Println(err.Error())
		}
		file, err := os.Open(path)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer file.Close()
		_, err = io.Copy(formFile, file)
		if err != nil {
			fmt.Println(err.Error())
		}
		params := map[string]string{
			"ReplicaNum": strconv.Itoa(replicaLocationList[i].ReplicaNum),
		}
		for key, value := range params {
			err = writer.WriteField(key, value)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
		contentType := writer.FormDataContentType()
		writer.Close()
		result, err := http.Post(replicaLocationList[i].Location+"/putchunk", contentType, buffer)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer result.Body.Close()
		response, err := ioutil.ReadAll(result.Body)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("Client response: ", string(response))
	}
}

func pathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func createFile(filename string) (file *os.File) {
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	return file
}

func deleteFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
