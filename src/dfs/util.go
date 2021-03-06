package dfs

import (
	"bufio"
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
	"strings"
)

func readByBytes(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Open file error", err.Error())
	}
	// var data []byte
	// scanner := bufio.NewScanner(file)
	// for scanner.Scan() {
	// 	data = append(data, scanner.Bytes()...)
	// }
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

func store(filename string, path string) (chunkLen int, offset int, fileSize int) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Open file error", err.Error())
	}
	var fileLen int
	scanner := bufio.NewScanner(file)
	flag := true
	i := 0
	size := 0
	for flag == true {
		var data []byte
		fileLen = 0
		for fileLen < SplitUnit {
			if scanner.Scan() == false {
				flag = false
				break
			}
			bytes := scanner.Bytes()
			newLine := []byte("\n")
			bytes = append(bytes, newLine...)
			fileLen += len(bytes)
			data = append(data, bytes...)

		}
		write(path+strconv.Itoa(i), data)
		size += fileLen
		i++
		if flag == false {
			break
		}
	}
	return i, fileLen, size
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

func PutChunk(path string, chunkNum int, replicaLocationList []ReplicaLocation) {
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

func Map(filename string, contents string, id int, option int) []KeyValue {
	// function to detect word separators.
	// ff := func(r rune) bool { return !unicode.IsDigit(r) }

	// split contents into an array of words.
	// words := strings.FieldsFunc(contents, ff)
	words := strings.Split(contents, "\n")
	kva := []KeyValue{}
	if id == 1 {
		for _, w := range words {
			number := strings.Fields(w)
			if len(number) == 14 {
				kv := KeyValue{number[1], number[0]}
				kva = append(kva, kv)
			}
		}
	} else if id == 2 {
		for _, w := range words {
			number := strings.Fields(w)
			if len(number) == 14 {
				t, err := strconv.Atoi(number[12])
				if err != nil {
					continue
				}
				if t == option {
					kv := KeyValue{number[4], "1"}
					kva = append(kva, kv)
				}
			}
		}
	} else if id == 3 {
		for _, w := range words {
			number := strings.Fields(w)
			if len(number) == 14 {
				kv := KeyValue{number[1], number[9] + " " + number[11]}
				kva = append(kva, kv)
			}
		}
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func Reduce(key string, values []string, id int) string {
	// return the number of occurrences of this word.
	if id == 1 {
		s := map[string]bool{}
		count := 0
		for i := 0; i < len(values); i++ {
			if _, ok := s[values[i]]; !ok {
				count++
				s[values[i]] = true
			}
		}
		return strconv.Itoa(len(values)) + " " + strconv.Itoa(count)
	} else if id == 3 {
		ret := make([]int, 8)
		for i := 0; i < len(values); i++ {
			time, _ := strconv.Atoi(strings.Split(strings.Fields(values[i])[0], ":")[0])
			duration, _ := strconv.Atoi(strings.Fields(values[i])[1])
			ret[time/3] += duration
		}
		return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ret)), " "), "[]")
	}
	return strconv.Itoa(len(values))
}
