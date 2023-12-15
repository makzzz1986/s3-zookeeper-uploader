package main

import (
	"fmt"
	"os"

	"github.com/makzzz1986/s3-zookeeper-uploader/cmd"
)

var (
	zkHost string
)

func init() {
	zkHost = os.Getenv("ZK_HOST")
	if zkHost == "" {
		zkHost = "127.0.0.1"
	}
}

func main() {
	fmt.Println("The app has been started")

	// conn, err := cmd.Connection(zkHost)
	// if err != nil {
	// 	panic(err)
	// }

	// cmd.List(conn, "/")
	// data, err := cmd.Get(conn, "/security.json")
	// // data, err := cmd.Get(conn, "/aliases.json")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(string(data[:]))

	// data, _ = cmd.Get(conn, "/autoscaling")
	// fmt.Println(string(data[:]))

	// treePath := "/overseer_elect"
	// tree, err := cmd.Tree(conn, treePath)
	// if err != nil {
	// 	panic(err)
	// } else {
	// 	fmt.Printf("\nPrinting file tree of %s\n", treePath)
	// 	for _, file := range tree {
	// 		fmt.Println(file)
	// 	}
	// }

	// data, err := os.ReadFile("notes/testfile.txt") // just pass the file name
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	cmd.GetHash(data)
	// }

	// uploaded, err := cmd.Upload(conn, "/tmp", []byte{})
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println(uploaded)
	// }

	// uploaded, err := cmd.Upload(conn, "/tmp/more/testfile.txt", data)
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	fmt.Println(uploaded)
	// }

	// hash, _ := cmd.Hash(conn, "/tmp/more/testfile.txt")
	// fmt.Println(hash)
	result, err := cmd.GetS3ListObjects("solr-updater-2", "TAG2/")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(result)
	}
}
