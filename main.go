package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-zookeeper/zk"
	"github.com/makzzz1986/s3-zookeeper-uploader/cmd"
	log "github.com/sirupsen/logrus"
)

var (
	zkHost        string
	AwsRegionName = "eu-west-1"
)

func init() {
	zkHost = os.Getenv("ZK_HOST")
	if zkHost == "" {
		zkHost = "127.0.0.1"
	}

	log.SetOutput(os.Stdout)
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" || strings.ToLower(logLevel) == "info" {
		log.SetLevel(log.InfoLevel) // default is INFO
	} else if strings.ToLower(logLevel) == "debug" {
		log.SetLevel(log.DebugLevel)
	} else if strings.ToLower(logLevel) == "warn" {
		log.SetLevel(log.WarnLevel)
	} else if strings.ToLower(logLevel) == "error" {
		log.SetLevel(log.ErrorLevel)
	} else if strings.ToLower(logLevel) == "critical" || strings.ToLower(logLevel) == "fatal" {
		log.SetLevel(log.FatalLevel)
	}
}

func main() {
	fmt.Println("The app has been started")

	zkConn, err := cmd.ZkConnection(zkHost)
	if err != nil {
		panic(err)
	}

	s3bucket := "solr-updater-2"
	s3client, err := cmd.S3Connection(AwsRegionName)
	if err != nil {
		panic(err)
	}

	result, err := cmd.S3ListObjects(s3client, s3bucket, "TAG3/")
	if err != nil {
		panic(err)
	} else {
		log.Infoln(result)
		comparison, err := cmd.ZkZnodesToUpdate(zkConn, result)
		if err != nil {
			panic(err)
		} else {
			ok, err := SyncObjects(s3client, zkConn, comparison)
			if err != nil {
				panic(err)
			} else {
				log.Infof("S3 folder replication ended with: %v", ok)
			}
		}
	}
}

func SyncObjects(s3Conn *s3.Client, zkConn *zk.Conn, s3Folder cmd.S3Folder) (cmd.S3Folder, error) {
	log.Infof("Replication the folder s3://%s/%s to Zookeeper", s3Folder.BucketName, s3Folder.FolderName)
	var updatedObjects []cmd.S3Object
	for _, object := range s3Folder.Objects {
		if object.ToUpdate {
			log.Debugf("Uploading the file s3://%s/%s to %s", s3Folder.BucketName, object.Key, object.FilePath)
			synced, err := SyncObject(s3Conn, zkConn, s3Folder.BucketName, object.Key, object.FilePath)
			if err != nil {
				return s3Folder, err
			} else {
				log.Infof("The file s3://%s/%s is synced to %s", s3Folder.BucketName, object.Key, object.FilePath)
				object.Synced = synced
			}
		}
		updatedObjects = append(updatedObjects, object)
	}
	s3Folder.Objects = updatedObjects
	return s3Folder, nil
}

func SyncObject(s3Conn *s3.Client, zkConn *zk.Conn, bucket string, s3key string, zkznode string) (bool, error) {
	log.Infof("Syncing the file from s3//%s/%s to Zookeeper %s", bucket, s3key, zkznode)
	log.Debugf("Downloading %s", s3key)
	data, err := cmd.S3GetObject(s3Conn, bucket, s3key)
	if err != nil {
		return false, err
	} else {
		_, err := cmd.ZkUpload(zkConn, zkznode, data)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func NeedToSync(zkConn *zk.Conn, s3Folder cmd.S3Folder) (bool, cmd.S3Folder, error) {
	toUpdate := false
	comparison, err := cmd.ZkZnodesToUpdate(zkConn, s3Folder)
	if err != nil {
		return true, s3Folder, err
	}
	s3Folder = comparison
	for _, object := range s3Folder.Objects {
		if !object.Synced {
			toUpdate = true
		}
	}
	return toUpdate, s3Folder, nil
}
