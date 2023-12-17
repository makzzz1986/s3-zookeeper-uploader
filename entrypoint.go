package s3_zookeeper_uploader

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

func SyncObjects(s3Conn *s3.Client, zkConn *zk.Conn, s3Folder S3Folder) (S3Folder, error) {
	log.Infof("Replication the folder s3://%s/%s to Zookeeper", s3Folder.BucketName, s3Folder.FolderName)
	var updatedObjects []S3Object
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
	data, err := S3GetObject(s3Conn, bucket, s3key)
	if err != nil {
		return false, err
	} else {
		_, err := ZkUpload(zkConn, zkznode, data)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func NeedToSync(zkConn *zk.Conn, s3Folder S3Folder) (bool, S3Folder, error) {
	toUpdate := false
	comparison, err := ZkZnodesToUpdate(zkConn, s3Folder)
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
