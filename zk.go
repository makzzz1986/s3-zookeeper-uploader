package s3_zookeeper_uploader

import (
	"crypto/md5"
	"encoding/hex"
	"net/url"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

func ZkConnection(host string) (*zk.Conn, error) {
	// Need to add host check
	log.Infof("Connecting to %s\n", host)
	conn, _, err := zk.Connect([]string{host}, time.Second) //*10)
	return conn, err
}

func ZkList(conn *zk.Conn, path string) ([]string, error) {
	children, _, err := conn.Children(path)
	return children, err
}

func ZkTree(conn *zk.Conn, path string) ([]string, error) {
	log.Debugf("Getting childrens of %s\n", path)
	var files []string
	childrens, stat, err := conn.Children(path)
	if err != nil {
		return files, err
	}
	if stat.NumChildren == 0 {
		log.Debugf("Adding a file %v\n", path)
		files = append(files, path)
	} else {
		log.Debugf("Finding more childrens of %s, getting deeper!\n", path)
		for _, children := range childrens {
			newPath, err := url.JoinPath(path, children)
			if err != nil {
				return files, err
			}
			newChildrens, err := ZkTree(conn, newPath)
			if err != nil {
				return files, err
			}
			files = append(files, newChildrens...)
		}
	}
	return files, err
}

func ZkGet(conn *zk.Conn, path string) ([]byte, error) {
	log.Infof("Getting file from %s\n", path)
	// Need to add host and path check
	data, _, err := conn.Get(path)
	if err != nil {
		return nil, err
	}
	// log.Debugf("Stats: %v", stat)
	return data, err
}

func ZkHash(conn *zk.Conn, path string) (string, error) {
	log.Debugf("Getting MD5 hash of znode %s\n", path)
	exists, _, _ := ZkExists(conn, path)
	if !exists {
		log.Infof("File on %s does not exist\n", path)
		return "", nil
	} else {
		log.Debugf("File on %s does exist\n", path)
	}
	data, err := ZkGet(conn, path)
	if err != nil {
		return "", err
	}

	log.Debugf("The data of %s is taken, its length is %d\n", path, len(data))
	hash := ZkGetHash(data)
	return hash, nil
}

func hashesEqual(first string, second string) bool {
	first = strings.TrimRight(first, "\r\n")
	first = strings.TrimRight(first, "\n")
	second = strings.TrimRight(second, "\r\n")
	second = strings.TrimRight(second, "\n")
	log.Debugf("Comparing hashes [%s] and [%s]", first, second)
	return strings.Compare(first, second) == 0
}

func ZkZnodesToUpdate(conn *zk.Conn, s3Folder S3Folder) (S3Folder, error) {
	log.Infoln("Checking if the files need to be updated on Zookeeper")
	var updatedObjects []S3Object
	for _, object := range s3Folder.Objects {
		log.Debugf("Checking if the file %s exists on Zookeeper on path %s and what its MD5 hash", object.Key, object.FilePath)
		hash, err := ZkHash(conn, object.FilePath)
		if err != nil {
			return s3Folder, err
		} else {
			object.Checked = true
			if hashesEqual(hash, object.MD5) {
				object.ToUpdate = false
				object.Synced = true
				log.Debugf("The file %s exists on Zookeper on path %s and MD5 hashes are equal", object.Key, object.FilePath)
			} else {
				object.ToUpdate = true
				object.Synced = false
				log.Warnf("The file %s should be uploaded to Zookeper as znode %s", object.Key, object.FilePath)
			}
			updatedObjects = append(updatedObjects, object)
		}
	}
	s3Folder.Objects = updatedObjects
	return s3Folder, nil
}

func ZkHashesByPaths(conn *zk.Conn, paths []string) ([]string, error) {
	log.Debugf("Getting MD5 hashes of znode %s\n", strings.Join(paths, ", "))

	var hashes []string
	for _, znode := range paths {
		hash, err := ZkHash(conn, znode)
		if err != nil {
			return hashes, err
		} else {
			hashes = append(hashes, hash)
		}
	}
	return hashes, nil
}

func ZkGetHash(data []byte) string {
	log.Debugf("Getting MD5 hash of data with len: %d\n", len(data))
	hash := md5.Sum(data)
	hex := hex.EncodeToString(hash[:])
	return hex
}

func ZkUpload(conn *zk.Conn, path string, data []byte) (string, error) {
	log.Debugf("Uploading file to %s\n", path)
	// Need to add host and path check
	exists, stat, err := ZkExists(conn, path)
	if err != nil {
		return "", err
	}
	if exists {
		log.Warnf("File on %s exists, updating\n", path)
		updated, err := ZkUpdate(conn, path, data, stat.Version)
		return updated, err
	} else {
		log.Warnf("File on %s does not exist, creating znode\n", path)
		createFolders := ZkCreateFolderTree(conn, path)
		if createFolders == nil {
			updated, err := ZkCreate(conn, path, data)
			return updated, err
		} else {
			return "", err
		}
	}
	// return "", nil
}

// Creating a path of empty znodes if needed, for example:
// for path "/myfolder/mysubfolder/myfile"
// it creates folders:
// /myfolder
// /myfolder/mysubfolder
func ZkCreateFolderTree(conn *zk.Conn, path string) error {
	log.Infof("Create a folder tree for %s if needed\n", path)
	path = strings.TrimPrefix(path, "/")
	var folderTree []string
	folderTree = append(folderTree, "")
	for i, subfolder := range strings.Split(path, "/") {
		subfolderTree := folderTree[i] + "/" + subfolder
		// Checking if we haven't reached the last element, the file itself
		if subfolderTree[1:] == path {
			log.Debugf("We reached the file, exiting of folders creating")
			break
		}
		folderTree = append(folderTree, subfolderTree)
		log.Debugf("Checking if %s folder exists\n", subfolderTree)
		exists, _, err := ZkExists(conn, subfolderTree)
		if err != nil {
			return err
		} else {
			if !exists {
				_, err := ZkCreate(conn, subfolderTree, []byte{})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ZkExists(conn *zk.Conn, path string) (bool, *zk.Stat, error) {
	// Need to add host and path check
	log.Debugf("Check if znode %s exists\n", path)

	exists, stat, _ := conn.Exists(path)
	log.Debugf("Exists: %v", exists)
	return exists, stat, nil
}

func ZkUpdate(conn *zk.Conn, path string, data []byte, version int32) (string, error) {
	log.Warnf("Updating a new file to %s, with length %d and version %d\n", path, len(data), version)
	_, err := conn.Set(path, data, version)
	return path, err
}

func ZkCreate(conn *zk.Conn, path string, data []byte) (string, error) {
	log.Warnf("Creating a new file to %s, with length %d\n", path, len(data))
	newPath, err := conn.Create(path, data, 0, ZkPublicACL())
	return newPath, err
}

func ZkPublicACL() []zk.ACL {
	var publicAclArray []zk.ACL
	publicAcl := zk.ACL{Perms: 31, Scheme: "world", ID: "anyone"}
	publicAclArray = append(publicAclArray, publicAcl)
	return publicAclArray
}

func ZkGetAcl(conn *zk.Conn, path string) {
	// Need to add host and path check
	log.Debugf("Getting ACL of %s\n", path)

	acl, _, err := conn.GetACL(path)
	if err != nil {
		panic(err)
	}
	log.Debugln(acl)
}
