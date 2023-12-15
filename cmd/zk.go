package cmd

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

func ZkConnection(host string) (*zk.Conn, error) {
	// Need to add host check
	log.Infof("Connecting to %s\n", host)
	c, _, err := zk.Connect([]string{host}, time.Second) //*10)
	return c, err
}

func List(c *zk.Conn, path string) {
	children, stat, err := c.Children(path)
	if err != nil {
		panic(err)
	}
	log.Infof("%+v %+v\n", children, stat)
}

func Tree(c *zk.Conn, path string) ([]string, error) {
	log.Debugf("Getting childrens of %s\n", path)
	var files []string
	childrens, stat, err := c.Children(path)
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
			newChildrens, err := Tree(c, newPath)
			if err != nil {
				return files, err
			}
			files = append(files, newChildrens...)
		}
	}
	return files, err
}

func Get(c *zk.Conn, path string) ([]byte, error) {
	log.Infof("Getting file from %s\n", path)
	// Need to add host and path check
	exists, err := Exists(c, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Infof("File on %s does not exist", path)
		return nil, errors.New("the file is not found")
	}
	data, stat, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	log.Debugf("Stats: %v", stat)
	return data, err
}

func Hash(c *zk.Conn, path string) (string, error) {
	log.Debugf("Getting MD5 hash of znode %s\n", path)
	data, err := Get(c, path)
	if err != nil {
		return "", err
	}

	log.Debugf("The data of %s is taken, its length is %d\n", path, len(data))
	hash := GetHash(data)
	return hash, nil
}

func GetHash(data []byte) string {
	log.Debugf("Getting MD5 hash of data with len: %d\n", len(data))
	hash := md5.Sum(data)
	hex := hex.EncodeToString(hash[:])
	return hex
}

func Upload(c *zk.Conn, path string, data []byte) (string, error) {
	log.Debugf("Uploading file to %s\n", path)
	// Need to add host and path check
	exists, err := Exists(c, path)
	if err != nil {
		return "", err
	}
	if exists {
		log.Warnf("File on %s exists, updating\n", path)
		updated, err := Update(c, path, data)
		return updated, err
	} else {
		log.Warnf("File on %s does not exist, creating znode\n", path)
		createFolders := CreateFolderTree(c, path)
		if createFolders == nil {
			updated, err := Create(c, path, data)
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
func CreateFolderTree(c *zk.Conn, path string) error {
	log.Infof("Create a folder tree for %s if needed\n", path)
	path = strings.TrimPrefix(path, "/")
	var folderTree []string
	folderTree = append(folderTree, "")
	for i, subfolder := range strings.Split(path, "/") {
		subfolderTree := folderTree[i] + "/" + subfolder
		// Checking if we haven't reached the last element, the file itself
		if subfolderTree[1:] == path {
			log.Debugf("We reached the file, exiting")
			break
		}
		folderTree = append(folderTree, subfolderTree)
		log.Debugf("Checking if %s folder exists\n", subfolderTree)
		exists, err := Exists(c, subfolderTree)
		if err != nil {
			return err
		} else {
			if !exists {
				_, err := Create(c, subfolderTree, []byte{})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func Exists(c *zk.Conn, path string) (bool, error) {
	// Need to add host and path check
	log.Debugf("Check if %s exists\n", path)

	exists, _, err := c.Exists(path)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func Update(c *zk.Conn, path string, data []byte) (string, error) {
	// Need to add host and path check
	log.Warnf("Updating a new file to %s, with length %d\n", path, len(data))
	_, err := c.Set(path, data, 0)
	return path, err
}

func Create(c *zk.Conn, path string, data []byte) (string, error) {
	// Need to add host and path check
	log.Warnf("Creating a new file to %s, with length %d\n", path, len(data))
	newPath, err := c.Create(path, data, 0, PublicACL())
	return newPath, err
}

func PublicACL() []zk.ACL {
	var publicAclArray []zk.ACL
	publicAcl := zk.ACL{Perms: 31, Scheme: "world", ID: "anyone"}
	publicAclArray = append(publicAclArray, publicAcl)
	return publicAclArray
}

func GetAcl(c *zk.Conn, path string) {
	// Need to add host and path check
	log.Debugf("Getting ACL of %s\n", path)

	acl, _, err := c.GetACL(path)
	if err != nil {
		panic(err)
	}
	log.Debugln(acl)
}
