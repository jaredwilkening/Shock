package node

import (
	"encoding/json"
	"fmt"
	"github.com/MG-RAST/Shock/shock-server/conf"
	"os"
	"path/filepath"
)

// HasFile determines weither the node file has been set
func (node *Node) HasFile() bool {
	if node.File.Name == "" && node.File.Size == 0 && len(node.File.Checksum) == 0 && node.File.Path == "" {
		return false
	}
	return true
}

// HasFile determines weither the node has a particular index
func (node *Node) HasIndex(index string) bool {
	if index == "size" {
		return true
	} else {
		if node.HasFile() {
			if _, err := os.Stat(node.IndexPath() + "/" + index); err == nil {
				return true
			}
		}
	}
	return false
}

// HasFile determines weither the node linkage has parent
func (node *Node) HasParent() bool {
	for _, linkage := range node.Linkages {
		if linkage.Type == "parent" {
			return true
		}
	}
	return false
}

// Path returns the path to the node in the data directory
func (node *Node) Path() string {
	return getPath(node.Id)
}

func getPath(id string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", conf.Conf["data-path"], id[0:2], id[2:4], id[4:6], id)
}

// Path returns the path to the node in the data directory
func (node *Node) IndexPath() string {
	return getIndexPath(node.Id)
}

func getIndexPath(id string) string {
	return fmt.Sprintf("%s/idx", getPath(id))
}

// FilePath returns the path to the node file in the data
// directory. Note: this is not a reliable method of accessing
// the file content. Use node.FileReader to access content.
func (node *Node) FilePath() string {
	if node.File.Path != "" {
		return node.File.Path
	}
	return getPath(node.Id) + "/" + node.Id + ".data"
}

// FileExt returns the file extension
func (node *Node) FileExt() string {
	if node.File.Name != "" {
		return filepath.Ext(node.File.Name)
	}
	return ""
}

// ToJson marshals the node to json
func (node *Node) ToJson() (s string, err error) {
	m, err := json.Marshal(node)
	s = string(m)
	return
}
