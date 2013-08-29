// Package node implements node related functioniality
package node

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	e "github.com/MG-RAST/Shock/shock-server/errors"
	"github.com/MG-RAST/Shock/shock-server/node/acl"
	"github.com/MG-RAST/Shock/shock-server/node/file"
	"github.com/MG-RAST/Shock/shock-server/node/file/index"
	"github.com/MG-RAST/Shock/shock-server/user"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"os"
)

type Node struct {
	Id           string            `bson:"id" json:"id"`
	Version      string            `bson:"version" json:"version"`
	File         file.File         `bson:"file" json:"file"`
	Attributes   interface{}       `bson:"attributes" json:"attributes"`
	Indexes      Indexes           `bson:"indexes" json:"indexes"`
	Acl          acl.Acl           `bson:"acl" json:"-"`
	VersionParts map[string]string `bson:"version_parts" json:"-"`
	Tags         []string          `bson:"tags" json:"tags"`
	Revisions    []Node            `bson:"revisions" json:"-"`
	Linkages     []linkage         `bson:"linkage" json:"linkages"`
	CreatedOn    string            `bson:"created_on" json:"created_on"`
	LastModified string            `bson:"last_modified" json:"last_modified"`
}

type linkage struct {
	Type      string   `bson: "relation" json:"relation"`
	Ids       []string `bson:"ids" json:"ids"`
	Operation string   `bson:"operation" json:"operation"`
}

type Indexes map[string]IdxInfo

type IdxInfo struct {
	Type        string `bson:"index_type" json:"-"`
	TotalUnits  int64  `bson:"total_units" json:"total_units"`
	AvgUnitSize int64  `bson:"average_unit_size" json:"average_unit_size"`
}

type FormFiles map[string]FormFile

type FormFile struct {
	Name     string
	Path     string
	Checksum map[string]string
}

// New creates a new Node and initializes
// default values.
func New() (node *Node) {
	node = new(Node)
	node.Indexes = make(map[string]IdxInfo)
	node.File.Checksum = make(map[string]string)
	node.Id = uuid.New()
	node.LastModified = "-"
	return
}

// LoadFromDisk will load a node from disk
func LoadFromDisk(id string) (n *Node, err error) {
	path := getPath(id)
	if nbson, err := ioutil.ReadFile(path + "/" + id + ".bson"); err != nil {
		return nil, err
	} else {
		n = new(Node)
		if err = bson.Unmarshal(nbson, &n); err != nil {
			return nil, err
		}
	}
	return
}

// CreateNodeUpload will create a new node from
// upload parameters and files.
func CreateNodeUpload(u *user.User, params map[string]string, files FormFiles) (node *Node, err error) {
	node = New()
	if u.Uuid != "" {
		node.Acl.SetOwner(u.Uuid)
		node.Acl.Set(u.Uuid, acl.Rights{"read": true, "write": true, "delete": true})
	} else {
		node.Acl = acl.Acl{Owner: "", Read: make([]string, 0), Write: make([]string, 0), Delete: make([]string, 0)}
	}
	err = node.Mkdir()
	if err != nil {
		return
	}
	err = node.Update(params, files)
	if err != nil {
		return
	}
	err = node.Save()
	return
}

// FileReader determines the type of file creates
// an appropriate file reader. Warning: do not directly
// attempt to create a file reader. Some node files
// are not compatiable will direct access.
func (node *Node) FileReader() (reader file.ReaderAt, err error) {
	if node.File.Virtual {
		readers := []file.ReaderAt{}
		nodes := Nodes{}
		if _, err := dbFind(bson.M{"id": bson.M{"$in": node.File.VirtualParts}}, &nodes, nil); err != nil {
			return nil, err
		}
		if len(nodes) > 0 {
			for _, n := range nodes {
				if r, err := n.FileReader(); err == nil {
					readers = append(readers, r)
				} else {
					return nil, err
				}
			}
		}
		return file.MultiReaderAt(readers...), nil
	}
	return os.Open(node.FilePath())
}

// Index returns the named index from disk or virtualizes it
func (node *Node) Index(name string) (idx index.Index, err error) {
	if index.Has(name) {
		idx = index.NewVirtual(name, node.FilePath(), node.File.Size, 10240)
	} else {
		idx = index.New()
		err = idx.Load(node.IndexPath() + "/" + name + ".idx")
	}
	return
}

// Delete will delete an node and the contents on disk if it is not
// referenced from other nodes.
func (node *Node) Delete() (err error) {
	nodes := Nodes{}
	if _, err = dbFind(bson.M{"virtual_parts": node.Id}, &nodes, nil); err != nil {
		return err
	}
	if len(nodes) != 0 {
		return errors.New(e.NodeReferenced)
	} else {
		if err = dbDelete(bson.M{"id": node.Id}); err != nil {
			return err
		}
	}
	return node.Rmdir()
}

// SetIndexInfo sets index info and saves node
func (node *Node) SetIndexInfo(indextype string, idxinfo IdxInfo) (err error) {
	node.Indexes[indextype] = idxinfo
	err = node.Save()
	return
}

// SetFileFormat sets file format and saves node
func (node *Node) SetFileFormat(format string) (err error) {
	node.File.Format = format
	err = node.Save()
	return
}

// SetAttributes sets attributes from json encoded file and saves node
func (node *Node) SetAttributes(attr FormFile) (err error) {
	attributes, err := ioutil.ReadFile(attr.Path)
	if err != nil {
		return
	}
	err = json.Unmarshal(attributes, &node.Attributes)
	if err != nil {
		return
	}
	err = node.Save()
	return
}
