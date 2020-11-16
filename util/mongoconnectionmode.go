package util

type MongoConnectionMode int

const (
	Direct  MongoConnectionMode = iota // directly connect to a specific node
	Cluster                            // use server selection and read preference to pick a node
)

func (m MongoConnectionMode) String() string {
	if m == Direct {
		return "direct"
	}
	return "cluster"
}
