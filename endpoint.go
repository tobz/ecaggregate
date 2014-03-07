package ecaggregate

import "fmt"
import "net"
import "code.google.com/p/go-semver/version"

var NewConfigGetVersion = &version.Version{Major: 1, Minor: 4, Patch: 14}

type Cluster struct {
	ConfigGetCommand string
	Address          net.Addr
}

func NewCluster(endpointRaw string, clusterVersion string) (*Cluster, error) {
	// Make sure the endpoint resolves.
	ta, err := net.ResolveTCPAddr("tcp", endpointRaw)
	if err != nil {
		return nil, fmt.Errorf("error parsing endpoint address: %s", err)
	}

	// Parse the cluster version to figure out what the get command should be.
	v, err := version.Parse(clusterVersion)
	if err != nil {
		return nil, fmt.Errorf("error parsing cluster version: %s", err)
	}

	configGetCommand := "config get cluster"
	if v.Less(NewConfigGetVersion) {
		configGetCommand = "get AmazonElastiCache:cluster"
	}

	return &Cluster{ConfigGetCommand: configGetCommand, Address: ta}, nil
}
