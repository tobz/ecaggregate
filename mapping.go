package ecaggregate

import "fmt"
import "net"

type Mapping struct {
	ListenAddr   net.Addr
	ClusterNames []string
}

func NewMapping(listenAddr string, clusterNames []string) (*Mapping, error) {
	// Make sure the listen address resolves.
	la, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("error parsing listen address: %s", err)
	}

	return &Mapping{ListenAddr: la, ClusterNames: clusterNames}, nil
}
