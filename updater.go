package ecaggregate

import "io"
import "fmt"
import "net"
import "log"
import "time"
import "bytes"
import "bufio"
import "strings"
import "io/ioutil"

var (
	crlf      = []byte("\r\n")
	space     = []byte(" ")
	resultEnd = []byte("END\r\n")
)

type Updater struct {
	clusterEndpoints map[string]*Cluster
	clusterNodes     map[string][]string
	updateInterval   time.Duration
	logger           *log.Logger
}

func NewUpdater(clusters map[string]*Cluster, updateInterval time.Duration, logger *log.Logger) *Updater {
	return &Updater{
		clusterEndpoints: clusters,
		clusterNodes:     make(map[string][]string),
		updateInterval:   updateInterval,
		logger:           logger,
	}
}

func (u *Updater) Run() {
	// Create a runner to execute our check every internal period.
	go func() {
		for {
			// Try and talk to each configured node to get its configuration.
			for clusterName, endpoint := range u.clusterEndpoints {
				c, err := getBoundedConnection(endpoint.Address)
				if err != nil {
					// Log something here, maybe, but ultimately, just skip, since we'll check in again.
					continue
				}

				// Wrap our connection in buffered I/O.
				rw := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))

				// Write the config get command to the server.
				if _, err := fmt.Fprintf(rw, "%s\r\n", endpoint.ConfigGetCommand); err != nil {
					// Log something here and skip.
					u.logger.Printf("Failed to write config get command to endpoint '%s': %s", clusterName, err)
					c.Close()

					continue
				}

				// Flush the buffer.
				if err := rw.Flush(); err != nil {
					// Log something here and skip.
					u.logger.Printf("Failed to flush config get command to endpoint '%s': %s", clusterName, err)
					c.Close()

					continue
				}

				// Try and parse a response back.
				r, err := parseGetResponse(rw.Reader)
				if err != nil {
					// Log something here and skip.
					u.logger.Printf("Failed to parse get response from endpoint '%s': %s", clusterName, err)
					c.Close()

					continue
				}

				// Now we can close our connection.
				c.Close()

				// Pull out the individual nodes and set them for the given node.
				trimmed := strings.TrimSpace(r)
				parts := strings.Split(trimmed, "\n")
				if len(parts) != 2 {
					// Bad response.  Log something here and skip.
					u.logger.Printf("Invalid number of parts in response from endpoint '%s': only %d part(s), expected 2", clusterName, len(parts))
					continue
				}

				nodes := strings.Split(parts[1], " ")
				if len(nodes) == 1 && nodes[0] == "" {
					// No nodes available?  Log something here and skip.
					u.logger.Printf("No available nodes from endpoint '%s'!", clusterName)
					continue
				}

				// Set the nodes for this cluster.
				u.clusterNodes[clusterName] = nodes
			}

			// Sleep for our update interval now.
			time.Sleep(u.updateInterval)
		}
	}()
}

func (u *Updater) GetNodesForCluster(clusterName string) ([]string, error) {
	nodes, ok := u.clusterNodes[clusterName]
	if !ok {
		return nil, fmt.Errorf("nodes for cluster '%s' not found", clusterName)
	}

	return nodes, nil
}

func getBoundedConnection(addr net.Addr) (net.Conn, error) {
	return net.DialTimeout(addr.Network(), addr.String(), time.Second)
}

func parseGetResponse(r *bufio.Reader) (string, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return "", err
	}

	if bytes.Equal(line, resultEnd) {
		return "", fmt.Errorf("result end")
	}

	size, err := scanGetResponseLine(line)
	if err != nil {
		return "", err
	}

	value, err := ioutil.ReadAll(io.LimitReader(r, int64(size)+2))
	if err != nil {
		return "", err
	}

	if !bytes.HasSuffix(value, crlf) {
		return "", fmt.Errorf("elasticache: corrupt get result read")
	}

	value = value[:size]

	return string(value), nil
}

func scanGetResponseLine(line []byte) (int, error) {
	var size int
	var key string
	var flags uint32

	// Set up our sscanf target.
	dest := []interface{}{&key, &flags, &size}

	// Figure out what our pattern to scan in should be.
	pattern := "CONFIG %s %d %d\r\n"

	// Scan the line.
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("elasticache: unexpected line in get response: %q", line)
	}

	return size, nil
}
