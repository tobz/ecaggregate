package ecaggregate

import "io"
import "net"
import "fmt"
import "time"
import "bufio"
import "bytes"
import "strings"

type Server struct {
	mapping        *Mapping
	updater        *Updater
	configResponse string
}

func NewServer(m *Mapping, u *Updater) *Server {
	return &Server{m, u, ""}
}

func (s *Server) Listen() error {
	go func() {
		update := time.Tick(time.Second)

		for {
			select {
			case <-update:
				var nodes []string

				for _, clusterName := range s.mapping.ClusterNames {
					partialNodes, err := s.updater.GetNodesForCluster(clusterName)
					if err != nil {
						continue
					}

					nodes = append(nodes, partialNodes...)
				}

				s.configResponse = strings.Join(nodes, " ")
			}
		}
	}()

	ln, err := net.Listen("tcp", s.mapping.ListenAddr.String())
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}

			go s.handleConnection(conn)
		}
	}()

	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	defer rw.Writer.Flush()

	buf := make([]byte, 128)
	for {
		if !s.processRequest(rw, &buf) {
			break
		}
	}
}

func (s *Server) processRequest(rw *bufio.ReadWriter, buf *[]byte) bool {
	if !readLine(rw.Reader, buf) {
		return false
	}

	line := *buf
	if len(line) == 0 {
		return false
	}

	if bytes.HasPrefix(line, []byte("config get cluster")) {
		return s.processConfigGetCluster(rw)
	}

	return false
}

func (s *Server) processConfigGetCluster(rw *bufio.ReadWriter) bool {
	// Hard-coded config version for now.
	configData := fmt.Sprintf("%d\r\n%s", 1, s.configResponse)

	response := fmt.Sprintf("CONFIG cluster 0 %d\r\n%s\r\n\r\nEND\r\n", len(configData), configData)
	rw.Writer.WriteString(response)

	return false
}

func readBytesUntil(r *bufio.Reader, endCh byte, lineBuf *[]byte) bool {
	line := *lineBuf
	line = line[0:0]
	for {
		s, err := r.ReadSlice(endCh)
		if err == nil {
			line = append(line, s...)
			break
		}

		if err == bufio.ErrBufferFull {
			line = append(line, s...)

			c, _ := r.ReadByte()
			line = append(line, c)
			continue
		}

		if err == io.EOF && len(line) == 0 {
			*lineBuf = line
			return true
		}

		return false
	}

	*lineBuf = line[:len(line)-1]
	return true
}

func readLine(r *bufio.Reader, lineBuf *[]byte) bool {
	if !readBytesUntil(r, '\n', lineBuf) {
		return false
	}

	line := *lineBuf
	if len(line) == 0 {
		return true
	}

	lastN := len(line) - 1
	if line[lastN] == '\r' {
		line = line[:lastN]
	}

	*lineBuf = line
	return true
}
