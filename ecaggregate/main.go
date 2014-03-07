package main

import "os"
import "fmt"
import "log"
import "flag"
import "time"
import "syscall"
import "os/signal"
import "github.com/tobz/ecaggregate"
import "github.com/kylelemons/go-gypsy/yaml"

var configurationFile string

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	flag.StringVar(&configurationFile, "conf", "", "the location of the configuration file")
	flag.Parse()

	if configurationFile == "" {
		logger.Fatalf("you must specify a configuration file!")
	}

	if _, err := os.Stat(configurationFile); err != nil && os.IsNotExist(err) {
		logger.Fatalf("specified configuration file '%s' does not exit!", configurationFile)
	}

	c, err := yaml.ReadFile(configurationFile)
	if err != nil {
		logger.Fatalf("error reading configuration file: %s", err)
	}

	// Make sure it's valid.
	clusterCount, err := c.Count("clusters")
	if err != nil {
		logger.Fatalf("'clusters' must be a list of clusters, including name and configuration endpoint")
	}

	mappingCount, err := c.Count("mappings")
	if err != nil {
		logger.Fatalf("'mappings' must be a list of mappings, where there is a name, a port to listen on, and the clusters to send back information for on those ports")
	}

	clusters := make(map[string]*ecaggregate.Cluster)

	for i := 0; i < clusterCount; i++ {
		var clusterName string
		var clusterEndpoint string
		var clusterVersion string

		clusterName, err := c.Get(fmt.Sprintf("clusters[%d].name", i))
		if err != nil {
			logger.Fatalf("missing 'name' entry for cluster #%d", i)
		}

		clusterEndpoint, err = c.Get(fmt.Sprintf("clusters[%d].endpoint", i))
		if err != nil {
			logger.Fatalf("missing 'endpoint' entry for cluster '%s'", clusterName)
		}

		clusterVersion, err = c.Get(fmt.Sprintf("clusters[%d].version", i))
		if err != nil {
			logger.Fatalf("missing 'version' entry for cluster '%s'", clusterName)
		}

		cluster, err := ecaggregate.NewCluster(clusterEndpoint, clusterVersion)
		if err != nil {
			logger.Fatalf("couldn't get cluster object: %s", err)
		}

		clusters[clusterName] = cluster
	}

	mappings := make(map[string]*ecaggregate.Mapping)

	for i := 0; i < mappingCount; i++ {
		var mappingName string
		var mappingListenAddr string
		var clusterName string
		var clusterNames []string

		mappingName, err := c.Get(fmt.Sprintf("mappings[%d].name", i))
		if err != nil {
			logger.Fatalf("missing 'name' entry for mapping #%d", i)
		}

		mappingListenAddr, err = c.Get(fmt.Sprintf("mappings[%d].listenAddress", i))
		if err != nil {
			logger.Fatalf("missing 'listenAddress' entry for mapping '%s'", mappingName)
		}

		count, err := c.Count(fmt.Sprintf("mappings[%d].clusters", i))
		if err != nil {
			logger.Fatalf("missing 'clusters' entry for mapping '%s'", mappingName)
		}

		for j := 0; j < count; j++ {
			clusterName, err = c.Get(fmt.Sprintf("mappings[%d].clusters[%d]", i, j))
			if err != nil {
				logger.Fatalf("error while trying to get cluster entry #%d for mapping '%s'", j, mappingName)
			}

			if _, ok := clusters[clusterName]; !ok {
				logger.Fatalf("mapping '%s' references non-existent cluster '%s'", mappingName, clusterName)
			}

			clusterNames = append(clusterNames, clusterName)
		}

		mapping, err := ecaggregate.NewMapping(mappingListenAddr, clusterNames)
		if err != nil {
			logger.Fatalf("couldn't get mapping object: %s", err)
		}

		if _, ok := mappings[mappingName]; ok {
			logger.Fatalf("duplicate mapping found for '%s'!", mappingName)
		}

		for name, m := range mappings {
			if m.ListenAddr.String() == mapping.ListenAddr.String() {
				logger.Fatalf("mapping '%s' has same listen address '%s' as existing mapping '%s'!", mappingName, mappingListenAddr, name)
			}
		}

		mappings[mappingName] = mapping
	}

	updateIntervalRaw, err := c.Get("updateInterval")
	if err != nil {
		logger.Fatalf("you must specify an update interval!")
	}

	updateInterval, err := time.ParseDuration(updateIntervalRaw)
	if err != nil {
		logger.Fatalf("failed to parse update interval: %s", err)
	}

	logger.Printf("Loaded %d clusters and %d mappings.\n", len(clusters), len(mappings))

	updater := ecaggregate.NewUpdater(clusters, updateInterval, logger)
	updater.Run()

	logger.Println("Updater running.")

	for name, mapping := range mappings {
		// The server object launches goroutines which close over itself, so we shouldn't need
		// to purposefully keep the objects in scope.
		server := ecaggregate.NewServer(mapping, updater)
		if err := server.Listen(); err != nil {
			logger.Fatalf("caught an error while starting a listener for '%s': %s", name, err)
			continue
		}

		logger.Printf("Listener '%s' running.", name)
	}

	// Spin until we're told to stop.
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)

	sig := <-s

	logger.Printf("Got signal '%s'.  Exiting.\n", sig)
}
