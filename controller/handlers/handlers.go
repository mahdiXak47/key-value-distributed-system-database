package handlers

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/mahdiXak47/key-value-distributed-system-database/controller/cluster"
)

// IndexHandler renders the main dashboard with current cluster state.
func IndexHandler(cl *cluster.Cluster, tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if client wants JSON response
		if r.Header.Get("Accept") == "application/json" {
			// Get current cluster state
			nodes := cl.GetNodes()
			partitions := cl.GetPartitions()

			// Prepare response data
			response := struct {
				Nodes []struct {
					Address    string `json:"address"`
					IsLeader   bool   `json:"is_leader"`
					Partitions []int  `json:"partitions"`
				} `json:"nodes"`
				Partitions []struct {
					ID       int      `json:"id"`
					Leader   string   `json:"leader"`
					Replicas []string `json:"replicas"`
				} `json:"partitions"`
			}{}

			// Convert nodes to response format
			for _, node := range nodes {
				nodeInfo := struct {
					Address    string `json:"address"`
					IsLeader   bool   `json:"is_leader"`
					Partitions []int  `json:"partitions"`
				}{
					Address:  node.Address,
					IsLeader: false, // Will be updated when processing partitions
				}
				response.Nodes = append(response.Nodes, nodeInfo)
			}

			// Convert partitions to response format
			for _, part := range partitions {
				// Get leader address
				leaderAddr := ""
				if leader, ok := nodes[part.LeaderID]; ok {
					leaderAddr = leader.Address
					// Mark node as leader
					for i := range response.Nodes {
						if response.Nodes[i].Address == leaderAddr {
							response.Nodes[i].IsLeader = true
							response.Nodes[i].Partitions = append(response.Nodes[i].Partitions, part.ID)
						}
					}
				}

				// Get replica addresses
				replicaAddrs := []string{}
				for _, replicaID := range part.Replicas {
					if replica, ok := nodes[replicaID]; ok {
						replicaAddrs = append(replicaAddrs, replica.Address)
					}
				}

				response.Partitions = append(response.Partitions, struct {
					ID       int      `json:"id"`
					Leader   string   `json:"leader"`
					Replicas []string `json:"replicas"`
				}{
					ID:       part.ID,
					Leader:   leaderAddr,
					Replicas: replicaAddrs,
				})
			}

			// Set response headers and encode JSON
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		// Prepare data for template: list of nodes and partitions
		data := struct {
			Nodes      []*cluster.Node
			Partitions []struct {
				ID         int
				LeaderName string
				Replicas   []string
			}
		}{}

		// Get nodes and partitions data
		nodes := cl.GetNodes()
		partitions := cl.GetPartitions()

		// Copy nodes into slice
		for _, node := range nodes {
			data.Nodes = append(data.Nodes, node)
		}

		// For partitions, get leader name and replica statuses
		for _, part := range partitions {
			leaderName := ""
			if leader, ok := nodes[part.LeaderID]; ok {
				leaderName = leader.Name
			}
			repNames := []string{}
			for _, nid := range part.Replicas {
				if node, ok := nodes[nid]; ok {
					status := "Down"
					if node.Active {
						status = "Up"
					}
					repNames = append(repNames, node.Name+"("+status+")")
				}
			}
			data.Partitions = append(data.Partitions, struct {
				ID         int
				LeaderName string
				Replicas   []string
			}{
				ID:         part.ID,
				LeaderName: leaderName,
				Replicas:   repNames,
			})
		}

		log.Printf("IndexHandler: Rendering dashboard with %d nodes and %d partitions.", len(data.Nodes), len(data.Partitions))
		// Execute the template
		tmpl.Execute(w, data)
	}
}

// AddNodeHandler processes adding a new node (POST /node/add).
func AddNodeHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request to add new node")

		// Parse form data
		if err := r.ParseForm(); err != nil {
			log.Printf("Controller: Error parsing form data: %v", err)
			http.Error(w, "Error parsing form data", http.StatusBadRequest)
			return
		}

		name := r.FormValue("name")
		addr := r.FormValue("address")
		log.Printf("Controller: Adding node - Name: %s, Address: %s", name, addr)

		if name == "" || addr == "" {
			log.Printf("Controller: Invalid input - Name and address are required")
			http.Error(w, "Name and address are required", http.StatusBadRequest)
			return
		}

		if err := cl.AddNode(name, addr); err != nil {
			log.Printf("Controller: Failed to add node - Error: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("Controller: Successfully added node %s at %s", name, addr)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// RemoveNodeHandler processes removing a node (POST /node/remove).
func RemoveNodeHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request to remove node")
		r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		log.Printf("Controller: Removing node with ID: %d", id)

		cl.RemoveNode(id)
		log.Printf("Controller: Successfully removed node %d", id)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// AddPartitionHandler processes adding a partition (POST /partition/add).
func AddPartitionHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request to add new partition")
		if err := cl.AddPartition(); err != nil {
			log.Printf("Controller: Failed to add partition - Error: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("Controller: Successfully added new partition")
		http.Redirect(w, r, "/?message=Partition+added+successfully", http.StatusSeeOther)
	}
}

// RemovePartitionHandler processes removing a partition (POST /partition/remove).
func RemovePartitionHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request to remove partition")
		r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		log.Printf("Controller: Removing partition with ID: %d", id)

		cl.RemovePartition(id)
		log.Printf("Controller: Successfully removed partition %d", id)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// TransferPartitionHandler processes transferring a partition to a new node.
func TransferPartitionHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request to transfer partition")
		r.ParseForm()
		pid, _ := strconv.Atoi(r.FormValue("id"))
		newNodeID, _ := strconv.Atoi(r.FormValue("newNodeID"))
		log.Printf("Controller: Transferring partition %d to node %d", pid, newNodeID)

		cl.TransferPartition(pid, newNodeID)
		log.Printf("Controller: Successfully transferred partition %d to node %d", pid, newNodeID)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// ChangeLeaderHandler processes forcing a new leader for a partition.
func ChangeLeaderHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request to change partition leader")
		r.ParseForm()
		pid, _ := strconv.Atoi(r.FormValue("id"))
		newLeaderID, _ := strconv.Atoi(r.FormValue("newLeaderID"))
		log.Printf("Controller: Changing leader for partition %d to node %d", pid, newLeaderID)

		cl.ChangeLeader(pid, newLeaderID)
		log.Printf("Controller: Successfully changed leader for partition %d to node %d", pid, newLeaderID)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// ClusterStatusHandler returns the current cluster status in JSON format
func ClusterStatusHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Controller: Received request for cluster status")
		if cl == nil {
			log.Println("ClusterStatusHandler error: cluster.Cluster instance is nil")
			http.Error(w, "Internal server error: cluster data unavailable", http.StatusInternalServerError)
			return
		}

		// Get current cluster state
		nodes := cl.GetNodes()
		partitions := cl.GetPartitions()
		log.Printf("Controller: Current cluster state - Nodes: %d, Partitions: %d", len(nodes), len(partitions))

		if nodes == nil {
			log.Println("ClusterStatusHandler error: cl.GetNodes() returned nil map")
			http.Error(w, "Internal server error: nodes data is nil", http.StatusInternalServerError)
			return
		}
		if partitions == nil {
			log.Println("ClusterStatusHandler error: cl.GetPartitions() returned nil map")
			http.Error(w, "Internal server error: partitions data is nil", http.StatusInternalServerError)
			return
		}

		// Prepare response data
		// Ensure response struct is initialized with {}
		response := struct {
			Nodes []struct {
				Address    string `json:"address"`
				IsLeader   bool   `json:"is_leader"`  // This field might be redundant if Node struct in response has this
				Partitions []int  `json:"partitions"` // Partitions this node is part of (leader or replica)
			} `json:"nodes"`
			Partitions []struct {
				ID       int      `json:"id"`
				Leader   string   `json:"leader"`   // Leader's address
				Replicas []string `json:"replicas"` // All replica addresses for this partition
			} `json:"partitions"`
		}{} // Correct initialization

		// Temporary map to track leadership and partitions per node address for response
		nodeDetailsMap := make(map[string]struct {
			IsLeader   bool
			Partitions map[int]struct{}
		})

		// Populate partition details in response and gather info for nodes
		for _, part := range partitions {
			log.Printf("Controller: Processing partition %d", part.ID)
			leaderAddr := ""
			if leaderNode, ok := nodes[part.LeaderID]; ok {
				leaderAddr = leaderNode.Address
				// Ensure leader address has http:// prefix
				if !strings.HasPrefix(leaderAddr, "http://") && !strings.HasPrefix(leaderAddr, "https://") {
					leaderAddr = "http://" + leaderAddr
				}
				log.Printf("Controller: Partition %d leader: %s", part.ID, leaderAddr)
				if _, exists := nodeDetailsMap[leaderAddr]; !exists {
					nodeDetailsMap[leaderAddr] = struct {
						IsLeader   bool
						Partitions map[int]struct{}
					}{Partitions: make(map[int]struct{})}
				}
				nodeDetail := nodeDetailsMap[leaderAddr]
				nodeDetail.IsLeader = true // A node is a leader if it leads at least one partition
				nodeDetail.Partitions[part.ID] = struct{}{}
				nodeDetailsMap[leaderAddr] = nodeDetail
			}

			replicaAddrs := []string{}
			for _, replicaID := range part.Replicas {
				if replicaNode, ok := nodes[replicaID]; ok {
					replicaAddr := replicaNode.Address
					// Ensure replica address has http:// prefix
					if !strings.HasPrefix(replicaAddr, "http://") && !strings.HasPrefix(replicaAddr, "https://") {
						replicaAddr = "http://" + replicaAddr
					}
					replicaAddrs = append(replicaAddrs, replicaAddr)
					log.Printf("Controller: Partition %d replica: %s", part.ID, replicaAddr)
					// Mark this node as part of this partition
					if _, exists := nodeDetailsMap[replicaAddr]; !exists {
						nodeDetailsMap[replicaAddr] = struct {
							IsLeader   bool
							Partitions map[int]struct{}
						}{Partitions: make(map[int]struct{})}
					}
					nodeDetail := nodeDetailsMap[replicaAddr]
					nodeDetail.Partitions[part.ID] = struct{}{}
					nodeDetailsMap[replicaAddr] = nodeDetail
				}
			}
			response.Partitions = append(response.Partitions, struct {
				ID       int      `json:"id"`
				Leader   string   `json:"leader"`
				Replicas []string `json:"replicas"`
			}{
				ID:       part.ID,
				Leader:   leaderAddr,
				Replicas: replicaAddrs,
			})
		}

		// Populate node details in response
		for _, node := range nodes { // Iterate over the original nodes map from cl.GetNodes()
			nodeSpecificDetails, ok := nodeDetailsMap[node.Address]
			if !ok {
				// Node might not be part of any partition yet, or not a leader/replica of any known partition
				nodeSpecificDetails = struct {
					IsLeader   bool
					Partitions map[int]struct{}
				}{Partitions: make(map[int]struct{})}
			}
			pIDs := []int{}
			for pid := range nodeSpecificDetails.Partitions {
				pIDs = append(pIDs, pid)
			}

			// Ensure address has http:// prefix
			address := node.Address
			if !strings.HasPrefix(address, "http://") && !strings.HasPrefix(address, "https://") {
				address = "http://" + address
			}

			response.Nodes = append(response.Nodes, struct {
				Address    string `json:"address"`
				IsLeader   bool   `json:"is_leader"`
				Partitions []int  `json:"partitions"`
			}{
				Address:    address,
				IsLeader:   nodeSpecificDetails.IsLeader,
				Partitions: pIDs,
			})
		}

		log.Printf("Controller: Sending cluster status response")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("ClusterStatusHandler error: Failed to encode JSON response: %v", err)
			// If encoding fails, an error might have already been partially written or headers set.
			// Avoid writing http.Error here if headers are already sent.
			// This case usually means a problem with the `response` struct itself (e.g. unsupported types for JSON).
		}
	}
}
