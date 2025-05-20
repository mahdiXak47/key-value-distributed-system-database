package handlers

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strconv"

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

		// Execute the template
		tmpl.Execute(w, data)
	}
}

// AddNodeHandler processes adding a new node (POST /node/add).
func AddNodeHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		name := r.FormValue("name")
		addr := r.FormValue("address")

		if err := cl.AddNode(name, addr); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// RemoveNodeHandler processes removing a node (POST /node/remove).
func RemoveNodeHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		cl.RemoveNode(id)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// AddPartitionHandler processes adding a partition (POST /partition/add).
func AddPartitionHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := cl.AddPartition(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// RemovePartitionHandler processes removing a partition (POST /partition/remove).
func RemovePartitionHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		cl.RemovePartition(id)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// TransferPartitionHandler processes transferring a partition to a new node.
func TransferPartitionHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		pid, _ := strconv.Atoi(r.FormValue("id"))
		newNodeID, _ := strconv.Atoi(r.FormValue("newNodeID"))
		cl.TransferPartition(pid, newNodeID)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// ChangeLeaderHandler processes forcing a new leader for a partition.
func ChangeLeaderHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		pid, _ := strconv.Atoi(r.FormValue("id"))
		newLeaderID, _ := strconv.Atoi(r.FormValue("newLeaderID"))
		cl.ChangeLeader(pid, newLeaderID)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

// ClusterStatusHandler returns the current cluster status in JSON format
func ClusterStatusHandler(cl *cluster.Cluster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}
