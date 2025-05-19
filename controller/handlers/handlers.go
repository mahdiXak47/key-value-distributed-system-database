package handlers

import (
	"html/template"
	"net/http"
	"strconv"

	"github.com/mahdiXak47/key-value-distributed-system-database/controller/cluster"
)

// IndexHandler renders the main dashboard with current cluster state.
func IndexHandler(cl *cluster.Cluster, tmpl *template.Template) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
