package metadata

// PickNodes selects up to `replicationFactor` live nodes.
// nodes MUST contain only healthy nodes.
// This function is replication-aware and must not be used for single-node placement.
func PickNodes(nodes map[string]NodeStatus, n int) []string {
	res := make([]string, 0, n)
	for _, node := range nodes {
		res = append(res, node.Address)
		if len(res) == n {
			break
		}
	}
	return res
}

func PickReplicaNodes(nodes map[string]NodeStatus, rf int) []string {
	return PickNodes(nodes, rf)
}

func pickSource(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0]
}

func pickTarget(all map[string]NodeStatus, existing []string) string {
	exists := make(map[string]bool)
	for _, n := range existing {
		exists[n] = true
	}

	for id := range all {
		if !exists[id] {
			return id
		}
	}
	return ""
}
