package metadata

import "time"

const nodeTTL = 10 * time.Second

func (s *Server) StartCleanupLoop() {
	go func() {
		for {
			time.Sleep(5 * time.Second)

			now := time.Now()
			s.State.Mu.Lock()

			for id, node := range s.State.Nodes {
				if now.Sub(node.Lastseen) > nodeTTL {
					delete(s.State.Nodes, id)
				}
			}
			s.State.Mu.Unlock()
		}
	}()
}
