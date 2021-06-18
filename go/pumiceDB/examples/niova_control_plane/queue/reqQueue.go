package reqqueue

import (
	"niovactlplane/niovareqlib"
)

//Structure definition for queue.
type Queue struct {
	nodes []*niovareqlib.NiovaCtlReq
	size  int
	head  int
	tail  int
	Count int
}

// NewQueue returns a new queue with the given initial size.
func NewQueue(size int) *Queue {
	return &Queue{
		nodes: make([]*niovareqlib.NiovaCtlReq, size),
		size:  size,
	}
}

// Push adds a request object to the queue.
func (q *Queue) Push(n *niovareqlib.NiovaCtlReq) {
	if q.head == q.tail && q.Count > 0 {
		nodes := make([]*niovareqlib.NiovaCtlReq, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}
	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.Count++
}

// Pop removes and returns a request object from the queue in FIFO order.
func (q *Queue) Pop() *niovareqlib.NiovaCtlReq {
	if q.Count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.Count--
	return node
}
