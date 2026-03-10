package redis

import "fmt"

type keySet struct {
	prefix string
}

func newKeySet(queue string) keySet {
	if queue == "" {
		queue = "queue"
	}
	return keySet{prefix: queue}
}

func (k keySet) ready() string {
	return fmt.Sprintf("%s:ready", k.prefix)
}

func (k keySet) delayed() string {
	return fmt.Sprintf("%s:delayed", k.prefix)
}

func (k keySet) inflight() string {
	return fmt.Sprintf("%s:inflight", k.prefix)
}

func (k keySet) message(id string) string {
	return fmt.Sprintf("%s:msg:%s", k.prefix, id)
}

func (k keySet) status(id string) string {
	return fmt.Sprintf("%s:status:%s", k.prefix, id)
}

func (k keySet) dlq() string {
	return fmt.Sprintf("%s:dlq", k.prefix)
}
