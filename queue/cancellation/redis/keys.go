package redis

import "fmt"

type keySet struct {
	prefix string
}

func newKeySet(prefix string) keySet {
	if prefix == "" {
		prefix = defaultPrefix
	}
	return keySet{prefix: prefix}
}

func (k keySet) stream() string {
	return fmt.Sprintf("%s:stream", k.prefix)
}

func (k keySet) request(key string) string {
	return fmt.Sprintf("%s:req:%s", k.prefix, key)
}
