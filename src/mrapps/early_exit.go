package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc_long.go
//

import (
	"strconv"
	"strings"
	"time"

	"cpsc416/mr"
)

// The map function is called once for each file of input.
// This map function just returns 1 for each file
func Map(filename string, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{filename, "1"})
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	// some reduce tasks sleep for a long time; potentially seeing if
	// a worker will accidentally exit early
	if strings.Contains(key, "sherlock") || strings.Contains(key, "tom") {
		time.Sleep(time.Duration(3 * time.Second))
	}
	// return the number of occurrences of this file.
	return strconv.Itoa(len(values))
}
