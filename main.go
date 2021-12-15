package main

import (
	"fmt"
	"os"
	"strconv"

	"ngram-mapreduce/ngram"
)

func main() {
	args := os.Args[1:]
	if len(args) < 3 || args[0] == "--help" || args[0] == "-h" {
		fmt.Println("Usage: -t=<threads> -n=<ngram-count> <directory>")
		return
	}

	threadNum, err := strconv.Atoi(args[0][3:])
	if err != nil {
		fmt.Println("Error: -t must be an integer")
		return
	}

	ngramNum, err := strconv.Atoi(args[1][3:])
	if err != nil {
		fmt.Println("Error: -n must be an integer")
		return
	}

	files, err := os.ReadDir(args[2])
	if err != nil {
		fmt.Println("Error: that is not a valid directory")
		return
	}

	ngram.Compute(int32(threadNum), int32(ngramNum), files)
}
