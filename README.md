# MapReduce Ngram

This Golang program implements the MapReduce pattern with Goroutines and channels to count n-grams in a directory of text files.

## Usage
`ngram-mapreduce -t=<thread-count> -n=<n-gram count> <directory>`

## Installation
```sh
go build
./ngram-mapreduce -t=3 -n=2 <directory>
```
