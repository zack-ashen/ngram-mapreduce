package ngram

import (
	"ngram-mapreduce/mapreduce"
)

func Compute(threadNum int32, ngramNum int32, files []string) {
	mapreduce.Compute(threadNum, ngramNum, files)
}
