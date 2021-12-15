package ngram

import (
	"io/fs"
	"ngram-mapreduce/mapreduce"
	"os"
)

func Compute(threadNum int32, ngramNum int32, files []os.DirEntry) {
	validFiles := []fs.DirEntry{}
	for i := range files {
		if files[i].Name()[:4] == ".txt" {
			validFiles = append(validFiles, files[i])
		}
	}

	mapreduce.Compute(threadNum, ngramNum, validFiles)
}
