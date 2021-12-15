package ngram

import (
	"fmt"
	"io/fs"
	"os"
)

func Compute(threadNum int32, ngramNum int32, files []os.DirEntry) {
	validFiles := []fs.DirEntry{}
	for i := range files {
		if files[i].Name()[:4] == ".txt" {
			validFiles = append(validFiles, files[i])
		}
	}

	fmt.Println(threadNum, ngramNum, validFiles[0].Name())
}
