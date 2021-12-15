package ngram

import (
	"fmt"
	"os"
)

func compute(threadNum int32, ngramNum int32, files []os.DirEntry) {
	fmt.Println(threadNum, ngramNum, files)
}
