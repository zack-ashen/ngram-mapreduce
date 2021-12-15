package mapreduce

import "os"

func Compute(threadNum int32, ngramNum int32, files []os.DirEntry) {
	collectionMessages := make(chan map[string]int32)

}
