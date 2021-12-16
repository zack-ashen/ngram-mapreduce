package mapreduce

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Pair struct {
	Key   string
	Value int32
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func display(workerId int32, freq map[string]int32) {
	displayString := "---Frequencies for Worker ID: " + strconv.Itoa(int(workerId)) + "---\n"
	freqList := make(PairList, len(freq))

	i := 0
	for key, value := range freq {
		freqList[i] = Pair{key, value}
		i++
	}
	sort.Sort(sort.Reverse(freqList))

	for i := 0; i < 5; i++ {
		displayString += freqList[i].Key + "-->" + strconv.Itoa(int(freqList[i].Value)) + "\n"
	}

	fmt.Println(displayString)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func groupBy(localFreq map[string]int32, threadNum int32) []map[string]int32 {
	collections := make([]map[string]int32, threadNum)
	for i := range collections {
		collections[i] = make(map[string]int32)
	}

	for key, value := range localFreq {
		hashedNgram := hash(key)
		responsibleWorker := hashedNgram % uint32(threadNum)
		collections[responsibleWorker][key] = value

		//fmt.Println(key, "|", value, "|", responsibleWorker, "|", collections[responsibleWorker][key])
	}

	/*for _, collection := range collections {
		for key, value := range collection {
			fmt.Println(key, "|", value)
		}

		fmt.Println("------------------------")
	}*/

	return collections
}

func processFile(file string, frequencies *map[string]int32, ngramNum int32) {
	contentB, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}
	content := string(contentB)

	re1, err := regexp.Compile(`[\r\n\t\f\v]`)
	if err != nil {
		log.Fatal(err)
	}

	re2, err := regexp.Compile(`[^a-zA-Z \r\n\t\f\v]`)
	if err != nil {
		log.Fatal(err)
	}

	content = re1.ReplaceAllString(content, " ")
	content = re2.ReplaceAllString(content, " | ")
	content = strings.ToLower(content)

	curNgram := make([]string, ngramNum)
	ngrams := []string{}
	for _, word := range strings.Fields(content) {
		if len(curNgram) == int(ngramNum) {
			ngrams = append(ngrams, strings.Join(curNgram[:], " "))
			curNgram, _ = curNgram[1:], curNgram[0]
		}

		if word == "|" {
			curNgram = nil
			continue
		}

		curNgram = append(curNgram, word)
	}
	ngrams = ngrams[1:]

	for _, ngram := range ngrams {
		(*frequencies)[ngram]++
	}
}

func mapStep(workerId int32, files []string, threadNum int32, ngramNum int32) map[string]int32 {
	frequencies := make(map[string]int32)
	for i := workerId; i < int32(len(files)); i += threadNum {
		processFile(files[i], &frequencies, ngramNum)
	}

	return frequencies
}

func Compute(threadNum int32, ngramNum int32, files []string) {
	collectionMessages := make([]chan map[string]int32, threadNum)
	for i := 0; i < int(threadNum); i++ {
		collectionMessages[i] = make(chan map[string]int32, threadNum)
	}

	wg := new(sync.WaitGroup)
	wg.Add(int(threadNum))
	for i := 0; i < int(threadNum); i++ {
		go func(workerId int32, messages []chan map[string]int32, threadNum int32, ngramNum int32) {
			localFreq := mapStep(workerId, files, threadNum, ngramNum)
			localCollections := groupBy(localFreq, threadNum)

			for i := range localCollections {
				messages[i] <- localCollections[i]
			}

			subcollection := make(map[string]int32)
			for i := 0; i < int(threadNum); i++ {
				recievedCollection := <-messages[workerId]
				for ngram, freq := range recievedCollection {
					subcollection[ngram] += freq
				}
			}

			display(workerId, subcollection)

			wg.Done()
		}(int32(i), collectionMessages, threadNum, ngramNum)
	}

	wg.Wait()
}
