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

// display the frequencies of ngrams for a specific worker
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

// hash a string to int32
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// group frequencies into subcollections to be distributed to other workers
func groupBy(localFreq map[string]int32, threadNum int32) []map[string]int32 {
	// initialize collections array
	collections := make([]map[string]int32, threadNum)
	for i := range collections {
		collections[i] = make(map[string]int32)
	}

	// distribute frequency to subcollection
	for key, value := range localFreq {
		hashedNgram := hash(key)
		responsibleWorker := hashedNgram % uint32(threadNum)
		collections[responsibleWorker][key] = value
	}

	return collections
}

// process file and get the frequencies of ngrams
func processFile(file string, frequencies *map[string]int32, ngramNum int32) {
	// convert file to string
	contentB, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}
	content := string(contentB)

	// match end line, tabs, etc...
	re1, err := regexp.Compile(`[\r\n\t\f\v]`)
	if err != nil {
		log.Fatal(err)
	}

	// match any punctuation or non normal character
	re2, err := regexp.Compile(`[^a-zA-Z \r\n\t\f\v]`)
	if err != nil {
		log.Fatal(err)
	}

	content = re1.ReplaceAllString(content, " ")
	content = re2.ReplaceAllString(content, " | ")
	content = strings.ToLower(content)

	// parse ngrams from file
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

// mapStep maps ngrams to frequencies for each file a worker is responsible for
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
			// map and groupBy frequencies
			localFreq := mapStep(workerId, files, threadNum, ngramNum)
			localCollections := groupBy(localFreq, threadNum)

			// distribute collections to other workers
			for i := range localCollections {
				messages[i] <- localCollections[i]
			}

			// recieve collections from other workers
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
