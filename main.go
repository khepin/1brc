package main

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"unsafe"

	"github.com/dolthub/swiss"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("missing file name")
		return
	}
	file := os.Args[1]
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	finfo, _ := f.Stat()
	fileSize := int(finfo.Size())
	fmt.Println(finfo.Size())
	data, _ := syscall.Mmap(int(f.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)

	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	pageSize := os.Getpagesize() * 16
	fmt.Println(pageSize, fileSize/pageSize)

	chunkSize := fileSize

	// channel of [start, end] offsets that the workers pull from
	chunks := make(chan [2]int, 10_000)
	go func() {
		cur := 0
		for {
			if cur > chunkSize {
				close(chunks)
				return
			}

			end := cur + pageSize
			if end > fileSize {
				end = fileSize - 1
			}
			chunks <- [2]int{cur, end}
			cur += pageSize
		}
	}()

	wg := sync.WaitGroup{}
	// results := make(chan ResultMap, cpus*10) // Standard go map
	results := make(chan *swiss.Map[string, *MeasureAggregate], cpus*10) // Testing with swiss map

	// Start 1 worker / cpu
	for i := 0; i < cpus; i++ {
		wg.Add(1)
		go func(idx int) {
			// Each worker get its own hashmap to store results and avoid concurrent access
			// They will be merged at the end
			// localResults := ResultMap{}
			localResults := swiss.NewMap[string, *MeasureAggregate](11_000)
			results <- localResults

			for {
				chunk, ok := <-chunks // grab next chunk of data to be worked on
				if !ok {
					break
				}
				processChunk(data, chunk, localResults)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	close(results)

	// printMapResults(results)
	printSwissResults(results)
}

func processChunk(data []byte, chunk [2]int, results *swiss.Map[string, *MeasureAggregate]) {
	start := chunk[0]
	end := chunk[1]
	maxEnd := len(data)

	if start > 0 {
		// Move start to the next newline. The `end` of each chunk is unlikely to be perfectly aligned with a `\n` so we move to the next
		// newline as our start.
		// Similarly, we process after `end` until the next newline too
		for data[start] != '\n' {
			start++
		}
		start++
	}

	for data[end] != '\n' {
		end++
	}

	cur := start
	nameBounds := [2]int{}  // [start, end] of the station name
	wholeBounds := [2]int{} // [start, end] of the whole number
	dec := 0                // decimal part
	for i := start; i < end; i++ {
		if data[i] == ';' {
			nameBounds = [2]int{cur, i}
			cur = i + 1
		}
		if data[i] == '.' {
			wholeBounds = [2]int{cur, i}
			cur = i + 1
		}

		if data[i] == '\n' || i == maxEnd-1 {
			dec = int(data[cur]) - 48 // 48 is the ascii code for 0, get decimal int value
			cur = i + 1

			whole := 0
			mul := 1
			sign := 1
			// parse the whole part to an int.
			// we work only with ints at this stage. So 12.1 becomes 121, 8.0 becomes 80
			// We are guaranteed to have exactly 1 digit by the file format
			for a := wholeBounds[1] - 1; a >= wholeBounds[0]; a-- {
				if data[a] == '-' {
					sign = -1
					continue
				}
				whole += (int(data[a]) - 48) * mul
				mul *= 10
			}
			val := sign * (whole*10 + dec)

			// get a string type from the underlying bytes without copy
			nameString := unsafe.String(&data[nameBounds[0]], nameBounds[1]-nameBounds[0])

			// Get the current aggregate for this station
			agg, ok := results.Get(nameString)
			if !ok {
				agg = &MeasureAggregate{
					Sum: 0.0,
					Min: val,
					Max: val,
				}
				results.Put(nameString, agg)
			}

			// Update the aggregate with new value
			agg.Sum += val
			agg.Count++

			if val < agg.Min {
				agg.Min = val
			}
			if val > agg.Max {
				agg.Max = val
			}

			if i >= end {
				return
			}
		}
	}
}

func printMapResults(results chan ResultMap) {
	red := <-results
	for result := range results {
		for k, v := range result {
			agg, ok := red[k]
			if !ok {
				red[k] = v
			} else {
				agg.Sum += v.Sum
				agg.Count += v.Count
				if v.Min < agg.Min {
					agg.Min = v.Min
				}
				if v.Max > (agg.Max) {
					agg.Max = v.Max
				}
			}
		}
	}

	for k, v := range red {
		avg := float64(v.Sum) / (10 * float64(v.Count))
		fmt.Printf("%s: %f : %f <> %f\n", k, float64(avg)/10, float64(v.Min)/10, float64(v.Max)/10)
	}
}

func printSwissResults(results chan *swiss.Map[string, *MeasureAggregate]) {
	red := <-results
	for result := range results {
		result.Iter(func(k string, v *MeasureAggregate) bool {
			agg, ok := red.Get(k)
			if !ok {
				red.Put(k, v)
			} else {
				agg.Sum += v.Sum
				agg.Count += v.Count
				if v.Min < agg.Min {
					agg.Min = v.Min
				}
				if v.Max > (agg.Max) {
					agg.Max = v.Max
				}
			}

			return false
		})
	}

	red.Iter(func(k string, v *MeasureAggregate) bool {
		avg := float64(v.Sum) / (10 * float64(v.Count))
		fmt.Printf("%s: %f : %f <> %f\n", k, avg, float64(v.Min)/10, float64(v.Max)/10)
		return false
	})
}

type ResultMap map[string]*MeasureAggregate

func (r ResultMap) Get(key string) (*MeasureAggregate, bool) {
	v, ok := r[key]
	return v, ok
}

func (r ResultMap) Put(key string, value *MeasureAggregate) {
	r[key] = value
}

type MeasureAggregate struct {
	Sum   int
	Min   int
	Max   int
	Count int
}
