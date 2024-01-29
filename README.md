# 1 Billion Row Challenge

Tests in Go to be as performant as possible in the [1brc](https://github.com/gunnarmorling/1brc)

The actual execution time is hihgly input dependent. My main test file had 10k station names with each station name exactly 10characters long. This creates a roughly 16GB file.
But doing only 500 stations with names of random length `[2, 15]` while creating a slightly smaller file resulted in about 20% more runtime. Using random names of length `[2, 100]` gave me a 55GB file which ... took much longer to process!

## Results

I ran the test with 2 different HashMaps: the go standard library hashmap, and [Swiss Map](https://github.com/dolthub/swiss) and got the following results for 1B rows:

| test           | runtime |
|----------------|---------|
| go std hashmap | ~5.85s  |
| swiss map      | ~4.35s  |

### Running on

This was running on a 2021 M1 Max Macbook Pro with 10 vCPUs


### General approach

1. The file is memory mapped into a giant `[]byte` slice
2. Start a goroutine that sends `[2]int` `[begin,end]` arrays on a channel. Each slice is `os.Getpagesize()` smaller sizes perform worse, larger ones don't improve much
3. Start `runtime.NumCPU()` worker coroutines
4. Each coroutine creates its own `map[string]*AggregateMetric` to aggregate its local results. This means that there's no concurrent accesses to each store of results.
5. Then pulls a `[2]int` off the chunks channel and starts working on that
6. The workers read directly from the giant `[]byte` slice at various offsets. Well, they start from one place and then move sequentially from there until the end of their chunk.

Other interesting things:
1. use `unsafe.String` to get the station name instead of casting to a string to avoid a copy
2. since each metric is guaranteed to have exactly 1 digit, we multiply the metric by 10 which allows us to work only with `int` until the very end when it's time to display the results
