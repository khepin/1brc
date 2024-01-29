package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
	"unsafe"
)

const numlines = 1_000_000_000
const numstations = 500

func genfile() {
	rand.NewSource(time.Now().UnixNano())
	stations := []string{}
	for i := 0; i < numstations; i++ {
		stations = append(stations, generate(1+rand.Intn(15)))
		// stations = append(stations, generate(10))
	}
	sb := strings.Builder{}
	for i := 0; i < numlines; i++ {
		station := stations[rand.Intn(len(stations))]
		sb.WriteString(station)
		sb.WriteString(";")
		sb.WriteString(fmt.Sprint(rand.Intn(198) - 99))
		sb.WriteString(".")
		sb.WriteString(fmt.Sprint(rand.Intn(9)))
		sb.WriteString("\n")

		if i%1_000_000 == 0 {
			fmt.Print(sb.String())
			sb.Reset()
		}
	}
	fmt.Print(sb.String())
}
func generate(size int) string {
	b := make([]byte, size)
	rand.Read(b)
	for i := 0; i < size; i++ {
		b[i] = alphabet[b[i]%byte(len(alphabet))]
	}
	return *(*string)(unsafe.Pointer(&b))
}

var alphabet = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
