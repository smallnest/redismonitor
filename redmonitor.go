package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"time"

	xxhashasm "github.com/cespare/xxhash"
)

var (
	src   = flag.String("src", "", "src address of monitoring redis")
	dst   = flag.String("dst", "", "dst address of redis copying data to")
	count = flag.Int("c", 10, "connections")
)

func main() {
	conn, err := net.DialTimeout("tcp", *src, 10*time.Second)
	if err != nil {
		panic(err)
	}

	// handshake
	_, err = conn.Write([]byte("*1\r\n$7\r\nMONITOR\r\n"))
	if err != nil {
		log.Fatalf("can't set MONITOR: %v", err)
	}

	r := bufio.NewReader(conn)
	line, _, err := r.ReadLine()
	if err != nil {
		log.Fatalf("can't read MONITOR response: %v", err)
	}

	if string(line) != "+OK" {
		log.Fatalf("read MONITOR response: %s", line)
	}

	// start workers
	chans := make([]chan [][]byte, *count)
	for i := 0; i < *count; i++ {
		chans[i] = make(chan [][]byte, 1024*1024)
		go startWorker(chans[i])
	}

	var buf bytes.Buffer
	for {
		ln, isPrefix, err := r.ReadLine()
		if err != nil {
			log.Fatalf("can't read line: %v", err)
		}

		buf.Write(ln)
		if isPrefix {
			continue
		}

		data := buf.Bytes()
		line := make([]byte, len(data))
		copy(line, data)
		buf.Reset()
		cmd := bytes.Split(line, []byte{' '})
		if len(cmd) == 0 {
			continue
		}
		if cmd[0][0] != '+' {
			continue
		}
		cmd = cmd[1:]
		for i, f := range cmd {
			cmd[i] = bytes.Trim(f, "\"")
		}

		if string(cmd[0]) == "MONITOR" {
			continue
		}

		var key []byte
		if len(cmd) > 1 { // most case
			key = cmd[1]
		} else {
			key = cmd[0]
		}

		i := xxhashFunc(key) % uint64(*count)
		chans[i] <- cmd
	}
}

func xxhashFunc(key []byte) uint64 {
	x := xxhashasm.New()
	x.Write(key)
	return x.Sum64()
}

func startWorker(ch chan [][]byte) {
start:
	conn, err := net.DialTimeout("tcp", *dst, 10*time.Second)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			_, err := io.Copy(ioutil.Discard, conn)
			if err != nil {
				return
			}
		}
	}()
	w := NewRESPWriter(conn)
	for s := range ch {
		err := w.WriteCommand(s...)
		if err != nil {
			fmt.Printf("failed to write: %v", err)
			goto start
		}
	}
}
