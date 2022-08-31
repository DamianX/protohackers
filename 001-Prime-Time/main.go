package main

import (
	"bufio"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/buger/jsonparser"
)

var trueResponse []byte = append([]byte(`{"method":"isPrime","prime":true}`), byte('\n'))
var falseResponse []byte = append([]byte(`{"method":"isPrime","prime":false}`), byte('\n'))

func main() {
	const port = 8000
	listen, err := net.Listen("tcp4", fmt.Sprint(":", port))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	defer listen.Close()
	fmt.Println(fmt.Sprint("Listening on port ", port))
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(time.Second * 5))

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		conn.SetDeadline(time.Now().Add(time.Second * 5))
		line := scanner.Text()
		log.Print("Received: ", line)
		err := handleRequest(line, conn)
		if err != nil {
			log.Print(err)
			conn.Close()
			return
		}
	}

	if err := scanner.Err(); err != nil {
		log.Print("error reading socket:", err)
		conn.Close()
		return
	}
}

func handleRequest(jayson string, conn net.Conn) error {
	method, err := jsonparser.GetUnsafeString([]byte(jayson), "method")
	if err != nil || method != "isPrime" {
		log.Print("Potentially malformed json received: ")
		conn.Write([]byte(jayson))
		conn.Close()
		return err
	}

	numberInt64, err := jsonparser.GetInt([]byte(jayson), "number")
	if err != nil {
		numberFloat64, floatErr := jsonparser.GetFloat([]byte(jayson), "number")
		if floatErr != nil {
			log.Print("Potentially malformed json received.")
			conn.Write([]byte(jayson))
			conn.Close()
			return floatErr
		}
		numberInt64 = int64(numberFloat64)
	}

	if big.NewInt(numberInt64).ProbablyPrime(20) {
		conn.Write(trueResponse)
	} else {
		conn.Write(falseResponse)
	}
	return nil
}
