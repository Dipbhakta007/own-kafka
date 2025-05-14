package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	var header []byte = make([]byte, 12)
	_, err = conn.Read(header)

	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		os.Exit(1)
	}

	body := make([]byte, 0)
	body = append(body, 0x00, 0x00) //error code
	body = append(body, 0x02)       //compact array length
	body = append(body, 0x00, 0x12,
		0x00, 0x00,
		0x00, 0x04) //compact array item
	body = append(body, 0x00, 0x00,
		0x00, 0x00) //throttle time ms
	body = append(body, 0x00) //tagged fields

	correlation_id := binary.BigEndian.Uint32(header[8:12])

	headerBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBuf, correlation_id)
	fullResPonse := append(headerBuf, body...)

	final := make([]byte, 4)
	binary.BigEndian.PutUint32(final, uint32(len(fullResPonse)))
	final = append(final, fullResPonse...)

	conn.Write(final)

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		fmt.Println("Connection is a TCP connection!")
		tcpConn.CloseWrite()
	}

}
