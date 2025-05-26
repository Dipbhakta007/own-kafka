package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var messageSize []byte = make([]byte, 4)
		_, err := io.ReadFull(conn, messageSize)
		if err != nil {
			fmt.Println("Connection closed")
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				fmt.Println("Connection is a TCP connection!")
				tcpConn.CloseWrite()
			}
			break
		}
		messageSizeInt := binary.BigEndian.Uint32(messageSize)
		var reqMessage []byte = make([]byte, messageSizeInt)

		_, err = io.ReadFull(conn, reqMessage)
		if err != nil {
			fmt.Println("Error reading request message")
			break
		}
		requestApiVersion := binary.BigEndian.Uint16(reqMessage[2:4])
		correlationId := binary.BigEndian.Uint32(reqMessage[4:8])

		response := buildRespose(requestApiVersion, correlationId)

		conn.Write(response)

	}
}

func buildRespose(requestApiVersion uint16, correlationId uint32) []byte {
	body := make([]byte, 0)
	if requestApiVersion > 4 {
		body = append(body, 0x00, 0x23) //error code
	} else {
		body = append(body, 0x00, 0x00) //error code
		body = append(body, 0x03)       //compact array length
		body = append(body,
			0x00, 0x12,
			0x00, 0x00,
			0x00, 0x04,
			0x00) //compact array item
		body = append(body,
			0x00, 0x4b,
			0x00, 0x00,
			0x00, 0x00,
			0x00) //compact array item
		body = append(body, 0x00, 0x00,
			0x00, 0x00) //throttle time ms
		body = append(body, 0x00) //tagged fields
	}

	headerBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBuf, correlationId)
	fullResPonse := append(headerBuf, body...)

	final := make([]byte, 4)
	binary.BigEndian.PutUint32(final, uint32(len(fullResPonse)))
	final = append(final, fullResPonse...)
	return final
}

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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}

}
