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

const (
	API_VERSIONS        = 18
	DESCRIBE_PARTITIONS = 75
)

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
		requestApiKey := binary.BigEndian.Uint16(reqMessage[0:2])
		requestApiVersion := binary.BigEndian.Uint16(reqMessage[2:4])
		correlationId := binary.BigEndian.Uint32(reqMessage[4:8])

		var response []byte

		switch requestApiKey {
		case API_VERSIONS:
			response = buildApiVersionResponse(requestApiVersion, correlationId)
		case DESCRIBE_PARTITIONS:
			topics := extractTopicNames(reqMessage[8:])
			response = buildDescribePartitionsResponse(topics, correlationId)
		default:
			fmt.Printf("Unhandled API Key: %d\n", requestApiKey)
			continue
		}

		conn.Write(response)

	}
}

func extractTopicNames(topicStream []byte) []string {
	idx := 0
	topicNum := int(topicStream[idx]) - 1
	idx++

	var topics = make([]string, 0)

	for range topicNum {
		topicNameLen := int(topicStream[idx]) - 1
		idx++
		topicName := string(topicStream[idx : idx+topicNameLen])
		topics = append(topics, topicName)
		idx += 16
		partitionsLen := int(topicStream[idx]) - 1
		idx += 4 * partitionsLen
		idx++
	}
	return topics

}

func buildFullResponse(body []byte, correlationId uint32) []byte {
	headerBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBuf, correlationId)
	fullResPonse := append(headerBuf, body...)

	final := make([]byte, 4)
	fmt.Println(len(fullResPonse))
	binary.BigEndian.PutUint32(final, uint32(len(fullResPonse)))
	final = append(final, fullResPonse...)
	return final
}

func buildDescribePartitionsResponse(topics []string, correlationId uint32) []byte {
	var body = make([]byte, 0)
	body = append(body, 0x00, 0x00,
		0x00, 0x00) //throttle time ms
	body = append(body, byte(len(topics)+1))
	for _, topicName := range topics {
		body = append(body, byte(len(topicName)+1)) // topicName
		body = append(body, topicName...)           // topicName
		body = append(body, (make([]byte, 16))...)  //topic ID
		body = append(body, 0x00, 0x03)             // Error code
		body = append(body, 0x01)                   // No Partitions
		body = append(body, 0x00)                   // Tagged Fields
	}
	body = append(body, 0x00) // Tagged Fields

	return buildFullResponse(body, correlationId)
}

func buildApiVersionResponse(requestApiVersion uint16, correlationId uint32) []byte {
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
	return buildFullResponse(body, correlationId)
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
