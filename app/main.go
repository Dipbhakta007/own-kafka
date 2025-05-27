package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit

const (
	API_VERSIONS        = 18
	DESCRIBE_PARTITIONS = 75
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		messageSize := make([]byte, 4)
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
		reqMessage := make([]byte, messageSizeInt)
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
	cilentIdLength := binary.BigEndian.Uint16(topicStream[0:2])
	fmt.Println(int(cilentIdLength))

	idx := int(cilentIdLength + 2)
	idx++ // tag buffer

	fmt.Println(idx)
	fmt.Println(topicStream[idx])

	topicNum := int(topicStream[idx]) - 1
	idx++

	var topics []string
	fmt.Println("Topic numbers")
	fmt.Println(topicNum)

	for range topicNum {
		topicNameLen := int(topicStream[idx]) - 1
		idx++
		topicName := string(topicStream[idx : idx+topicNameLen])
		fmt.Println("Topic Found")
		fmt.Println(topicName)
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
	binary.BigEndian.PutUint32(final, uint32(len(fullResPonse)))
	final = append(final, fullResPonse...)
	return final
}

func buildDescribePartitionsResponse(topics []string, correlationId uint32) []byte {
	body := make([]byte, 0)

	body = append(body, 0x00) // Tagged buffer
	body = append(body,
		0x00, 0x00,
		0x00, 0x00) // Throttle time ms

	body = append(body, byte(len(topics)+1))
	for _, topicName := range topics {
		body = append(body, 0x00, 0x03)             // Error code
		body = append(body, byte(len(topicName)+1)) // Topic name length
		body = append(body, topicName...)           // Topic name
		body = append(body, make([]byte, 16)...)    // Topic ID
		body = append(body, 0x00)                   // Is internal
		body = append(body, 0x01)                   // No partitions
		body = append(body,
			0x00, 0x00,
			0x0d, 0xf8) // Authorized operations
		body = append(body, 0x00) // Tagged Fields
	}

	body = append(body, 0xff) // Next cursor (null)
	body = append(body, 0x00) // Tagged Fields

	return buildFullResponse(body, correlationId)
}

func buildApiVersionResponse(requestApiVersion uint16, correlationId uint32) []byte {
	body := make([]byte, 0)

	if requestApiVersion > 4 {
		body = append(body, 0x00, 0x23) // Error code
	} else {
		body = append(body, 0x00, 0x00) // Error code
		body = append(body, 0x03)       // Compact array length

		body = append(body,
			0x00, 0x12,
			0x00, 0x00,
			0x00, 0x04,
			0x00) // Compact array item

		body = append(body,
			0x00, 0x4b,
			0x00, 0x00,
			0x00, 0x00,
			0x00) // Compact array item

		body = append(body, 0x00, 0x00,
			0x00, 0x00) // Throttle time ms

		body = append(body, 0x00) // Tagged fields
	}

	return buildFullResponse(body, correlationId)
}

func main() {
	fmt.Println("Logs from your program will appear here!")

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
