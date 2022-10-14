package proxy

import (
	"bytes"
	"fmt"
	"go-mysql-proxy/protocol"
	"io"
	"log"
	"net"
)

func NewConnection(host string, port string, conn net.Conn, id uint64, enableDecoding bool) *Connection {
	return &Connection{
		host:           host,
		port:           port,
		conn:           conn,
		id:             id,
		enableDecoding: enableDecoding,
	}
}

type Connection struct {
	id             uint64
	conn           net.Conn
	host           string
	port           string
	enableDecoding bool
}

func (r *Connection) Handle() error {
	address := fmt.Sprintf("%s%s", r.host, r.port)
	mysql, err := net.Dial("tcp", address)
	if err != nil {
		log.Printf("Failed to connection to MySQL: [%d] %s", r.id, err.Error())
		return err
	}

	if !r.enableDecoding {
		// client to server

		go func() {
			copied, err := io.Copy(mysql, r.conn)
			if err != nil {
				log.Printf("Conection error: [%d] %s", r.id, err.Error())
			}

			log.Printf("Connection closed. Bytes copied: [%d] %d", r.id, copied)
		}()

		copied, err := io.Copy(r.conn, mysql)
		if err != nil {
			log.Printf("Connection error: [%d] %s", r.id, err.Error())
		}

		log.Printf("Connection closed. Bytes copied: [%d] %d", r.id, copied)

		return nil
	}

	handshakePacket := &protocol.InitialHandshakePacket{}
	err = handshakePacket.Decode(mysql)

	//fmt.Printf("salt:: %v", handshakePacket.AuthPluginData)

	if err != nil {
		log.Printf("Failed ot decode handshake initial packet: %s", err.Error())
		return err
	}

	//fmt.Printf("InitialHandshakePacket:\n%s\n", handshakePacket)

	res, _ := handshakePacket.Encode()

	written, err := r.conn.Write(res)
	if err != nil {
		log.Printf("Failed to write %d: %s", written, err.Error())
		return err
	}

	authorizationPacket := &protocol.AuthorizationPacket{}
	err = authorizationPacket.Decode(r.conn)

	res, _ = authorizationPacket.Encode(handshakePacket.AuthPluginData)

	written, err = mysql.Write(res)

	//go io.Copy(mysql, r.conn)
	//
	//io.Copy(r.conn, mysql)
	var b bytes.Buffer
	// Copy bytes from client to server and requestParser
	go io.Copy(io.MultiWriter(mysql, &Request{b}), r.conn)

	// Copy bytes from server to client and responseParser
	var b1 bytes.Buffer
	io.Copy(io.MultiWriter(r.conn, &Response{b1}), mysql)

	return nil
}

type Request struct {
	buf bytes.Buffer
}

type Response struct {
	buf bytes.Buffer
}

func (req *Request) Write(packet []byte) (n int, err error) {

	if len(packet) < 6 {
		fmt.Printf("\nПроизошёл выход.\n")
		return len(packet), nil
	}

	if packet[4] != 3 {
		req.buf.Write(packet[5:])
	} else {
		req.buf.Write(packet[7:])
	}

	fmt.Printf("\nКоманда: %v", &req.buf)

	//fmt.Printf("\np= %v", packet)

	req.buf.Reset()
	return len(packet), nil
}

func (res *Response) Write(packet []byte) (n int, err error) {

	res.buf.Write(packet[7:])

	if packet[4] != 0xff {
		fmt.Printf("\nКоманда успешно выполнилась.")
		res.buf.Reset()
		return len(packet), nil
	}
	if len(packet) > 11 {
		fmt.Printf("\nОтвет от сервера: %v\n", &res.buf)
		res.buf.Reset()
	}

	return len(packet), nil
}
