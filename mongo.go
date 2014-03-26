package main

import (
    // "labix.org/v2/mgo/bson"
    "bufio"
    "bytes"
    "fmt"
    "log"
    "net"
    "runtime/debug"
)

const (
    OP_REPLY        = 1
    OP_MSG          = 1000
    OP_UPDATE       = 2001
    OP_INSERT       = 2002
    OP_QUERY        = 2004
    OP_GET_MORE     = 2005
    OP_DELETE       = 2006
    OP_KILL_CURSORS = 2007
)

var opCodes map[int]string = map[int]string{
    OP_REPLY:        "OP_REPLY",
    OP_MSG:          "OP_MSG",
    OP_UPDATE:       "OP_UPDATE",
    OP_INSERT:       "OP_INSERT",
    2003:            "RESERVED",
    OP_QUERY:        "OP_QUERY",
    OP_GET_MORE:     "OP_GET_MORE",
    OP_DELETE:       "OP_DELETE",
    OP_KILL_CURSORS: "OP_KILL_CURSORS",
}

type MongoBackend struct {
    Host string
    Port int
}

func (b *MongoBackend) Connect() (net.Conn, error) {
    conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", b.Host, b.Port))
    return conn, err
}

type MongoCmd struct {
    MessageLength int
    RequestID     int
    ResponseTo    int
    OpCode        int
    Payload       []byte
}

func (c *MongoCmd) Send(conn net.Conn) {
    buf := make([]byte, 16)
    packInt32(buf[0:4],   c.MessageLength)
    packInt32(buf[4:8],   c.RequestID)
    packInt32(buf[8:12],  c.ResponseTo)
    packInt32(buf[12:16], c.OpCode)

    _, err := conn.Write(buf)
    if err != nil {
        debug.PrintStack()
        log.Fatal(err)
    }
    if c.Payload != nil {
        _, err = conn.Write(c.Payload)
        if err != nil {
            debug.PrintStack()
            log.Fatal(err)
        }
    }
}

func (c *MongoCmd) Print() {
    fmt.Printf("CMD(op=")
    if opstr, ok := opCodes[c.OpCode]; ok {
        fmt.Printf("%s, ", opstr)
    } else {
        fmt.Printf("%d, ", c.OpCode)
    }
    fmt.Printf("len=%d, req=%d, to=%d)\n",
               c.MessageLength,
               c.RequestID, c.ResponseTo)
}

func (c *MongoCmd) Selector() []byte {
    switch c.OpCode {
        case OP_UPDATE:
            b := c.Payload[4:]
            b = bytes.SplitN(b, []byte{0}, 2)[1]
            length := unpackInt32(b[4:8])
            doc := b[length:]
            return doc
        case OP_INSERT:
            b := c.Payload[4:]
            b = bytes.SplitN(b, []byte{0}, 2)[1]
            length := unpackInt32(b[4:8])
            // TODO: could be more than one document in a row
            doc := b[length:]
            return doc
        case OP_QUERY:
            b := c.Payload[4:]
            b = bytes.SplitN(b, []byte{0}, 2)[1]
            length := unpackInt32(b[8:12])
            doc := b[8+length:]
            return doc
    }
    return nil
}

func parseCmd(header []byte) *MongoCmd {
    messageLength := unpackInt32(header[0:4])
    requestId := unpackInt32(header[4:8])
    responseTo := unpackInt32(header[8:12])
    opCode := unpackInt32(header[12:16])
    return &MongoCmd{messageLength, requestId, responseTo, opCode, nil}
}

func mongoStream(conn net.Conn) (chan *MongoCmd, chan error) {
    buf := make([]byte, 16)
    stream := make(chan *MongoCmd)
    errchan := make(chan error)
    go func() {
        reader := bufio.NewReader(conn)
        for {
            _, err := reader.Read(buf)
            if err != nil {
                errchan <- err
                return
            }

            cmd := parseCmd(buf)
            length := cmd.MessageLength - 16
            if length > 16 {
                payload := make([]byte, length)
                _, err := reader.Read(payload)
                if err != nil {
                    debug.PrintStack()
                    log.Fatal(err)
                }
                cmd.Payload = payload
            }
            stream <- cmd
        }
    }()
    return stream, errchan
}
