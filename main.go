package main

import (
    "log"
    "net"
)

func main() {
    // TODO: command-line args:
    // port
    // target server
    // strip auth?
    // cache ttl

    backend := &MongoBackend{"localhost", 27017}

    server, err := net.Listen("tcp", ":30001")
    if err != nil {
        log.Fatal(err)
    }

    for {
        conn, err := server.Accept()
        if err != nil {
            log.Fatal(err)
        }
        go handleProxy(conn, backend)
    }
}
