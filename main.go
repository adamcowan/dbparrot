package main

import (
    "fmt"
    "log"
    "net"
    "os"
    "strconv"
)

func main() {
    // TODO: command-line args:
    // port
    // target server
    // strip auth?
    // cache ttl

    port := 27017
    if len(os.Args) > 1 {
        p, err := strconv.Atoi(os.Args[1])
        if err != nil {
            fmt.Println(err)
        } else {
            port = p
        }
    }

    backend := &MongoBackend{"localhost", port}

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
