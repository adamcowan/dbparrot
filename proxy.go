package main

import (
    "runtime/debug"
    "fmt"
    "log"
    "net"
)

type Proxy struct {
    Conn      net.Conn
    Closing   chan int
}

func handleProxy(client net.Conn, backend *MongoBackend) {
    closing := make(chan int, 1)
    proxy := &Proxy{client, closing}
    proxy.Closing <- 1

    server, err := backend.Connect()
    if err != nil {
        debug.PrintStack()
        log.Fatal(err)
    }

    go func() {
        clientStream, clientError := mongoStream(client)
        serverStream, serverError := mongoStream(server)
        defer func() {
            client.Close()
            server.Close()
            close(clientStream)
            close(clientError)
            close(serverStream)
            close(serverError)
        }()
        for {
            select {
                case cmd := <-serverStream:
                    fmt.Printf("server: ")
                    cmd.Print()
                    switch cmd.OpCode {
                        case OP_REPLY:
                            CachePut(cmd.ResponseTo, cmd)
                    }
                    cmd.Send(client)

                case err := <-serverError:
                    fmt.Printf("server error: %s\n", err)
                    break

                case cmd := <-clientStream:
                    fmt.Printf("client: ")
                    cmd.Print()
                    switch cmd.OpCode {
                        case OP_UPDATE:
                            fmt.Println("Ignoring UPDATE")
                            continue
                        case OP_INSERT:
                            fmt.Println("Ignoring INSERT")
                            continue
                        case OP_GET_MORE:
                            fallthrough
                        case OP_QUERY:
                            result := CacheGet(cmd.Payload)
                            if result != nil {
                                response := *result
                                response.ResponseTo = cmd.RequestID

                                fmt.Printf("cache hit: ")
                                response.Print()
                                response.Send(client)
                                continue
                            } else {
                                CachePrep(cmd.RequestID, cmd.Payload)
                            }
                    }
                    cmd.Send(server)

                case err := <-clientError:
                    fmt.Printf("client error: %s\n", err)
                    break
            }
        }
    }()
}

func (p *Proxy) Close() {
    if <-p.Closing == 1 {
        p.Conn.Close()
    }
    p.Closing <- 0
}
