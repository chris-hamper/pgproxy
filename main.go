package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v4"

	"github.com/chris-hamper/pgproxy/pkg/proxy"
)

var options struct {
	listenAddress   string
	responseCommand string
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:15432", "Listen address")
	flag.Parse()

	// clientConn, err := net.Dial("tcp", "localhost:5432")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println("Client connection opened")

	// f := proxy.NewFrontend(clientConn)
	// err = f.Run()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	c, err := pgx.Connect(context.TODO(), "postgres://postgres:password@localhost:5432")
	c.PgConn().Conn()

	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		b := proxy.NewBackend(conn)
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
