package proxy

import (
	"errors"
	"fmt"
	"net"

	"github.com/jackc/pgproto3/v2"
)

type Frontend struct {
	frontend   *pgproto3.Frontend
	conn       net.Conn
}

func NewFrontend(conn net.Conn) *Frontend {
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	return &Frontend{
		frontend: frontend,
		conn:     conn,
	}
}

func (f *Frontend) Run() error {
	defer f.Close()

	startupMessage := &pgproto3.StartupMessage{
		ProtocolVersion: 0x30000,
		Parameters: map[string]string{
			"application_name": "pgproxy",
			"client_encoding": "UTF8",
			"database": "postgres",
			"user": "postgres",
		},
	}

	err := f.frontend.Send(startupMessage)
	if err != nil {
		return err
	}

	msg, err := f.frontend.Receive()
	if err != nil {
		return err
	}
	fmt.Printf("%#v", msg)

	switch msg.(type) {
	case *pgproto3.AuthenticationSASL:
		f.authenticateSASL()
	default:
		return errors.New("unsupported authentication method")
	}

	return nil
}

func (f *Frontend) authenticateSASL() error {
	err := f.frontend.Send(&pgproto3.SASLInitialResponse{
		AuthMechanism: "SCRAM-SHA-256", // TODO: verify server offered this, use -PLUS if possible
		Data: []byte(`n,,,r=fyko+d2lbbFgONRv9qkxdawL`), // TODO: generate real nonce
	})
	if err != nil {
		return err
	}

	msg, err := f.frontend.Receive()
	if err != nil {
		return err
	}
	switch msg.(type) {
	case *pgproto3.AuthenticationSASLContinue:

	}
	fmt.Printf("%#v", msg)

	return nil
}

func (f *Frontend) Close() error {
	return f.conn.Close()
}
