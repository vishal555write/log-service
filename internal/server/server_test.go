// package server

// import (
// 	"context"
// 	"io/ioutil"
// 	"net"
// 	"testing"

// 	"github.com/stretchr/testify/require"
// 	api "github.com/vishal555write/proglog/api/v1"
// 	"github.com/vishal555write/proglog/internal/log"
// 	"google.golang.org/grpc"
// )
// func TestServer(t *testing.T) {
// for scenario, fn := range map[string]func(
// t *testing.T,
// client api.LogClient,
// config *Config,
// ){
// "produce/consume a message to/from the log succeeeds":
// testProduceConsume,
// "produce/consume stream succeeds":
// testProduceConsumeStream,
// "consume past log boundary fails":
// testConsumePastBoundary,
// } {
// t.Run(scenario, func(t *testing.T) {
// client, config, teardown := setupTest(t, nil)
// defer teardown()
// fn(t, client, config)
// })
// }
// }

// func setupTest(t *testing.T, fn func(*Config)) (
// client api.LogClient,
// cfg *Config,
// teardown func(),
// ) {
// t.Helper()
// l, err := net.Listen("tcp", ":0")
// require.NoError(t, err)
// clientOptions := []grpc.DialOption{grpc.WithInsecure()}
// cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
// require.NoError(t, err)
// dir, err := ioutil.TempDir("", "server-test")
// require.NoError(t, err)
// clog, err := log.NewLog(dir, log.Config{})
// require.NoError(t, err)
// cfg = &Config{
// CommitLog: clog,
// }
// if fn != nil {
// fn(cfg)
// }
// server, err := NewGRPCServer(cfg)
// require.NoError(t, err)
// go func() {
// server.Serve(l)
// }()
// client = api.NewLogClient(cc)
// return client, cfg, func() {
// server.Stop()
// cc.Close()
// l.Close()
// clog.Remove()
// }
// }
// func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
// ctx := context.Background()

// want := &api.Record{
// Value: []byte("hello world"),
// }
// produce, err := client.Produce(
// ctx,
// &api.ProduceRequest{
// Record: want,
// },
// )
// require.NoError(t, err)
// consume, err := client.Consume(ctx, &api.ConsumeRequest{
// Offset: produce.Offset,
// })
// require.NoError(t, err)
// require.Equal(t, want.Value, consume.Record.Value)
// require.Equal(t, want.Offset, consume.Record.Offset)
// }
// func testConsumePastBoundary(
// t *testing.T,
// client api.LogClient,
// config *Config,
// ) {
// ctx := context.Background()
// produce, err := client.Produce(ctx, &api.ProduceRequest{
// Record: &api.Record{
// Value: []byte("hello world"),
// },
// })
// require.NoError(t, err)
// consume, err := client.Consume(ctx, &api.ConsumeRequest{
// Offset: produce.Offset + 1,
// })
// if consume != nil {
// t.Fatal("consume not nil")
// }
// got := grpc.Code(err)
// want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
// if got != want {
// t.Fatalf("got err: %v, want: %v", got, want)
// }
// }

// func testProduceConsumeStream(
// t *testing.T,
// client api.LogClient,
// config *Config,
// ) {
// ctx := context.Background()
// records := []*api.Record{{
// Value: []byte("first message"),
// Offset: 0,
// }, {
// Value: []byte("second message"),
// Offset: 1,
// }}
// {
// }
// {
// stream, err := client.ProduceStream(ctx)
// require.NoError(t, err)
// for offset, record := range records {
// err = stream.Send(&api.ProduceRequest{
// Record: record,
// })
// require.NoError(t, err)
// res, err := stream.Recv()
// require.NoError(t, err)
// if res.Offset != uint64(offset) {
// t.Fatalf(
// "got offset: %d, want: %d",
// res.Offset,
// offset,
// )
// }
// }
// stream, err = client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0},)
// require.NoError(t, err)
// for i, record := range records {
// res, err := stream.Recv()

// require.NoError(t, err)
// require.Equal(t, res.Record, &api.Record{
// Value: record.Value,
// Offset: uint64(i),
// })
// }
// }
// }
//////////----------------------------------------////////////////
// package server

// import (
// 	"context"
// 	"io/ioutil"
// 	"net"
// 	"testing"

// 	"github.com/stretchr/testify/require"
// 	api "github.com/vishal555write/proglog/api/v1"
// 	"github.com/vishal555write/proglog/internal/log"
// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/codes"
// 	"google.golang.org/grpc/status"
// )

// func TestServer(t *testing.T) {
// 	for scenario, fn := range map[string]func(
// 		t *testing.T,
// 		client api.LogClient,
// 		config *Config,
// 	){
// 		"produce/consume a message to/from the log succeeds": testProduceConsume,
// 		"produce/consume stream succeeds":                     testProduceConsumeStream,
// 		"consume past log boundary fails":                     testConsumePastBoundary,
// 	} {
// 		t.Run(scenario, func(t *testing.T) {
// 			client, config, teardown := setupTest(t, nil)
// 			defer teardown()
// 			fn(t, client, config)
// 		})
// 	}
// }

// func setupTest(t *testing.T, fn func(*Config)) (
// 	client api.LogClient,
// 	cfg *Config,
// 	teardown func(),
// ) {
// 	t.Helper()

// 	// 1) Listen
// 	l, err := net.Listen("tcp", ":0")
// 	require.NoError(t, err)

// 	// 2) Temp dir & log (commit log implementation)
// 	dir, err := ioutil.TempDir("", "server-test")
// 	require.NoError(t, err)
// 	clog, err := log.NewLog(dir, log.Config{})
// 	require.NoError(t, err)

// 	// 3) Server config and start server BEFORE dialing the client
// 	cfg = &Config{
// 		CommitLog: clog,
// 	}
// 	if fn != nil {
// 		fn(cfg)
// 	}
// 	server, err := NewGRPCServer(cfg)
// 	require.NoError(t, err)

// 	go func() {
// 		// Serve will block; run it in a goroutine
// 		_ = server.Serve(l)
// 	}()

// 	// 4) Dial the server (client connection)
// 	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
// 	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
// 	require.NoError(t, err)

// 	client = api.NewLogClient(cc)

// 	return client, cfg, func() {
// 		server.Stop()
// 		cc.Close()
// 		l.Close()
// 		clog.Remove()
// 	}
// }

// func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
// 	ctx := context.Background()

// 	want := &api.Record{
// 		Value: []byte("hello world"),
// 	}

// 	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
// 	require.NoError(t, err)

// 	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
// 	require.NoError(t, err)

// 	require.Equal(t, want.Value, consume.Record.Value)
// 	// Compare produced offset to consumed offset (want.Offset was not set)
// 	require.Equal(t, produce.Offset, consume.Record.Offset)
// }

// func testConsumePastBoundary(
// 	t *testing.T,
// 	client api.LogClient,
// 	config *Config,
// ) {
// 	ctx := context.Background()
// 	produce, err := client.Produce(ctx, &api.ProduceRequest{
// 		Record: &api.Record{
// 			Value: []byte("hello world"),
// 		},
// 	})
// 	require.NoError(t, err)

// 	consume, err := client.Consume(ctx, &api.ConsumeRequest{
// 		Offset: produce.Offset + 1,
// 	})
// 	if consume != nil {
// 		t.Fatal("consume not nil")
// 	}

// 	// Use status.Code to check gRPC error codes (more robust & modern)
// 	if status.Code(err) != codes.OutOfRange {
// 		t.Fatalf("got err: %v, want code: %v", status.Code(err), codes.OutOfRange)
// 	}
// }

// func testProduceConsumeStream(
//     t *testing.T,
//     client api.LogClient,
//     config *Config,
// ) {
//     ctx := context.Background()

//     // Records to send (no offsets)
//     values := [][]byte{
//         []byte("first message"),
//         []byte("second message"),
//     }

//     // --- Produce Stream ---
//     produceStream, err := client.ProduceStream(ctx)
//     require.NoError(t, err)

//     for i, value := range values {
//         // Send record
//         err = produceStream.Send(&api.ProduceRequest{
//             Record: &api.Record{Value: value},
//         })
//         require.NoError(t, err)

//         // Receive response
//         res, err := produceStream.Recv()
//         require.NoError(t, err)

//         // Offset must match incrementally
//         require.Equal(t, uint64(i), res.Offset)
//     }

//     // --- Consume Stream ---
//     consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
//     require.NoError(t, err)

//     for i, value := range values {
//         res, err := consumeStream.Recv()
//         require.NoError(t, err)

//         // Compare record fields individually
//         require.Equal(t, value, res.Record.Value)
//         require.Equal(t, uint64(i), res.Record.Offset)
//     }
// }

package server

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/vishal555write/proglog/internal/config"
	"google.golang.org/grpc/credentials"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "github.com/vishal555write/proglog/api/v1"
	"github.com/vishal555write/proglog/internal/auth"
	"github.com/vishal555write/proglog/internal/log"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		// ...
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"unauthorized fails": testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient,
				nobodyClient,
				config,
				teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}



func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
	}
}


func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}


func testConsumePastBoundary(
	t *testing.T,
	client, _ api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}


func testProduceConsumeStream(
	t *testing.T,
	client, _ api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}

	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}


func testUnauthorized(
	t *testing.T,
	_,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}


func setupTest1(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile: config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(
		l.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
	)
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
	}
}

func setupTest2(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		CAFile: config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(
		l.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
	)
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server: true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
	}
}
