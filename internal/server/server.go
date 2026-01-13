// package server

// import (
// 	"context"

// 	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
// 	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

// 	api "github.com/vishal555write/proglog/api/v1"
// 	"google.golang.org/grpc"
// )

// type Config struct{
// 	CommitLog CommitLog
// }

// var _ api.LogServer = (*grpcServer)(nil) //struct grpcServer have to implement all methord in LogServer interface

// type grpcServer struct{
// 	api.UnimplementedLogServer  //struct
// 	*Config
// }

// // func NewGRPCServer(config *Config) (*grpc.Server, error) {
// // gsrv := grpc.NewServer()
// // srv, err := newgrpcServer(config)
// // if err != nil {
// // return nil, err
// // }

// func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
// 	*grpc.Server,
// 	error,
// ) {
// 	opts = append(opts, grpc.StreamInterceptor(
// 		grpc_middleware.ChainStreamServer(
// 			grpc_auth.StreamServerInterceptor(authenticate),
// 		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
// 		grpc_auth.UnaryServerInterceptor(authenticate),
// 	)))
// 	gsrv := grpc.NewServer(opts...)
// 	srv, err := newgrpcServer(config)
// 	if err != nil {
// 		return nil, err
// 	}
// 	api.RegisterLogServer(gsrv, srv)
// 	return gsrv, nil
// }

// func newgrpcServer(config *Config) (srv *grpcServer,err error){
// 	srv= &grpcServer{
// 		Config: config,
// 	}
// 	return srv,nil
// }

// func(s *grpcServer) Produce(ctx context.Context,req *api.ProduceRequest)(*api.ProduceResponse,error){
// 	offset,err:=s.CommitLog.Append(req.Record)
// 	if err!=nil{return nil,err}
// 	return &api.ProduceResponse{Offset: offset},nil
// }

// func(s *grpcServer) Consume(ctx context.Context,req *api.ConsumeRequest)(*api.ConsumeResponse,error){
// 	record,err:=s.CommitLog.Read(req.Offset)
// 	if err!=nil{
// 		return nil,err
// 	}
// 	return &api.ConsumeResponse{Record: record},nil
// }

// func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer)error{
// 	for{
// 		req,err:=stream.Recv()
// 		if err!=nil{
// 			return err
// 		}
// 		res,err:=s.Produce(stream.Context(),req)
// 		if err!=nil{return err}
// 		if err=stream.Send(res);err!=nil{
// 			return err
// 		}
// 	}

// }

// // func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest,stream api.Log_ConsumeStreamServer)error{
// // 	for{
// // 		select{
// // 		case <-stream.Context().Done():   //client disconnect,grpc deadline/timeout expires,server crash processing,parent context is cancelled
// // 			return nil          //Without this case, your server might keep sending messages to a client that has already disconnected → memory leaks, goroutine leaks, wasted processing.
// // 		default:
// // 			res,err:=s.Consume(stream.Context(),req)
// // 			switch err.(type){
// // 			case nil:
// // 			case api.ErrOffsetOutOfRange:
// // 				continue
// // 			default:
// // 				return err
// // 			}
// // 			if err=stream.Send(res);err!=nil{
// // 				return err
// // 			}
// // 			req.Offset++

// // 		}
// // 	}
// // }
// func (s *grpcServer) ConsumeStream(
//     req *api.ConsumeRequest,
//     stream api.Log_ConsumeStreamServer,
// ) error {
//     for {
//         select {
//         case <-stream.Context().Done():
//             // Client disconnected or deadline exceeded
//             return nil

//         default:
//             // Read the next record
//             res, err := s.Consume(stream.Context(), req)
//             switch err.(type) {
//             case nil:
//                 // OK
//             case api.ErrOffsetOutOfRange:
//                 // No new records yet — keep waiting
//                 continue
//             default:
//                 return err
//             }

//             // 🔥 IMPORTANT FIX:
//             // Set the offset because the log does NOT store offset in the record.
//             res.Record.Offset = req.Offset

//             // Send record to client
//             if err := stream.Send(res); err != nil {
//                 return err
//             }

//             // Move to next offset
//             req.Offset++
//         }
//     }
// }

// type CommitLog interface{
// 	Append(*api.Record) (uint64,error)
// 	Read(uint64) (*api.Record,error)
// }

package server

import (
	"context"

	api "github.com/vishal555write/proglog/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)


var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (*grpcServer, error) {
	srv := &grpcServer{
		Config: config,
	}
	return srv, nil
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
	*grpc.Server,
	error,
) {
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(authenticate),
	)))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}


func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}


func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}


func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}


type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}


type Authorizer interface {
	Authorize(subject, object, action string) error
}


func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

