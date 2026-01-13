package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer (addr string) *http.Server {
httpsrv:=newHTPServer()
r:=mux.NewRouter()

r.HandleFunc("/",httpsrv.handleProduce).Methods("POST")
r.HandleFunc("/",httpsrv.handleConsume).Methods("GET")
return &http.Server{
	Addr:addr,
	Handler: r,
}
}

type httpServer struct{
	Log *Log
}
 
func newHTPServer() *httpServer{
	return &httpServer{
		Log:NewLog(),
	}
}

type ProduceRequest struct{
	Record Record `json:"record"`
}
type ProduceRespose struct{
	Offset uint64 `json:"offset"`
}

type ConsumerRequest struct{
	Offset uint64 `json:"offset"`
}
type ConsumerResponse struct{
	Record Record `json:"record"`
}

func(s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request){
	var req ProduceRequest
	err:=json.NewDecoder(r.Body).Decode(&req)
	if err !=nil{
		http.Error(w,err.Error(),http.StatusBadRequest)
		return
	}
	off,err:=s.Log.Append(req.Record)
	if err!=nil{
		http.Error(w,err.Error(),http.StatusInternalServerError)
		return
	}

	res:=ProduceRespose{Offset: off}
	err=json.NewEncoder(w).Encode(res)
	if err!=nil{
		http.Error(w,err.Error(),http.StatusInternalServerError)
		return
	}

}

func(s *httpServer) handleConsume(w http.ResponseWriter,r *http.Request){
	 var req ConsumerRequest
	 err:=json.NewDecoder(r.Body).Decode(&req)
	 if err!=nil{
		http.Error(w,err.Error(),http.StatusNotFound)
		return
	 }
    record,err:=s.Log.Read(req.Offset)
	if err == ErroffsetNotFound{
		http.Error(w,err.Error(),http.StatusNotFound)
	}
	if err!=nil{
		http.Error(w,err.Error(),http.StatusInternalServerError)
		return
	}
	res:=ConsumerResponse{Record: record}
	err=json.NewEncoder(w).Encode(res)
	if err!=nil{
		http.Error(w,err.Error(),http.StatusInternalServerError)
		return
	}
}
