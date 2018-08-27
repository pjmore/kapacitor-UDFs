package main

import (
		"bytes"
		"encoding/gob"
		"errors"
		"log"
		"os"
		
		"github.com/influxdata/kapacitor/udf/agent"
)

type expAvgHandler struct{
		field string
		as string
		size int
		alpha float64
		state map[string]*expAvgState

		agent *agent.Agent
}



type expAvgState struct {
		Size int
		Window []float64
		Avg float64
		Alpha float64
}


// returns a function that updates the value of the moving average
func (e *expAvgState) update(value float64) float64 {
	beta := 1.0 - e.Alpha
	l := len(e.Window)
	if e.Size == l {
		e.Avg = value*e.Alpha + (e.Avg - e.Window[0])*beta
		e.Window = e.Window[1:]
	}else{
		e.Avg = value*e.Alpha + e.Avg*beta
	}
	// multiplies all elements by beta
	for i := len(e.Window)-1; i >= 0; i--{
		e.Window[i] = e.Window[i]*beta
	}
	e.Window = append(e.Window,value*e.Alpha)
	return e.Avg
}

func newExpAvgHandler(a *agent.Agent) *expAvgHandler {
		return &expAvgHandler{
			state: make(map[string]*expAvgState),
			as: "avg",
			agent:a,
		}
}

func (e *expAvgHandler) Info() (*agent.InfoResponse, error){
	info:= &agent.InfoResponse{
		Wants: agent.EdgeType_STREAM,
		Provides: agent.EdgeType_STREAM,
		Options: map[string]*agent.OptionInfo{
		"field":{ValueTypes: []agent.ValueType{agent.ValueType_STRING}},
		"size":	{ValueTypes: []agent.ValueType{agent.ValueType_INT}},
		"as":	{ValueTypes: []agent.ValueType{agent.ValueType_STRING}},
		"alpha":{ValueTypes: []agent.ValueType{agent.ValueType_DOUBLE}},
		},
	}
	return info,nil
}

func (e *expAvgHandler) Init(r *agent.InitRequest) (*agent.InitResponse, error){
	init := &agent.InitResponse{
		Success:true,
		Error: "",
	}
	for _, opt := range r.Options{
		switch opt.Name{
			case "field":
				e.field = opt.Values[0].Value.(*agent.OptionValue_StringValue).StringValue
			case "size":
				e.size = int(opt.Values[0].Value.(*agent.OptionValue_IntValue).IntValue)
			case "as":
				e.as = opt.Values[0].Value.(*agent.OptionValue_StringValue).StringValue
			case "alpha":
				e.alpha = float64(opt.Values[0].Value.(*agent.OptionValue_DoubleValue).DoubleValue)
		
		}
	
	}
	if e.field == "" {
		init.Success = false
		init.Error += " must supply field"
	}
	if e.size == 0 {
		init.Success = false
		init.Error += " must supply window size"
	}
	if e.as == "" {
		init.Success = false
		init.Error += " invalid as name provided"
	}
	if e.alpha == 0 {
		init.Success = false
		init.Error += " must supply smoothing factor"
	}
	return init,nil
}

func (e *expAvgHandler) Snapshot() (*agent.SnapshotResponse,error){
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(e.state)
	
	return &agent.SnapshotResponse{
		Snapshot :buf.Bytes(),
	},nil
}

func (e *expAvgHandler) Restore(req *agent.RestoreRequest) (*agent.RestoreResponse,error){
	buf := bytes.NewReader(req.Snapshot)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&e.state)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	return &agent.RestoreResponse{
		Success: err ==nil,
		Error: msg,
	}, nil
}


func (e *expAvgHandler) Point(p *agent.Point) error{
	value := p.FieldsDouble[e.field]
	state := e.state[p.Group]
	if state == nil{
		state = &expAvgState{Size: e.size, Alpha: e.alpha}
		e.state[p.Group] = state
	}
	avg := state.update(value)
	p.FieldsDouble = map[string]float64{e.as: avg}
	p.FieldsInt = nil
	p.FieldsString = nil
	e.agent.Responses <- &agent.Response{
		Message: &agent.Response_Point{
			Point: p,
		},
	}
	return nil
}


func (e *expAvgHandler) BeginBatch(*agent.BeginBatch) error {
	return errors.New("batching not supported")
}

func (e *expAvgHandler) EndBatch(*agent.EndBatch) error {
	return errors.New("batching not supported")
}

func (e *expAvgHandler) Stop(){
	close(e.agent.Responses)
}

func main() {
	a := agent.New(os.Stdin, os.Stdout)
	h := newExpAvgHandler(a)
	a.Handler = h
	
	log.Println("Starting agent")
	a.Start()
	err :=a.Wait()
	if err !=nil {
		log.Fatal(err)
	}
}