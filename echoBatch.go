package main

import (
		"bytes"
		"encoding/gob"
		"errors"
		"log"
		"os"
		
		"github.com/influxdata/kapacitor/udf/agent"
)

// This struct holds all information that charecterizes the input and output stream
type echoHandler struct{
		field string
		as string
		size int
		period int
		state map[string]*echoState
		timeout int
		agent *agent.Agent
}



type echoState struct {
		Size int
		Window []agent.Point
}


// returns a function that updates the value of the exponential moving average
func (e *echoState) update(p *agent.Point ){
	e.Window = append(e.Window,p)
}

func (e *echoState) myAlgorithm() []*agent.Point{
	return e.Window
}

func newEchoHandler(a *agent.Agent) *echoAvgHandler {
		return &expAvgHandler{
			state: make(map[string]*echoAvgState),
			as: "echo",
			agent:a,
		}
}

// the Info function defines what form the input and output form of the data, as well as what data types the UDF requires

func (e *echoHandler) Info() (*agent.InfoResponse, error){
	info:= &agent.InfoResponse{
		Wants: agent.EdgeType_STREAM,
		Provides: agent.EdgeType_STREAM,
		Options: map[string]*agent.OptionInfo{
		"field":{ValueTypes: []agent.ValueType{agent.ValueType_STRING}},
		"size":	{ValueTypes: []agent.ValueType{agent.ValueType_INT}},
		"as":	{ValueTypes: []agent.ValueType{agent.ValueType_STRING}},
		"period":{ValueTypes:[]agent.ValueType{agent.ValueType_DURATION}},
		},
	}
	return info,nil
}

// The Init function is called by kapacitor after Info
// This is where the values are passed into the function and error checking takes place

func (e *echoHandler) Init(r *agent.InitRequest) (*agent.InitResponse, error){
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
			case "period": 
				e.period= opt.Values[0].Value.(*agent.OptionValue_DurationValue).DurationValue
		}
	
	}
	if e.field == "" {
		init.Success = false
		init.Error += " must supply field"
	}
	if e.size == 0 && e.period == 0{
		init.Success = false
		init.Error += " must supply a maximum size or maximum time"
	}
	if e.as == "" {
		init.Success = false
		init.Error += " invalid as name provided"
	}
	return init,nil
}

// Allows kapacitor to take a snapshot of the process variables

func (e *echoHandler) Snapshot() (*agent.SnapshotResponse,error){
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(e.state)
	
	return &agent.SnapshotResponse{
		Snapshot :buf.Bytes(),
	},nil
}


// Restores the process variables to their previous state
func (e *echoHandler) Restore(req *agent.RestoreRequest) (*agent.RestoreResponse,error){
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

func (e *echoHandler) createStartBatch(l int,group string) error {
	state = e.state[group]
	point = state.Window[len(state.Window)-1]
	e.agent.Responses <- &agent.Response{
		Message: &agent.Response_BeginBatch{
			EndBatch: {
				name:	point.name,
				group: 	point.group,
				size:	l,
			},
		},
	}

}

func (e * echoHandler) createEndBatch(timeout int,group string) error {
	state = e.state[group]
	point = state.Window[len(state.Window)-1]
	e.agent.Responses <- &agent.Response{
		Message: &agent.Response_BeginBatch{
			EndBatch: {
				name:	point.name,
				group: 	point.group,
				tmax:	point.time,
			},
		},
	}
}



func (e *echoHandler) empty(timeout int,group string) error {
	state = e.state[group]
	data := state.myAlgorithm()
	e.createStartBatch(len(data),group)
	for _,point := range data{
		e.agent.Responses <- &agent.Response{
			Message: &agent.Response_Point{
				Point: data,
			},
		}
	}
	e.createEndBatch(timeout,group)
	state.reset()
}


func (e *echoHandler) emptyAllGroups () error {
	for k,v := range e.state{
		v.empty(e.timeout,k)
	}
	e.timeout += e.period
}




func (e *echoHandler) Point(p *agent.Point) error{
	state := e.state[p.Group]
	if state == nil{
		state = &echoState{Size: e.size}
		e.state[p.Group] = state
	}
	
	if e.timeout < p.time && e.period > 0 {
		if e.timeout == 0{
			e.timeout = p.time + e.period
		}
		else{
			e.emptyAllGroups()
		}
	}
	if e.size < len(state.window) && e.size > 0 {
		state.empty()
	}
	
	state.update(p)
	return nil
}


// Since the input is a stream this is never called
func (e *echoHandler) BeginBatch(*agent.BeginBatch) error {
	return errors.New("Batch inputs are not supported")
}

func (e *echoHandler) EndBatch(*agent.EndBatch) error {
	return errors.New("Batch inputs are not supported")
}

func (e *echoHandler) Stop(){
	close(e.agent.Responses)
}

// A process UDF uses stdin and stdout to communicate with the process
func main() {
	a := agent.New(os.Stdin, os.Stdout)
	h := newEchoHandler(a)
	a.Handler = h
	
	log.Println("Starting agent")
	a.Start()
	err :=a.Wait()
	if err !=nil {
		log.Fatal(err)
	}
}
