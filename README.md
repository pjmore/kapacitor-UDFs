# kapacitor-UDFs
## Go
### 1. Exponential Moving average (expMovAvg.go) STREAM-STREAM
4 inputs to function.
- field - str
- size - int
- as - str
- alpha - float

Calculates the EMA[t] of a series x[t] where n is the length of the window and α is a smoothing factor where 0 < α < 1:
EMA[t+1] = x[t+1]*α + (EMA[t] - x[t-n]\(1-α)<sup>n-1</sup>)*(1-α)

or:

EMA[t+1] = α(x[t] + (1-α)x[t-1] + (1-α)<sup>2</sup>x[t-2] + (1-α)<sup>3</sup>x[t-3] ... (1-α)<sup>n</sup>x[t-n]) 
## Python
### 1. Exponential Moving average (EMA.py) STREAM-STREAM
Operates identically to the Go version

4 inputs to function.
- field - str
- size - int
- as - str
- alpha - float

Calculates the EMA[t] of a series x[t] where n is the length of the window and α is a smoothing factor where 0 < α < 1:
EMA[t+1] = x[t+1]*α + (EMA[t] - x[t-n]\(1-α)<sup>n-1</sup>)*(1-α)

or:

EMA[t+1] = α(x[t] + (1-α)x[t-1] + (1-α)<sup>2</sup>x[t-2] + (1-α)<sup>3</sup>x[t-3] ... (1-α)<sup>n</sup>x[t-n]) 

### 2. Echo batch (echoBatch.py) STREAM-BATCH
4 inputs to function
- field - str
- size - int
- as - str - not used yet
- period - duration

Echoes the data coming in on a stream as a batch. Period determines how long before the batch is dumped to the output and size determines the 
maximum size of the batch. If both are used the size will dump only the group that exceeded the size limit and the time will dump all of the groups independently of eachotehr.
 This could create strange behavior in downstream nodes.
	
