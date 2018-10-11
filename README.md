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
maximum size of the batch. If both are used the size will dump only the group that exceeded the size limit and the time will dump all of the groups independently of each other.
 This could create strange behavior in downstream nodes.
 
### 3. Windowed uncorrected sample standard deviation (Sigma.py) STREAM-STREAM
1 mandatory input
- field - str
3 optional
- window - int - default 100
- as - str - default sigma_<input field name>
- fillWindow - bool - default false
Calculates how many uncorrected sample standard deviations the newest point in the field is off of the mean. σ = (E[X<sup>2</sup>] - E[X]<sup>2</sup>)<sup>0.5</sup>
Unlike kapacitor's sigma function this only calculates σ over the last N points, as well as returning the direction of the deviation. e.g. a value two σ below the mean will 
will return -2. note that the error of this function compared to the sample standard deviation is 1/N so small windows are not recommended.

	
