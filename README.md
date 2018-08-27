# kapacitor-UDFs
## Go
### Exponential Moving average
4 inputs to function.
- field - str
- size - int
- as - str
- alpha - float

Calculates the EMA[t] of a series x[t] where n is the length of the window and α is a smoothing factor where 0 < α < 1:
EMA[t+1] = x[t+1]*α + (EMA[t] - x[t-n]\(1-α)<sup>n-1</sup>)*(1-α)
