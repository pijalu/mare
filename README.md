# mare
MaRe is a simple in memory map/reduce package.
It allows to perform M/R of slices or channels by providing a map and a reduce.
the Map should return a key and value, stored
the Reduce should reduce 2 values to  a single one value.

Result is a map, with keys beeing the list of mapped keys and containing the values

## interface
To allow a more generic use, this library uses interface{}.

## Example
See [mare_test.go](mare_test.go) for simple examples