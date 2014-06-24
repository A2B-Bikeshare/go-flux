package fluxd

import (
	"runtime"
)

func main() {
	ncpu := runtime.NumCPU()
	runtime.GOMAXPROCS(ncpu)

}
