test-prof:
	go test -cpuprofile=cpu.prof -run TestUESimulation

test-flame:
	go tool pprof -http=":8000" cpu.prof
