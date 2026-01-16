bench-prof:
	go test -bench=Benchmark_route_expansion -cpuprofile=bench.prof

bench-flame:
	go tool pprof -http=":8000" bench.prof
