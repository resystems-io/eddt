bench-prof:
	go test -bench=Benchmark_kv_set_merge/merge_into_set_with_100_ -cpuprofile=bench.prof

bench-flame:
	go tool pprof -http=":8000" bench.prof
