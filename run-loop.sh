#!/usr/bin/env bash

for i in 0 1 2 3 4 5 6 7 8 9; do
	./run.sh | tee results${i}.txt
done
