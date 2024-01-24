#!/usr/bin/env bash

run() {
    timeout 300 time target/release/dashmap-test $@ || echo TIMEOUT
}

cargo build --release

for p in 0 10 100 1000 10000 100000 1000000 10000000; do
    for w in 0 10 100 1000 10000 100000 1000000 10000000; do
        for r in 0 10 100 1000 10000 100000 1000000 10000000; do
            run hashmap -s 1 contention -p $p -w $w -r $r -e
            run dashmap -s 4 contention -p $p -w $w -r $r -e
            run dashmap -s 8 contention -p $p -w $w -r $r -e
        done
    done
done
