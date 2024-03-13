#!/bin/bash

for i in {1..10}
do
   go test -race -run TestConcurrent1 >> output.txt
done
