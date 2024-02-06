#!/bin/bash


for i in {1..1}
do
   go test -run 2A >> output.txt
done

for i in {1..10}
do
   go test -run 2B >> output.txt
done

