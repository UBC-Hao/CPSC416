#!/bin/bash


for i in {1..20}
do
   go test -run 2C >> output.txt
done

for i in {1..20}
do
   go test -run 2B >> output.txt
done

