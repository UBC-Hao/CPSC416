#!/bin/bash

for i in {1..5}
do
   go test -run 2C >> output.txt
done

for i in {1..5}
do
   go test -run 2B >> output.txt
done


for i in {1..5}
do
   go test -run 2A >> output.txt
done