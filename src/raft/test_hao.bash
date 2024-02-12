#!/bin/bash

for i in {1..100}
do
   go test -race >> output.txt
done
