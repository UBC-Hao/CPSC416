#!/bin/bash

for i in {1..40}
do
   go test -run 2C >> output.txt
done
