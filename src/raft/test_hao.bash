#!/bin/bash

for i in {1..100}
do
   go test -run TestFigure8Unreliable2C >> output2.txt
done
