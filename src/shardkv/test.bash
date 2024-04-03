#!/bin/bash

fail_occurred=0

for i in {1..300}
do
   go test -run TestConcurrent3 >> output3.txt
   if grep -q "FAIL" output3.txt; then
       fail_occurred=1
       break
   fi
   rm output3.txt
done


