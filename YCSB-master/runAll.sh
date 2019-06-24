#!/bin/bash
thread="1"
TARGETS="500 1000 1500 2000 2500 3000 3500 4000"

US="_"

for target in ${TARGETS[@]}
do    
    echo "Doing the run for DBLEI Target as $target and Thread as $thread"
       ./bin/ycsb run $1 -P $2 -p operationcount=$target -s &> "$3-$target.log"
	echo "Run for DBLEI Target as $target and Thread as $thread done"     	
done

echo "Script completed"