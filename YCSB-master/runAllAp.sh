#!/bin/bash
thread="1"
TARGETS="200 400 600 800"

US="_"

for target in ${TARGETS[@]}
do    
    echo "Doing the run for DBLEI Target as $target and Thread as $thread"
       ./bin/ycsb run dblei -P workloads/workloadaC -p operationcount=$target -s &> resultadosAp/dbleiV4Redistribuicao-WorkloadA-$target.log
	echo "Run for DBLEI Target as $target and Thread as $thread done"     	
done

echo "Script completed"