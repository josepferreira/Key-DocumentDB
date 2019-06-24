#!/bin/bash
for var in "$@"
do
    A="$(cut -d'.' -f1 <<<$var)"
    ../transforma.awk $var > "../resultadosAp/$A.result"
done