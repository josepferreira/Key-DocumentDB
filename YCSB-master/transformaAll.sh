#!/bin/bash
for var in "$@"
do
    A="$(cut -d'.' -f1 <<<$var)"
    ../transforma.awk $var > "../resultadosGrafico/$A.result"
done