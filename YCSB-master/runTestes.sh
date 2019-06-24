sh load.sh $2
./runAll.sh $1 $2 $3 
cd resultados
../transformaAll.sh *
cd ..
rm -r ycsb2graph-master/resultadosGrafico/
cp -r resultadosGrafico/  ycsb2graph-master
cd ycsb2graph-master
sh apresenta.sh resultadosGrafico/