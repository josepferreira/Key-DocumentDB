## Ligação ao ycsb

- Temos de criar uma classe **DBKVClient** que implementa a **DB**
- Temos também de criar uma classe **DBKVClientTest** que utiliza a classe anterior, para os testes

(Podemos seguir o exemplo do *rocksdb*: https://github.com/brianfrankcooper/YCSB/tree/master/rocksdb)


Provavelmente vamos é ter de acrescentar a nossa DB ao ficheiro **./bin/ycsb** para poder realizar os testes.

Os testes que podemos correr podem ser os que eles já têm pré-definidos. (https://github.com/brianfrankcooper/YCSB/tree/master/workloads)