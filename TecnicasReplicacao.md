# Replicação

## Replicação ativa - Master

### Transferência de estado

- pedido em total order multicast

- quando recebe pedido, começa a guardar os pedidos em fila que chegam a partir daí

- para já respondem todos, depois podemos ver como pomos só um a responder (mas para isso temos de ter vistas penso eu)
    - por isso é importante que o pedido leve id, para detetar respostas duplicadas

- falta pensar em como é que ele sabe se é o primeiro
    - para já é feito passando um argumento booleano

## Replicação Passiva - Slave

- 1 grupo para cada conjunto de chaves (chunk)

- o master indicas os id's

- vamos ter problemas se por exemplo tivermos 3 e falharem 2 e depois recuperarem, pq vai ser só um que vai ficar responsavel por tudo