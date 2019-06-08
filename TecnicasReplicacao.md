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