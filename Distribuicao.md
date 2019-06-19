# Distribuição

## Ao realizar a distribuição

- Transfere estado com várias mensagens
    - pode ser como na replicação passiva

- Se deixar de ser primario tem de alterar o id, mas mantem-se no grupo para responder aos gets e scans
    - vemos se altera o id ou se simplesmente ignora puts e removes

- Puts e removes são atrasados/ignorados
    - depende do anterior

- Só pode sair do grupo quando tiver a confirmação do outro(s) q já recuperou estado!
    - o q está a transferir estado para outro(s)!

- Vai ter de existir separacao entre os q tem o estado recuperado e podem responder aos gets e os q n podem (ainda estao a meio da transferencia)
    - ou entao vai para todos e so responde qd tiver o estado recuperado

- Vai ter de existir separacao nos SlaveIdentifiers entre os q estao a transferir estado e vao sair e os q sao para ficar! 


## Algoritmo de distribuição

- A tem:
    - k1
    - k2
    - k3

- B tem:
    - k4
    - k5
    - k6

- C tem:
    - k7
    - k8
    - k9

**Entra D e então: são transferidos por exemplo: k3 e k6 para D, ficando:**

- A tem:
    - k1
    - k2

- B tem:
    - k4
    - k5

- C tem:
    - k7
    - k8
    - k9

- D tem:
    - k3
    - k6

### **Esta distribuição é realizada com base na monitorização**
### **_Como é que vamos fazer a monitorização?_**
