### Tranferencia de estado

- O slave junta-se ao grupo
- Deve analisar a primeira mensagem de membership 
    - Tem de ser na que ele se junta -> mensagem de join cujo o sender é ele próprio

##### NO caso em que ele esta sozinho

- Não precisa de fazer nada e assume que a transferência esta realizada

##### No caso em que ele não é o primário

- Pede a transferência ao primário

##### No caso em que ele é o primário

- Deve de pedir a um dos outros. Pode pedir ao "ex-primário"

##### Como deve ser feito

- Vai por "partes", em que é feito um scan por cada parte e enviada na mensagem. A ultima mensagem deve de ir com um boolean para saber quando acaba