# Lista de tarefas a realizar
## Protótipo 1

1. fazer uma BD com 1 master e workers predefinidos
2. sem replicacao
3. sem elasticidade
4. sem recuperacao.
5. Para além disso é para fazer sem transaçoes.

### Arquitetura:

* nodes.Master - coordenador
* nodes.Slave - o que guarda as cenas da BD e realiza as funcoes
* nodes.Stub - do cliente, n guarda nada em cache


### Tarefas

* Jorge Oliveira - get/delete
* José Ferreira - put/scan
* depois fazer o stub também (este trata da comunicação com os slaves)

### Dúvidas

* pensar já em algo do algoritmo de distribuição?
* pensar em mais, tinha uma do stub mas n me lembro qual é

