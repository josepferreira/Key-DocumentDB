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
* como fazer o scan com o rocksdb? para já é realizado usando um iterador
* como juntar maps já ordenados de forma eficiente?
* pensar já nas projeções e seleções?

## Protótipo 2

1. adicionar projeções e seleções aos métodos get e scan
2. começar a pesquisar algoritmos de distribuição eficientes
3. tentar verificar ordenação nos mapas

