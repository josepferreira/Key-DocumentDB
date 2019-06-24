# Key-DocumentDB
Repositório para o projeto de LEI - Base de Dados NoSQL orientada ao modelo Serverless

## Instruções de Utilização

- As pastas Key-DocumentDBv1, Key-DocumentDBv2 e Key-DocumentDBv3 são as versões mias antigas da base de dados e é para analisar a sua evolução de uma forma mais eficaz
- A pasta DB tem o código mais atualizado deste projeto

### Para testar a nossa aplicação é encessário realizar os seguintes passos

1. cd DB
2. ./buildComponents.sh <- para criar a imagem do Docker do slave
3. sh startSpread.sh <- para iniciar o sread - **necessário ter o mesmo instalado**
4. sh runMaster.sh 1 Primeiro <- O primeiro argumento é para indicar o endereço do master, já o segundo só deve de ser colocado quando se inicia o primeiro master

*Nota:* Podem ser inicializados quantos masters se pretender

A script eliminaDocker.sh elimina os containers criados

### Para correr os testes

1. cd YCSB-master
2. sh runTestes.sh nomeBD workloadPath resultPath <- result path tem de seguir o seguinte padrão: resultados/X-Workload[Editavel]
