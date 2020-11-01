# Teste técnico para a posição de Engenheiro de Dados na plataforma [Cognitivo.ai](https://www.cognitivo.ai/)

## Organização dos dados

```
+---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
|id |name                              |email                |phone          |address                                       |age|create_date               |update_date               |
+---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
|1  |david.lynch@cognitivo.ai          |David Lynch          |(11) 99999-9997|Mulholland Drive, Los Angeles, CA, US         |72 |2018-03-03 18:47:01.954752|2018-03-03 18:47:01.954752|
|1  |david.lynch@cognitivo.ai          |David Lynch          |(11) 99999-9998|Mulholland Drive, Los Angeles, CA, US         |72 |2018-03-03 18:47:01.954752|2018-04-14 17:09:48.558151|
|2  |sherlock.holmes@cognitivo.ai      |Sherlock Holmes      |(11) 94815-1623|221B Baker Street, London, UK                 |34 |2018-04-21 20:21:24.364752|2018-04-21 20:21:24.364752|
|3  |spongebob.squarepants@cognitivo.ai|Spongebob Squarepants|(11) 91234-5678|124 Conch Street, Bikini Bottom, Pacific Ocean|13 |2018-05-19 04:07:06.854752|2018-05-19 04:07:06.854752|
|1  |david.lynch@cognitivo.ai          |David Lynch          |(11) 99999-9999|Mulholland Drive, Los Angeles, CA, US         |72 |2018-03-03 18:47:01.954752|2018-05-23 10:13:59.594752|
|3  |spongebob.squarepants@cognitivo.ai|Spongebob Squarepants|(11) 98765-4321|122 Conch Street, Bikini Bottom, Pacific Ocean|13 |2018-05-19 04:07:06.854752|2018-05-19 05:08:07.964752|
+---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
```
---
## Resultado esperado: 
```
+---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
|id |name                              |email                |phone          |address                                       |age|create_date               |update_date               |
+---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
|1  |david.lynch@cognitivo.ai          |David Lynch          |(11) 99999-9999|Mulholland Drive, Los Angeles, CA, US         |72 |2018-03-03 18:47:01.954752|2018-05-23 10:13:59.594752|
|2  |sherlock.holmes@cognitivo.ai      |Sherlock Holmes      |(11) 94815-1623|221B Baker Street, London, UK                 |34 |2018-04-21 20:21:24.364752|2018-04-21 20:21:24.364752|
|3  |spongebob.squarepants@cognitivo.ai|Spongebob Squarepants|(11) 98765-4321|122 Conch Street, Bikini Bottom, Pacific Ocean|13 |2018-05-19 04:07:06.854752|2018-05-19 05:08:07.964752|
+---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
```
---

## Requisitos
1. Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório data/input/users/load.csv, para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;

2. Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) para definição do registro mais recente;

3. Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output. Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;

## Solução
1. Foi utilizado o formato `parquet` utilizando o `snappy`,a compressão *default* do *pyspark*, por ele ser bastante otimizado para leitura. O formato `ORC` também foi levado em consideração, porém ele foi desenvolvido para ser utilizado na plataforma *HIVE* então não é indicado para casos gerais, mesmo tendo um *compress ratio* melhor quando usado junto com `zlib`. Ademais o `parquet` tem uma boa integração com ambientes de cloud para *DW* como o `Redshift`.
2. Foi utilizado uma abordagem com *window function* em conjunto com a função *rank()* para organizar os dados, após isso filtrar pela primeira posição e dropar as colunas auxiliares
3. Para a conversão dos dados foi criado uma função auxiliar para que tanto o path de leitura e o *dataframe* fosse o mais genérico possível, sendo assim caso seja necessário adicionar novas colunas para fazer o cast basta adicionar direto no arquivo de origem. Foi convertido também a coluna `id`, visto que ela era uma string e para dados utilizados como identificadores é recomendado a utilização de valores `inteiros`, então pensando no crescimento da tabela foi utilizado o tipo `long`.


