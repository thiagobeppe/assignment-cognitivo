import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

#Constantes -  path completo para executar utilizando spark-submit
PATH_TO_READ = r'C:\Users\thiag\Downloads\teste-eng-dados\data\input\users\load.csv'
PATH_TO_WRITE = r'C:\Users\thiag\Downloads\teste-eng-dados\data\output'
PATH_WITH_CONFIG =  r'C:\Users\thiag\Downloads\teste-eng-dados\config\types_mapping.json'


#Função Auxiliar para aplicar o cast no dataframe
def cast_by_file(path, df):
    with open(path) as types_mapping:
        types = json.load(types_mapping)
        for key in types:
            df = df.withColumn(key, col(key).cast(types[key]))
    return df

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Cognitivo's challeng solution").enableHiveSupport().getOrCreate()
    
    df = spark.read.csv(PATH_TO_READ, sep=',', header=True)

    #Solução do tópico 2 
    '''
        Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, 
        variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados, 
        a fim de apenas manter a última entrada de cada registro, 
        usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) 
        para definição do registro mais recente;
    '''

    window = Window.partitionBy(col('id')).orderBy(col('update_date').desc())
    df = df.withColumn("ranked_id", rank().over(window)).filter(col('ranked_id') == 1).drop(col("ranked_id")).sort(col('id').asc())

    #Solução do tópico 3
    '''
        Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração (types_mapping.json), 
        contendo os nomes dos campos e os respectivos tipos desejados de output.
        Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;
    '''

    #Aplicação da função auxiliar    
    df = cast_by_file(PATH_WITH_CONFIG, df)
    
    #Cast adicional para otimizar perfomance, tipos inteiros perfomam melhor para identificadores do que strings
    df = df.withColumn('id', col('id').cast('long'))

    #Solução do tópico 3
    '''
        Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório data/input/users/load.csv,
        para um formato colunar de alta performance de leitura de sua escolha.
        Justificar brevemente a escolha do formato;
    '''

    #Justificativa
    '''
    A escolha do parquet foi devido ele ser um dos mais otimizados em relação a leitura e mais versatil para utilização de DW em cloud como Redshift, 
    ORC possui um compress ratio melhor quando usado com zlib, porém, não é recomendado para um uso comum, visto que foi projetado pra usar com HIVE
    optei por escolher o parquet.
    '''

    df.coalesce(1).write.parquet(PATH_TO_WRITE, mode='overwrite')

