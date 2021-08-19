import unittest
from pyspark.sql import SparkSession

# Para importar a classe eu preciso apontar para o diretório e arquivo py,
# Importo a CLASSE.
from src.jobs.to_trusted import ToTrusted

#quero uma função que chame a função do job e faça o output

class ToTrustedTest(unittest.TestCase):

    # def __init__(self):
    #     pass

    def test_processo(self):
# /opt/tatiane/agosto/projeto/tests/locallake/raw/alunos.csv
# file:/opt/tatiane/agosto/projeto/tests/locallake/raw/alunos_diciplina.csv

        spark = SparkSession.builder.getOrCreate()

        fr_path_al = '../projeto/tests/locallake/raw/alunos.csv'
        fr_path_disc = '../projeto/tests/locallake/raw/alunos_diciplina.csv'
        to_path = '../projeto/tests/locallake/trusted/alunos_salvos.csv'
        chamada = ToTrusted(fr_path_al,fr_path_disc,to_path)
        resultado = chamada.processa()

        # spark = SparkSession.builder.getOrCreate()
        df = spark.read.format('delta').load(to_path)
        df.show(2)
        # # tati
        # df_output.collect()[0][3]
        # # salas
        # df_output.collect()[0][3]

        values = df.orderBy(['aluno']).collect()
# values[0]['Media']
#
# values[1]['Media']
        print(values[0]['Media'], values[1]['Media'])
        self.assertEqual(8.75, values[0]['Media'])
        self.assertEqual(7.5, values[1]['Media'])
        print('passou no teste')
