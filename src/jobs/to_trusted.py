from pyspark.sql import SparkSession
from src.commons.abstrata import Abstrata

class ToTrusted(Abstrata):
    def __init__(self, fr_path_al,fr_path_disc,to_path):
        super().__init__(fr_path_al,fr_path_disc,to_path)
        print('instanciou')
        # self.fr_path_al = fr_path_al
        # self.fr_path_disc = fr_path_disc
        # self.to_path = to_path

    def processa(self):
        spark = SparkSession.builder.getOrCreate()
        df_alunos = spark.read.format('csv').option('header', True).load(self.fr_path_al)
        df_disciplina = spark.read.format('csv').option('header', True).load(self.fr_path_disc)

        df_output = df_alunos.join(df_disciplina, ['aluno','curso'], 'left')
        df_output.show(1)

        df_output = df_output.groupBy(['aluno','curso','semestre']).agg({'nota': 'mean'}).alias("media")
        df_output = df_output.withColumnRenamed("avg(nota)", "Media")
        df_output.show()

        df_output.write.format('delta').mode('overwrite').save(self.to_path)
        # # # df.write(self.to_path)
        # print('deu certo')
        return 1
