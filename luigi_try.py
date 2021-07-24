from sqlalchemy import create_engine
import luigi
import pandas as pd 

class QuerySupir_Medan(luigi.Task):
    def requires(self):
        return []
    def output(self):
        return luigi.LocalTarget("supir_medanDB.csv")
    def run(self):
        engine = create_engine('sqlite:///supir_medan')
        result = pd.read_sql_query ('SELECT * FROM supir', engine)
        f = self.output().open('w')
        result.to_csv(f, encoding = 'utf-8', index=False,header=True,quoting=2)
        f.close()
class QueryGaji_Supir(luigi.Task):
    def requires(self):
        return[]
    def output(self):
        return luigi.LocalTarget("gaji_supir.csv")
    def run(self):
        engine = create_engine('sqlite:///gaji_supir')
        result = pd.read_sql_query('SELECT * FROM gaji', engine)
        f = self.output().open('w')
        result.to_csv(f,encoding = 'utf - 8', index=False, header=True,quoting=2)
        f.close()
class CreateReport(luigi.Task):
    def requires(self):
        return [QuerySupir_Medan(),QueryGaji_Supir()]
    def output(self):
        return luigi.LocalTarget("Laporan.csv")
    def run(self):
        df1 = pd.read_csv("supir_medanDB.csv", header = 0, encoding = 'utf-8',index_col = False)
        df2 = pd.read_csv("gaji_supir.csv", header = 0, encoding = 'utf-8',index_col = False)
        df3 = pd.merge(df1,df2,how='inner',on=['id'])
        f = self.output().open('w')
        df3.to_csv(f,encoding = 'utf-8',index=False,header=True,quoting=2)
        f.close()
if __name__ == '__main__':
    luigi.run(main_task_cls=CreateReport,local_scheduler=False)