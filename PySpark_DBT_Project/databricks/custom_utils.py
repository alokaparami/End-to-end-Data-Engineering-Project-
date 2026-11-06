# create a class
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class transformations:

    def dedup(self, df:DataFrame,dedup_cols:List,cdc:str):
        df = df.withColumn("dedupKey",concat(*dedup_cols))
        df = df.withColumn("dedupCount",row_number()\
                .over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col("dedupCount") == 1)
        df = df.drop("dedupKey","dedupCount")
        return df
    
    def process_timestamp(self,df):
        df = df.withColumn("process_timestamp",current_timestamp())
        return df

    # create another function to upsert
    def upsert(self,df,key_cols,table,cdc):
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg")\
            .merge(df.alias("src"), merge_condition)\
            .whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}")\
            .whenNotMatchedInsertAll()\
            .execute()

        return 1

       
