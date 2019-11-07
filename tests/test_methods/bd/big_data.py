from pyspark.sql import functions as f


def method_a(df, param_1, param_2, param_3):
    return df.withColumn(param_1, f.lit(1)).withColumn(param_2, f.lit("value"))\
        .withColumn(param_3, f.lit(4.00))


def method_b(df_1, df_2, param_1, param_2):
    return df_1.join(df_2, [param_1, param_2])
