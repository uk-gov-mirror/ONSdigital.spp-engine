import pandas as pd


def method_c(df, param_1, param_2, param_3):
    df[param_1] = 0
    df[param_2] = 1
    df[param_3] = 2


def method_d(df_1, df_2, param_1, param_2):
    return df_1.merge(df_2, on=[param_1, param_2], how='outer')
