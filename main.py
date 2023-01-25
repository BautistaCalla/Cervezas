import pandas as pd
import cx_Oracle
from pyspark.sql import SparkSession
import os
import glob
import numpy as np
from funciones import *
from datetime import datetime

print('Iniciando proceso de clasificacion de cervezas')

def main():

    # Traemos las cervezas que tenemos en d_producto (aprox 18k)
    sql_cervezas = """
    SELECT  dp.CODIGO_BARRAS_SIN_CEROS as CODIGO_BARRAS, dp.DESCRIPCION, dp.CANT_CONTENIDO
    FROM  dw.D_PRODUCTO dp 
    WHERE EST_MER_CODIGO = '.3.2.1.3.1'
    """

    with (
        cx_Oracle
        .connect(
            "iposs_sel/iposs_sel1023@192.168.1.167:1521/prvbibr1",
            encoding="UTF-8",
            nencoding="UTF-8", 
            )
        ) as con:
        cervezas_df = pd.read_sql(sql_cervezas, con)

    print('Cervezas en d_producto: ',cervezas_df.shape[0])

    # Traemos tabla de AMBEV, en donde conseguiremos la categorizacion PREMIUM/ESPECIAL

    sql_ambev = '''
    SELECT amb."Segmento 2", dp.CODIGO_BARRAS, dp.DESCRIPCION
    FROM DWCLA.D_PRODUCTO_3 amb
    JOIN dw.D_PRODUCTO dp ON amb.CODIGO_SE  = dp.CODIGO
    WHERE "Categoria" = 'Cerveja'
    '''

    with (
        cx_Oracle
        .connect(
            "iposs_sel/iposs_sel1023@192.168.1.167:1521/prvbibr1",
            encoding="UTF-8",
            nencoding="UTF-8",
            )
        ) as con:
        cervezas_ambev_df = pd.read_sql(sql_ambev, con)

    cervezas_ambev_df['CODIGO_BARRAS'] = cervezas_ambev_df['CODIGO_BARRAS'].astype(str)

    print('Cervezas en AMBEV: ',cervezas_ambev_df.shape[0])

    # Traemos descripciones hechas por los vendedores de las cervezas de d_producto
    
    # sql_descrip_cerv = """
    # SELECT dpe.EMP_CODIGO , dpe.CODIGO_BARRAS, dpe.DESCRIPCION
    # FROM  dw.D_PRODUCTO_EMPRESA dpe
    # RIGHT JOIN  dw.D_PRODUCTO dp ON dpe.CODIGO_BARRAS = dp.CODIGO_BARRAS
    # WHERE dp.EST_MER_CODIGO = '.3.2.1.3.1'
    # """

    # with (
    #     cx_Oracle
    #     .connect(
    #         "iposs_sel/iposs_sel1023@192.168.1.167:1521/prvbibr1",
    #         encoding="UTF-8",
    #         nencoding="UTF-8",
    #         )
    #     ) as con:
    #     descrip_cerv_df = pd.read_sql(sql_descrip_cerv, con)

    # descrip_cerv_df

    # print('Descripciones de cervezas: ',descrip_cerv_df.shape[0])

    # descrip_cerv_df.to_csv('descrip_cerv.csv', index=False)

    # En caso que no queramos traer la tabla de oracle, sino que la tengamos en un csv
    descrip_cerv_df = pd.read_csv('descrip_cerv.csv', dtype=  {'EMP_CODIGO': str, 'CODIGO_BARRAS': str, 'DESCRIPCION': str})
    descrip_cerv_df['CODIGO_BARRAS'] = descrip_cerv_df['CODIGO_BARRAS'].astype(str)

    # CERVEJAS de GPA

    sql_GPA = '''
    SELECT *
    FROM dwcla.arbol_grupo
    WHERE PEMP_CODIGO = 822 and PROD_CLASIF_3 = 'CERVEJAS'
    '''

    with (
        cx_Oracle
        .connect(
            "iposs_sel/iposs_sel1023@192.168.1.167:1521/prvbibr1",
            encoding="UTF-8",
            nencoding="UTF-8",
            )
        ) as con:
        prod_GPA_df = pd.read_sql(sql_GPA, con)

    prod_GPA_df['CODIGO_BARRAS'] = prod_GPA_df['CODIGO_BARRAS'].astype(str)


    # Creo una lista de codigos de barras de las cervezas a clasificar

    cat_asig = cervezas_df.CODIGO_BARRAS.unique()

    cat_asig_df = pd.DataFrame(cat_asig, columns=['CODIGO_BARRAS'])

    cat_asig_df['CODIGO_BARRAS'] = cat_asig_df['CODIGO_BARRAS'].astype(str)

    #############################CODIGO DE PRUEBA ################################
        # data1 = ['7896052607655', '7896052607662', '7898099397612']
        # data2 = [['CERVEJA TIGER PURE MALT 269ML LT', '7896052607655', 269],
        #     ['CERVEJA TIGER 269ML', '7896052607662', 269], 
        #     ['CHOPP WIENBIER 1,5L', '7898099397612', 1500]]

        # # Create the pandas DataFrame
        # cat_asig_df = pd.DataFrame(data1, columns=['CODIGO_BARRAS'])
        # cervezas_df = pd.DataFrame(data2, columns=['DESCRIPCION', 'CODIGO_BARRAS', 'CANT_CONTENIDO'])
        # cat_asig_df['CODIGO_BARRAS'] = cat_asig_df['CODIGO_BARRAS'].astype(str)
        # cervezas_df['CODIGO_BARRAS'] = cervezas_df['CODIGO_BARRAS'].astype(str)

    # Para cada codigo de barras le asigno una categoria. Si el cb no existe en d_producto, le asigno 'NO EXISTE'

    cat_asig_df['CAT_ASIGNADA'] = ''

    for index, cb in enumerate(cat_asig_df.CODIGO_BARRAS):
    #    if cb in cervezas_df['CODIGO_BARRAS'].values:
          try:
             most_common = get_most_common_words(cb, descrip_cerv_df)
             tag = get_category(cb, most_common, descrip_cerv_df, cervezas_df, cervezas_ambev_df, prod_GPA_df)
             cat_asig_df['CAT_ASIGNADA'][index] = tag
          except:
             cat_asig_df['CAT_ASIGNADA'][index] = 'ERROR'
    #    else:
    #       cat_asig_df['CAT_ASIGNADA'][index] = 'NO EXISTE'

    #Hago un print para seguir el progreso
    if index % 100 == 0:
            print(index, ' de ', len(cat_asig_df))

    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    # print("Current Time =", current_time)


    return cat_asig_df, current_time

if __name__ == "__main__":
    cat_asig_df, current_time = main()
    cat_asig_df.to_csv(f'cat_asig_df.csv {current_time}.csv', index=False)