import re
import math
from collections import Counter

import pandas as pd
import cx_Oracle as cx
from pyspark.sql import SparkSession
import os
import glob
from datetime import datetime
import polars as pl
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def get_category(codigo_barras,most_common, descrip_cerv_df, cervezas_df, cervezas_ambev_df, prod_GPA_df):

    

    categoria = ''
    
    pilsen = ('pilsen', 'pl', 'pil', 'pi', 'pils', 'pilse')
    garrafa = ('garrafa', 'garra', 'gf', 'gfa', 'grf', 'neck')
    lata = ('lata', 'lt', 'la', 'lat', 'lta', 'latao')
    escura = ('Escura', 'esc', 'escu', 'escur', 'malzbier', 'malz', 'preta', 'black', 'negra', 'brown')
    kit = ('kit','kit')
    sinal = ('alcool', 's/alcool', 'alcoo')
    sabor = ('vinho', 'vinhos', 'vinh', 'vin')

    (Pilsen, GFA, LTA, Escura, KIT, Nacional, Import, Sinalc, Sab, Premium, Especial) = (False, False, False, False, False, False, False, False, False, False, False)

    cantidad_descripciones = len(descrip_cerv_df[descrip_cerv_df['CODIGO_BARRAS'] == codigo_barras]["DESCRIPCION"])

#Busco en la descripcion de la empresa

    desc_empresa = cervezas_df.loc[cervezas_df['CODIGO_BARRAS'] == codigo_barras, 'DESCRIPCION'].item().lower().split()


    for x in sabor:
        if x in desc_empresa:
            Sab = True
            # categoria.append('SABOR')
            break   

    if Sab == False:
        for x in sinal:
            if x in desc_empresa:
                Sinalc = True
                # categoria.append('SEM ALCOOL')
                break 
    
    for x in kit:
        if x in desc_empresa:
            KIT = True
            # categoria.append('KIT')
            break 

    if Sinalc == False and Sab == False:  
        for x in pilsen:    
            if x in desc_empresa:
                Pilsen = True
                #categoria.append('PILSEN')
                break

        for x in escura:
            if x in desc_empresa:
                Escura = True
                #categoria.append('ESCURA')
                break

    for x in garrafa:
        if x in desc_empresa:
            GFA = True
            #categoria.append('GFA')
            break


    if GFA == False:
        for x in lata:
            if x in desc_empresa:
                LTA = True
                #categoria.append('LTA')
                break


    

#Busco en la descripcion de los proveedores

    counter = 0
    if Sab == False:
        for w in [l for l in most_common]:
            if w[0].lower() in sabor:
                counter += w[1]
                Sab = True
        if counter/cantidad_descripciones < 0.01:
            Sab = False

    if Sab == False:
        counter = 0
        if Sinalc == False:
            for w in [l for l in most_common]:
                if w[0].lower() in sinal:
                    counter += w[1]
                    Sinalc = True
            if counter/cantidad_descripciones < 0.01:
                Sinalc = False

    counter = 0
    if KIT == False:
        for w in [l for l in most_common]:
            if w[0].lower() in kit:
                counter += w[1]
                KIT = True
        if counter/cantidad_descripciones < 0.01:
            KIT = False

    counter = 0
    if GFA == False and LTA == False:
        for w in [l for l in most_common]:       
            if w[0].lower() in garrafa:
                counter += w[1]
                GFA = True
        if counter/cantidad_descripciones < 0.01:
            GFA = False


    #Si es garrafa busco su cantidad

    cantidad = cervezas_df.loc[cervezas_df['CODIGO_BARRAS'] == codigo_barras, 'CANT_CONTENIDO'].item()


    #cambiar a numero que aparece mas
    #y priorizar aquellos con ML

    if GFA ==True:
        if math.isnan(cantidad):
            cantidad = 0
            for w in [w[0].lower() for w in most_common]:
                x = re.findall('[0-9]+', w)
                if len(x) > 0 and int(x[0]) > cantidad:
                    cantidad = int(x[0])   

        # if cantidad > 355:
        #     categoria.append('ACIMA 355ML')
        #     #print(cantidad, 'ACIMA 355 ML')
        # elif cantidad <= 355 and cantidad > 0:
        #     categoria.append('ATE 355ML')
        #     #print(cantidad , 'ATE 355 ML')

                
    counter = 0
    if LTA == False and GFA == False:
        for w in [l for l in most_common]:
            if w[0].lower() in lata:
                counter += w[1]
                LTA = True
        if counter/cantidad_descripciones < 0.01:
            LTA = False

    counter = 0
    if Escura == False and Sinalc == False and Sab == False:
        for w in [l for l in most_common]:
            if w[0].lower() in escura:
                counter += w[1]
                Escura = True
        if counter/cantidad_descripciones < 0.01:
            Escura = False

    counter = 0    
    if Escura == False and Pilsen == False and Sinalc == False and Sab == False:
        for w in [l for l in most_common]:
            if w[0].lower() in pilsen:
                counter += w[1]
                Pilsen = True
        if counter/cantidad_descripciones < 0.01 and counter < 3:
            Pilsen = False




    if codigo_barras.startswith('789'):
        Nacional = True
    else:
        Import = True


    #Ahora si no es sin alcohol, pilsen, Escura o saborizada, me fijo en la base de AMBEV para ver si es ESPECIAL o PREMIUM

    if Sinalc == False:
        if codigo_barras in cervezas_ambev_df['CODIGO_BARRAS'].values:
            if cervezas_ambev_df.loc[cervezas_ambev_df['CODIGO_BARRAS'] == codigo_barras, 'Segmento 2'].item() == 'ESPECIAIS':
                Especial = True
                Pilsen = False
                Escura = False
                Sab == False
                categoria = 'CERV ESPECIAL'
            elif cervezas_ambev_df.loc[cervezas_ambev_df['CODIGO_BARRAS'] == codigo_barras, 'Segmento 2'].item() == 'PREMIUM':
                Premium = True
                Pilsen = False
                Escura = False
                Sab == False            
                categoria = 'CERV PREMIUM'
            elif cervezas_ambev_df.loc[cervezas_ambev_df['CODIGO_BARRAS'] == codigo_barras, 'Segmento 2'].item() == 'CORE PM':
                Premium = True
                Pilsen = False
                Escura = False
                Sab == False
                categoria = 'CERV PREMIUM'

    if Escura == True:
        categoria = 'CERV ESCURA'
    elif Pilsen == True:
        categoria = 'CERV PILSEN'
    elif Sinalc == True:
        categoria = 'CERV SEM ALCOOL'
    elif Sab == True:
        categoria = 'CERV SABORIZADA'

    if GFA == True:
        categoria = categoria + (' GFA')
    elif LTA == True:
        categoria = categoria + (' LATA')
    
    if Import == True:
        categoria = categoria + (' IMPORTADA')
    elif Nacional == True:
        categoria = categoria + (' NACIONAL')

    if GFA == True and Sinalc == True and cantidad > 355:
        categoria = categoria + ('-ACIMA 355ML')
    elif GFA == True and Sinalc == True and cantidad <= 355 and cantidad > 0:
        categoria = categoria + ('-ATE 355 ML')    

    elif GFA == True and (Pilsen == True or Sab == True) and cantidad > 355:
        categoria = categoria + (' - ACIMA 355ML')
    elif GFA == True and Pilsen == True and cantidad <= 355 and cantidad > 0:
        categoria = categoria + (' - ATE 355 ML') 

    elif GFA == True and Escura == True and Import == True and cantidad <= 355 and cantidad > 0:
        categoria = categoria + (' - ATE 355 ML')
    elif GFA == True and (Sab == True or Premium == True or (Escura == True and Nacional == True)) and cantidad <= 355 and cantidad > 0:
        categoria = categoria + (' - ATE 355ML')
    elif GFA == True and (Escura == True or Sab == True or Premium == True) and cantidad > 355:
        categoria = categoria + (' - ACIMA 355ML')

    if KIT == True:
        categoria = 'KIT' 
        if Pilsen == True:
            categoria = categoria + (' PILSEN')
        elif Premium == True:
            categoria = categoria + (' PREMIUM')
        elif Especial == True:
            categoria = categoria + (' CERVEJA ESPECIAL')
        

    categories = prod_GPA_df.PROD_CLASIF_5.unique()  

    #print(categoria)

    # if categoria not in categories:
    #     categoria = 'OUTROS'        
    
    # print(cantidad)

    return(categoria)



def get_most_common_words(codigo_barras, descrip_cerv_df):
    
    most_common = Counter(" ".join(descrip_cerv_df[descrip_cerv_df['CODIGO_BARRAS'] == codigo_barras]["DESCRIPCION"]).split()).most_common(100)
    most_common
    return most_common




