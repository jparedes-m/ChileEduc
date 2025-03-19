# Data unification from Chilean Education Ministry

# This code is intended to get every database to a unified format (same columns, same names, same order).

# IMPORTANT: THIS CODE IS ONLY MEANT TO BE RUN ONCE IN THE WORKFLOW BECAUSE IT OVERWRITES THE ORIGINAL DATA.

# IMPORTANT: THIS CODE TAKES A LONG TIME AND RESOURCES. IT IS RECOMMENDED TO RUN IT ON A POWERFUL MACHINE.

# Written by Jorge Paredes on December 2024

# [0] Preamble ----
import os
import time
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
os.chdir("C:/Users/jparedesm/Dropbox/ChileEduc")

# [1] Kinder Data

mapping_df = pd.read_excel('data/data mapping.xlsx', sheet_name='kinder')

def clean_kinder_data(input_file, mapping_year, map_df, output_file):
    # Crear un diccionario con old_name y new_name
    mapping_dict = map_df.set_index(mapping_year)["desired_names"].to_dict()
    
    # Leer el archivo de datos
    df = pd.read_csv(input_file, sep=';')
    
    # Renombrar columnas según el diccionario
    df.rename(columns={old_name: new_name for old_name, new_name in mapping_dict.items() if old_name in df.columns}, inplace=True)
    
    # Asegurar que todas las columnas deseadas están presentes y en orden
    all_desired_names = map_df["desired_names"].dropna().tolist()
    df = df.reindex(columns=all_desired_names)
    
    # Convertir los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()
    
    # Filtrar por COD_ENSE1_M == 10 (Quitar Educacion Especial)
    if mapping_year < 2012:
        df = df[df["cod_ense1_m"] == 10]
    else:
        df = df[df["cod_ense2_m"] == 1]
    
    # Filtrar solo "Jornada Completa" o "Jornada Manana" desde 2012
    if mapping_year >= 2012:
        df = df[(df["cod_jor_j"] == 1) | (df["cod_jor_j"] == 2)]
    
    # Guardar el resultado en el archivo de salida con separador punto y coma
    df.to_csv(output_file, sep=';', index=False, encoding="utf-8")

for year in range(2011, 2024):
    clean_kinder_data(
        input_file=f'data/intermediate/kinder/kinder_{year}.csv',
        mapping_year=year,
        map_df=mapping_df,
        output_file=f'data/intermediate/kinder/kinder_{year}.csv'
    )

# [2] Assistance Data
mapping_df = pd.read_excel('data/data mapping.xlsx', sheet_name='assistance')

def clean_assistance_data(input_file, mapping_year, map_df, output_file):
    # Crear un diccionario con old_name y new_name
    mapping_dict = map_df.set_index(mapping_year)["desired_names"].to_dict()

    # Leer el archivo de datos
    df = pd.read_csv(input_file, sep=';', encoding='latin1')

    # Renombrar columnas según el diccionario
    df.rename(columns={old_name: new_name for old_name, new_name in mapping_dict.items() if old_name in df.columns}, inplace=True)

    # Asegurar que todas las columnas deseadas están presentes y en orden
    all_desired_names = map_df["desired_names"].dropna().tolist()
    df = df.reindex(columns=all_desired_names)

    # Convertir los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()
    
    # Filtrar tipo de enseñanza (quitar educacion especial y educacion a adultos) cod_ense (ANEXO II)
    df = df[(df["cod_ense"] == 10) | (df["cod_ense"] == 110) | (df["cod_ense"] == 310) | (df["cod_ense"] == 410) | (df["cod_ense"] == 510) | (df["cod_ense"] == 610) | (df["cod_ense"] == 710) | (df["cod_ense"] == 810) | (df["cod_ense"] == 910)]
    
    # Guardar el resultado en el archivo de salida
    df.to_csv(output_file, sep=';', index=False, encoding="utf-8")

    # Liberar memoria
    del df

def process_year_month(year, month, map_df):
    year_month = f"{year}_{month:02d}"
    input_file = f'data/intermediate/assistance/assistance_{year_month}.csv'
    output_file = f'data/intermediate/assistance/assistance_{year_month}.csv'

    # Verificar que el archivo existe antes de procesarlo
    try:
        clean_assistance_data(
            input_file=input_file,
            mapping_year=year_month,
            map_df=map_df,
            output_file=output_file
        )
    except FileNotFoundError:
        print(f"Archivo no encontrado: {input_file}")
    except Exception as e:
        print(f"Error procesando {input_file}: {e}")

# Procesamiento en paralelo con ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    futures = []
    for year in range(2011, 2024):
        for month in range(3, 13):
            futures.append(executor.submit(process_year_month, year, month, mapping_df))

    # Esperar que todos los hilos terminen
    for future in futures:
        future.result()

# [3] Performance Data
mapping_df = pd.read_excel('data/data mapping.xlsx', sheet_name='performance')

def clean_performance_data(input_file, mapping_year, map_df, output_file):
    # Crear un diccionario con old_name y new_name
    mapping_dict = map_df.set_index(mapping_year)["desired_names"].to_dict()

    # Leer el archivo de datos
    df = pd.read_csv(input_file, sep=';', encoding='latin1')

    # Renombrar columnas según el diccionario
    df.rename(columns={old_name: new_name for old_name, new_name in mapping_dict.items() if old_name in df.columns}, inplace=True)

    # Asegurar que todas las columnas deseadas están presentes y en orden
    all_desired_names = map_df["desired_names"].dropna().tolist()
    df = df.reindex(columns=all_desired_names)

    # Convertir los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()
    
    # Filtrar tipo de enseñanza (quitar educacion especial y educacion a adultos) cod_ense (ANEXO II)
    df = df[(df["cod_ense"] == 10) | (df["cod_ense"] == 110) | (df["cod_ense"] == 310) | (df["cod_ense"] == 410) | (df["cod_ense"] == 510) | (df["cod_ense"] == 610) | (df["cod_ense"] == 710) | (df["cod_ense"] == 810) | (df["cod_ense"] == 910)]

    # Guardar el resultado en el archivo de salida
    df.to_csv(output_file, sep=';', index=False, encoding="utf-8")

    # Liberar memoria
    del df

# Procesamiento secuencial para los años
for year in range(2002, 2024):
    try:
        clean_performance_data(
            input_file=f'data/intermediate/performance/performance_{year}.csv',
            mapping_year=year,
            map_df=mapping_df,
            output_file=f'data/intermediate/performance/performance_{year}.csv'
        )
    except FileNotFoundError:
        print(f"Archivo no encontrado: data/intermediate/performance/performance_{year}.csv")
    except Exception as e:
        print(f"Error procesando el archivo performance_{year}: {e}")