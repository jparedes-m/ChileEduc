# Data concatenation from Chilean Education Ministry

# This code is intended to do a bind row using arrow package to concatenate all the data from the Chilean Education Ministry.

# IMPORTANT: THIS CODE TAKES A LONG TIME AND RESOURCES. IT IS RECOMMENDED TO RUN IT ON A POWERFUL MACHINE.

# The output made by this script is 3 'clean' files in parquet/csv format. 

# Written by Jorge Paredes on December 2024

# [0] Preamble ----
import os

# [1] Kinder ----
input_folder = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/intermediate/kinder'  # Carpeta con los CSVs
output_file = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Kinder_.csv'  # Archivo de salida

# Obtener lista de archivos CSV
csv_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.csv')]

# Verificar que haya archivos
if not csv_files:
    raise FileNotFoundError("No se encontraron archivos CSV en la carpeta especificada.")

# Combinar archivos sin paralelismo (más simple y robusto)
with open(output_file, 'w', encoding='utf-8', errors='replace') as outfile:
    for i, file in enumerate(csv_files):
        with open(file, 'r', encoding='utf-8', errors='replace') as infile:
            for j, line in enumerate(infile):
                if i > 0 and j == 0:  # Saltar la cabecera de archivos excepto el primero
                    continue
                outfile.write(line)

# [2] Assistance ----
input_folder = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/intermediate/assistance'  # Carpeta con los CSVs
output_file = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Assistance.csv'  # Archivo de salida

# Obtener lista de archivos CSV
csv_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.csv')]

# Verificar que haya archivos
if not csv_files:
    raise FileNotFoundError("No se encontraron archivos CSV en la carpeta especificada.")

# Combinar archivos sin paralelismo (más simple y robusto)
with open(output_file, 'w', encoding='utf-8', errors='replace') as outfile:
    for i, file in enumerate(csv_files):
        with open(file, 'r', encoding='utf-8', errors='replace') as infile:
            for j, line in enumerate(infile):
                if i > 0 and j == 0:  # Saltar la cabecera de archivos excepto el primero
                    continue
                outfile.write(line)

# [3] Performance ----
input_folder = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/intermediate/performance'  # Carpeta con los CSVs
output_file = 'C:/Users/jparedesm/Dropbox/ChileEduc/data/clean/Performance.csv'  # Archivo de salida

# Obtener lista de archivos CSV
csv_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.csv')]

# Verificar que haya archivos
if not csv_files:
    raise FileNotFoundError("No se encontraron archivos CSV en la carpeta especificada.")

# Combinar archivos sin paralelismo (más simple y robusto)
with open(output_file, 'w', encoding='utf-8', errors='replace') as outfile:
    for i, file in enumerate(csv_files):
        with open(file, 'r', encoding='utf-8', errors='replace') as infile:
            for j, line in enumerate(infile):
                if i > 0 and j == 0:  # Saltar la cabecera de archivos excepto el primero
                    continue
                outfile.write(line)
# Enf Of File