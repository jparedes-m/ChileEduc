# Data extraction from Chilean Education Ministry

# This code is intended to be the second to run after the scrapping script to extract the data from the downloaded files.

# Especifically, this script extracts all the csv or excel files for each year (or year and month) of each database (kinder, assistance, and performance).

# Written by Jorge Paredes on April 2024

# [0] Importing Libraries
import os
import re
import zipfile
import shutil
import patoolib
import time
from concurrent.futures import ThreadPoolExecutor
import tempfile
source_dir = "C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\raw"
dest_dir = "C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\intermediate"

# [1] Kinder

# List all the zip and rar files in the directory
zip_files = [os.path.join(source_dir+"\\kinder", f) for f in os.listdir(source_dir+"\\kinder") if f.endswith('.zip')]
rar_files = [os.path.join(source_dir+"\\kinder", f) for f in os.listdir(source_dir+"\\kinder") if f.endswith('.rar')]
files = [*zip_files, *rar_files]

def extract_file(file_path, dest_dir, exercise):
    if file_path.endswith('.zip'):
        # Open the zip file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Loop over each file in the zip file
            for file in zip_ref.namelist():
                # If the file is a CSV or Excel file
                if file.endswith('.csv') or file.endswith('.xlsx'):
                    # Extract the file to the destination directory
                    zip_ref.extract(file, dest_dir + exercise)
    elif file_path.endswith('.rar'):
        # Open the rar file
        patoolib.extract_archive(file_path, outdir=os.path.join(dest_dir, "del"))
        source_files = [os.path.join(dest_dir, "del", f) for f in os.listdir(os.path.join(dest_dir, "del")) if f.endswith('.csv') or f.endswith('.xlsx')]
        shutil.copy(source_files[0], dest_dir + exercise)
        time.sleep(3)
        for _ in range(5):  # Retry up to 5 times
                        try:
                            shutil.rmtree(dest_dir + "\\del")
                            break  # If the call succeeds, break the loop
                        except PermissionError:
                            time.sleep(1)
    else:
        return

for file in files:
    extract_file(file, dest_dir, "\\kinder")


# Rename the files:
kinder_files = os.listdir(dest_dir + "\\kinder")

for file in kinder_files:
    # Extract the year from the filename
    match = re.search(r'\d{4}', file)
    if match:
        year = match.group(0)
        # Construct the new filename
        new_filename = f"kinder_{year}.csv"
        # Rename the file
        os.rename(os.path.join(dest_dir+"\\kinder", file), os.path.join(dest_dir+"\\kinder", new_filename))

# [2] Assistance data
zip_files = [os.path.join(source_dir+"\\assistance", f) for f in os.listdir(source_dir+"\\assistance") if f.endswith('.zip')]
rar_files = [os.path.join(source_dir+"\\assistance", f) for f in os.listdir(source_dir+"\\assistance") if f.endswith('.rar')]
files = [*zip_files, *rar_files]

def extract_and_flatten(file_path, dest_dir):
    """
    Extract files from zip or rar, and flatten nested structures to find .csv files.
    """
    temp_dir = tempfile.mkdtemp()  # Crear un directorio temporal

    def process_nested(path):
        """
        Process nested structures: extract zip/rar, or move CSV files to final destination.
        """
        if os.path.isfile(path):  # Verifica si es un archivo
            if path.lower().endswith('.csv'):  # Insensible a mayúsculas/minúsculas
                # Mueve el archivo CSV al destino final
                dest_file_path = os.path.join(dest_dir, os.path.basename(path))
                shutil.move(path, dest_file_path)
                print(f"File extracted: {dest_file_path}")
            elif path.lower().endswith('.zip'):  # Si es un ZIP anidado, procesarlo
                nested_temp_dir = tempfile.mkdtemp()
                with zipfile.ZipFile(path, 'r') as nested_zip:
                    nested_zip.extractall(nested_temp_dir)
                    for nested_file in os.listdir(nested_temp_dir):
                        process_nested(os.path.join(nested_temp_dir, nested_file))
                shutil.rmtree(nested_temp_dir)
            elif path.lower().endswith('.rar'):  # Si es un RAR anidado, procesarlo
                nested_temp_dir = tempfile.mkdtemp()
                patoolib.extract_archive(path, outdir=nested_temp_dir)
                for nested_file in os.listdir(nested_temp_dir):
                    process_nested(os.path.join(nested_temp_dir, nested_file))
                shutil.rmtree(nested_temp_dir)  # Limpiar el directorio temporal

        elif os.path.isdir(path):  # Si es un directorio, explora su contenido
            for nested_file in os.listdir(path):
                process_nested(os.path.join(path, nested_file))

    try:
        if file_path.lower().endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                # Extraer todos los contenidos al directorio temporal
                zip_ref.extractall(temp_dir)
                for item in os.listdir(temp_dir):
                    process_nested(os.path.join(temp_dir, item))

        elif file_path.lower().endswith('.rar'):
            # Extraer contenido de archivos .rar
            patoolib.extract_archive(file_path, outdir=temp_dir)
            for item in os.listdir(temp_dir):
                process_nested(os.path.join(temp_dir, item))

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

    finally:
        # Eliminar el directorio temporal
        shutil.rmtree(temp_dir)
        
# Process files sequentially using a simple for loop
for file in files:
    print(f"Processing: {file}")
    extract_and_flatten(file, dest_dir+"\\assistance")
   
# Rename the files:
target_dir = r"C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\intermediate\\assistance"
months = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
}
for file in os.listdir(target_dir):
    # Convertir el nombre del archivo a minúsculas
    file_lower = file.lower()

    # Buscar la posición de "asistencia_"
    asistencia_index = file_lower.find("asistencia_")
    if asistencia_index == -1:
        print(f"Skipped (no 'asistencia_' found): {file}")
        continue

    # Extraer la parte relevante del nombre del archivo
    parts = file_lower[asistencia_index + len("asistencia_"):].split("_")
    if len(parts) < 2:
        print(f"Skipped (invalid format after 'asistencia_'): {file}")
        continue

    month_name = parts[0]  # Nombre del mes
    year = parts[1][:4]    # Primeros 4 caracteres después del segundo guion bajo

    # Convertir el mes a número
    month = months.get(month_name)
    if not month:
        print(f"Skipped (invalid month name): {file}")
        continue

    # Construir el nuevo nombre
    new_filename = f"assistance_{year}_{month}.csv"

    # Renombrar el archivo
    old_path = os.path.join(target_dir, file)
    new_path = os.path.join(target_dir, new_filename)

    # Manejar posibles colisiones de nombres
    counter = 1
    while os.path.exists(new_path):
        new_filename = f"assistance_{month}_{year}_{counter}.csv"
        new_path = os.path.join(target_dir, new_filename)
        counter += 1

    os.rename(old_path, new_path)
    print(f"Renamed: {old_path} -> {new_path}")


# [3] Performance data
zip_files = [os.path.join(source_dir+"\\performance", f) for f in os.listdir(source_dir+"\\performance") if f.endswith('.zip')]
rar_files = [os.path.join(source_dir+"\\performance", f) for f in os.listdir(source_dir+"\\performance") if f.endswith('.rar')]
files = [*zip_files, *rar_files]

def extract_file(file_path, dest_dir, exercise):
    if file_path.endswith('.zip'):
        # Open the zip file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Loop over each file in the zip file
            for file in zip_ref.namelist():
                # If the file is a CSV or Excel file
                if file.endswith('.csv') or file.endswith('.xlsx'):
                    # Extract the file to the destination directory
                    zip_ref.extract(file, dest_dir + exercise)
    elif file_path.endswith('.rar'):
        # Open the rar file
        patoolib.extract_archive(file_path, outdir=os.path.join(dest_dir, "del"))
        source_files = [os.path.join(dest_dir, "del", f) for f in os.listdir(os.path.join(dest_dir, "del")) if (f.endswith('.csv') or f.endswith('.xlsx')) and not f.startswith('Frecuencia')]
        shutil.copy(source_files[0], dest_dir + exercise)
        time.sleep(3)
        for _ in range(5):  # Retry up to 5 times
            try:
                shutil.rmtree(dest_dir + "\\del")
                break  # If the call succeeds, break the loop
            except PermissionError:
                time.sleep(1)
        
    else:
        return

for file in files:
    extract_file(file, dest_dir, "\\performance")
    
# Rename the files
def rename_performance_files(file_list, dest_dir):
    """
    Renames files from a list to the format performance_YYYY, where YYYY is the year,
    and moves them to the destination directory.
    """
    for file_path in file_list:
        # Extract the file name from the path
        file_name = os.path.basename(file_path)
        file_lower = file_name.lower()

        # Match patterns like "rendimiento_YYYY" or "rendimiento por estudiante YYYY"
        match = re.search(r'rendimiento(?:_|\s+por\s+estudiante\s+)?(\d{4})', file_lower)

        if match:
            # Extract the year
            year = match.group(1)

            # Construct the new file name
            new_file_name = f"performance_{year}"

            # Add the original file extension
            ext = os.path.splitext(file_name)[1]  # Keep the original extension
            new_file_name += ext

            # Move and rename the file
            dest_file_path = os.path.join(dest_dir, new_file_name)
            os.rename(file_path, dest_file_path)
            print(f"Renamed: {file_path} -> {dest_file_path}")
        else:
            print(f"Skipped: {file_path} (no match found)")

# List all files in the directory
performance_files = [os.path.join(dest_dir+"\\performance", f) for f in os.listdir(dest_dir+"\\performance")]

# Rename the files
dest_dir = r"C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\intermediate\\performance"

# Call the function
rename_performance_files(performance_files, dest_dir)