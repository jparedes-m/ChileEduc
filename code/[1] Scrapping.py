# Data scrapping from Chilean Education Ministry

# This code is intended to be run to get the latest data from the Chilean Education Ministry.

# Especifically, this script downloads data for kindergarten, assistance, and school performance.

# The sections are: Educacion Parvularia (Matricula educacion parvularia), Educacion Basica y Media (Asistencia declarada mensual por año), Educacion Basica y Media (Rendimiento por estudiante)

# Written by Jorge Paredes on December 2024

# [0] Importing Libraries
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import time
import os
import requests
os.chdir("C:/Users/jparedesm/Dropbox/ChileEduc")

# [1] Kinder garden data
download_directory = r"C:/Users/jparedesm/Dropbox/ChileEduc/data/raw/kinder"
## Set up navigator options
chrome_options = Options()
chrome_options.add_experimental_option('prefs',  {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True
    }
)

## Set up WebDriver
driver = webdriver.Chrome(options=chrome_options)
driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_directory}}
command_result = driver.execute("send_command", params)

## Navigate to the website
url = "https://datosabiertos.mineduc.cl/matricula-educacion-parvularia/"
driver.get(url)
time.sleep(2)

## Parse the website
html_content = driver.page_source
soup = BeautifulSoup(html_content, 'html.parser')
links = soup.find_all('a')

## Define function for downloading files
def download_file(link):
    if link.get('href') is not None:
        if ".zip" in link.get('href') or ".rar" in link.get('href'):
            download_url = link.get('href')
            response = requests.get(download_url)
            file_name = os.path.join(download_directory, download_url.split("/")[-1])
            with open(file_name, 'wb') as file:
                file.write(response.content)
                
## Download the .zip and .rar files in parallel
with ThreadPoolExecutor(max_workers=12) as executor:
    executor.map(download_file, links)

driver.quit()

dir_path = "C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\raw\\kinder"

def rename_file(file):
    # Split the filename on hyphen character
    parts = file.split('-')
    # Get the year and extension
    year_ext = parts[-1]
    # Construct the new filename
    new_filename = f"Parvularia_{year_ext}"
    # Get the full path of the current file
    old_file_path = os.path.join(dir_path, file)
    # Get the full path of the new file
    new_file_path = os.path.join(dir_path, new_filename)
    # Rename the file
    os.rename(old_file_path, new_file_path)

# List all files in the directory
files = os.listdir(dir_path)

# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=12) as executor:
    # Use the executor to rename the files in parallel
    executor.map(rename_file, files)


# [2] Assistance data

download_directory = r"C:/Users/jparedesm/Dropbox/ChileEduc/data/raw/assistance"

## Set up navigator options
chrome_options = Options()
chrome_options.add_experimental_option('prefs',  {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True
    }
)

## Set up WebDriver
driver = webdriver.Chrome(options=chrome_options)
driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_directory}}
command_result = driver.execute("send_command", params)

## Navigate to the website
url = "https://datosabiertos.mineduc.cl/asistencia-declarada-mensual-2/"
driver.get(url)
time.sleep(2)

## Parse the website
html_content = driver.page_source
soup = BeautifulSoup(html_content, 'html.parser')
links = soup.find_all('a')

## Download the .zip and .rar files
with ThreadPoolExecutor(max_workers=12) as executor:
    executor.map(download_file, links)

driver.quit()

# Define the directory path
dir_path = "C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\raw\\assistance"

# Map of Spanish month names to English month names
month_map = {
    'enero': '01',
    'febrero': '02',
    'marzo': '03',
    'abril': '04',
    'mayo': '05',
    'junio': '06',
    'julio': '07',
    'agosto': '08',
    'septiembre': '09',
    'octubre': '10',
    'noviembre': '11',
    'diciembre': '12'
}

# Function to rename a file
def rename_file(file):
    # Split the filename on hyphen character
    parts = file.lower().split('-')
    # Get the year
    year = parts[-1].split('.')[0]
    # Get the month in Spanish
    month_spanish = parts[-2]
    # Get the month in English
    month_english = month_map[month_spanish]
    # Get the extension
    ext = parts[-1].split('.')[1]
    # Construct the new filename
    new_filename = f"Assistance_{year}_{month_english}.{ext}"
    # Get the full path of the current file
    old_file_path = os.path.join(dir_path, file)
    # Get the full path of the new file
    new_file_path = os.path.join(dir_path, new_filename)
    # Rename the file
    os.rename(old_file_path, new_file_path)

# List all files in the directory
files = os.listdir(dir_path)

# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=12) as executor:
    # Use the executor to rename the files in parallel
    executor.map(rename_file, files)

# [3] Performance data
download_directory = r"C:/Users/jparedesm/Dropbox/ChileEduc/data/raw/performance"

## Set up navigator options
chrome_options = Options()
chrome_options.add_experimental_option('prefs',  {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True
    }
)

## Set up WebDriver
driver = webdriver.Chrome(options=chrome_options)
driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_directory}}
command_result = driver.execute("send_command", params)

## Navigate to the website
url = "https://datosabiertos.mineduc.cl/rendimiento-por-estudiante-2/"
driver.get(url)
time.sleep(2)

## Parse the website
html_content = driver.page_source
soup = BeautifulSoup(html_content, 'html.parser')
links = soup.find_all('a')

## Download the .zip and .rar files
with ThreadPoolExecutor(max_workers=12) as executor:
    executor.map(download_file, links)

driver.quit()

dir_path = "C:\\Users\\jparedesm\\Dropbox\\ChileEduc\\data\\raw\\performance"

def rename_file(file):
    # Split the filename on hyphen character
    parts = file.split('-')
    # Get the year and extension
    year_ext = parts[-1]
    # Construct the new filename
    new_filename = f"Performance_{year_ext}"
    # Get the full path of the current file
    old_file_path = os.path.join(dir_path, file)
    # Get the full path of the new file
    new_file_path = os.path.join(dir_path, new_filename)
    # Rename the file
    os.rename(old_file_path, new_file_path)

# List all files in the directory
files = os.listdir(dir_path)

# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=12) as executor:
    # Use the executor to rename the files in parallel
    executor.map(rename_file, files)


# End of File