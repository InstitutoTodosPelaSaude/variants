#!/usr/bin/env python
# coding: utf-8
# Created by: @BragatteMAS
# Adapted by: Anderson Brito

# Export automatically data
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

# Libraries
import pandas as pd
import numpy as np
import glob
import os
import os.path
import patoolib
import argparse
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Download data from covid.saude.gov.br",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--dir", required=False, type=str,  help="Download directory")
    parser.add_argument("--download", required=False, nargs=1, type=str, default='yes',
                        choices=['yes', 'no'], help="Download case data from covid.gov.br?")
    parser.add_argument("--file", required=False, help="Compressed .rar file from covid.gov.br")
    parser.add_argument("--driver", required=True, help="Path where the geckodriver is located")
    parser.add_argument("--output", required=True, help="TSV file with aggregated COVID-19 cases and deaths in Brazil")
    args = parser.parse_args()

    directory = args.dir
    output = args.output
    infile = args.file
    download = args.download
    geckodriver = args.driver
    path = os.path.abspath(os.getcwd()) + '/'

    # setting for download
    # download_dir = (path + 'temp/br_rar/')
    if directory in ['', None]:
        directory = ''
    download_dir = path + directory + '/temp/'

    # paths
    # if os.path.exists(path + output) == True:
    #     os.remove(path + output)
    # if os.path.exists(download_dir) == True:
    #     print('found')
    #     os.system("rm -rf " + path + 'temp')

    options = Options()
    options.set_preference('browser.download.folderList', 2)
    options.set_preference('browser.download.dir', download_dir)
    options.set_preference('general.warnOnAboutConfig', False)
    options.set_preference("browser.download.manager.closeWhenDone", True)
    options.set_preference("browser.download.panel.shown", False)
    options.set_preference("browser.download.manager.alertOnEXEOpen", False)
    options.set_preference("browser.download.alwaysOpenPanel", False)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "attachment/csv")
    
    # Avoid opening browser
    # options.add_argument('--headless')

    if download == 'yes':
        print('\t- Downloading compressed epidata from covid.saude.gov.br ...')
        s = Service(path + geckodriver)#'config/geckodriver_mac')
        browser = webdriver.Firefox(options=options, service=s)

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        previous_file_quantity = len(os.listdir(download_dir))

        browser.get('https://covid.saude.gov.br')
        time.sleep(10)
        browser.find_element(By.XPATH, '/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content/div[1]/div[2]/ion-button').click()
        
        # Check if file is already downloaded
        while len(os.listdir(download_dir)) == previous_file_quantity:
            time.sleep(5)
            print('\t\t- Waiting for download to finish...')

        print('\t\t- Done!')
        # browser.quit()
        print('\t- Browser closed!')
    else:
        os.system("mkdir " + download_dir)
        os.system("cp " + infile + " " + download_dir)

    # Get compressed files (.rar or .zip) from download directory
    print('\t- Choosing most recent file...')
    all_files = os.listdir(download_dir)
    compressed_files = [download_dir + file for file in all_files if file.endswith('.rar') or file.endswith('.zip')]

    # Get the most recent file
    most_recent_file = max(compressed_files, key=os.path.getctime)

    print(f'\n\t- Decompressing data files from {most_recent_file}...')
    
    # patoolib.extract_archive(arqbr_rar, outdir=(path + 'temp/br_rar'))
    patoolib.extract_archive(most_recent_file, outdir=(download_dir))

    # create list to append
    li = []
    for arquivo in glob.glob(download_dir + '*.csv'):
        li.append(arquivo)
    # browser.close()

    print('\n\t- Appending data files...')
    # add files from list to tabelas
    tabelas = []
    for arquivo in li:
        tabelas.append(pd.read_csv(arquivo, index_col=None, header=0, sep=';', encoding='utf8'))
    # browser.close()

    # editing file
    covid_br = pd.concat(tabelas, axis=0, ignore_index=True)
    covid_br['municipio'].fillna('', inplace=True)
    covid_br['codmun'].fillna('0', inplace=True)
    covid_br['codRegiaoSaude'].fillna('0', inplace=True)
    covid_br['codmun'] = covid_br['codmun'].astype(int).astype(str)
    covid_br['codRegiaoSaude'] = covid_br['codRegiaoSaude'].astype(int).astype(str)
    covid_br['data'] = pd.to_datetime(covid_br['data'])
    # browser.close()

    # Correct errors from fill 0
    covid_br.replace(np.inf, 0, inplace=True)
    covid_br.replace(-np.inf, 0, inplace=True)

    print('\n\t- Saving single TSV file...\n')
    # Saving TSV file
    covid_br.to_csv(output, sep='\t', index=False)

    # # remove temporary files
    # os.system("rm -rf " + path + 'temp')
