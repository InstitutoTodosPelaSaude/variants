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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Download data from covid.saude.gov.br",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--dir", required=False, type=str,  help="Download directory")
    parser.add_argument("--download", required=False, nargs=1, type=str, default='yes',
                        choices=['yes', 'no'], help="Download case data from covid.gov.br?")
    parser.add_argument("--file", required=False, help="Compressed .rar file from covid.gov.br")
    parser.add_argument("--output", required=True, help="TSV file with aggregated COVID-19 cases and deaths in Brazil")
    args = parser.parse_args()

    directory = args.dir
    output = args.output
    infile = args.file
    download = args.download
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

    if download == 'yes':
        print('\t- Downloading compressed epidata from covid.saude.gov.br ...')
        s = Service(path + 'config/geckodriver')
        browser = webdriver.Firefox(options=options, service=s)
        # browser = webdriver.Firefox(service=s, options=options)
        browser.get('https://covid.saude.gov.br')
        browser.find_element(By.XPATH, '/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content/div[1]/div[2]/ion-button').click()
        print('\t\t- Done!')
        browser.close()
    else:
        os.system("mkdir " + download_dir)
        os.system("cp " + infile + " " + download_dir)

    # rar file download
    arqbr_rar = ""
    while arqbr_rar == "":
        # arqbr_rar = (','.join(glob.glob(path + 'temp/br_rar/*.rar')))
        arqbr_rar = (','.join(glob.glob(download_dir + '*.zip')))

#    browser.close()
    print('\n\t- Decompressing data files...')
    # patoolib.extract_archive(arqbr_rar, outdir=(path + 'temp/br_rar'))
    patoolib.extract_archive(arqbr_rar, outdir=(download_dir))

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
