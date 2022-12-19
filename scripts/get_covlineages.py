#!/opt/anaconda3/envs/diag/bin/python3

# Created by: Anderson Brito
# Email: andersonfbrito@gmail.com
# Release date: 2022-09-19
# Last update: : 2022-11-11

from bs4 import BeautifulSoup as BS
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
import time
import argparse
import os
import re
import requests
import json

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

pd.set_option('display.max_columns', 500)

if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #     description="Download data from",
    #     formatter_class=argparse.ArgumentDefaultsHelpFormatter
    # )
    # parser.add_argument("--input", required=True, help="TSV file listing parental lineages of SARS-CoV-2 variants")
    # parser.add_argument("--output", required=True, help="TSV containing the latest WHO variants and their respective pango lineages")
    # args = parser.parse_args()
    #
    # input = args.input
    # output = args.output
    # path = os.path.abspath(os.getcwd()) + '/'

    path = '/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/ITpS/projetos_itps/vigilanciagenomica/analyses/test/'
    input = path + 'config/cov-lineages.tsv'
    output = path + 'config/who_variants_json.tsv'

    def load_table(file):
        df = ''
        if str(file).split('.')[-1] == 'tsv':
            separator = '\t'
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] == 'csv':
            separator = ','
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] in ['xls', 'xlsx']:
            df = pd.read_excel(file, index_col=None, header=0, sheet_name=0, dtype='str')
            df.fillna('', inplace=True)
        else:
            print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX')
            exit()
        return df

    # open list of variants
    df = load_table(input)

    # Opening JSON file
    url = 'https://raw.githubusercontent.com/cov-lineages/lineages-website/master/_data/lineage_data.json'
    resp = requests.get(url)
    json_data = resp.json()

    # cov-lineages data
    dfJ = pd.DataFrame()
    for lineage in json_data:
        entry = {k: json_data[lineage][k] for k in ['Lineage', 'Description']}
        dfJ = dfJ.append(entry, ignore_index=True)
    dfJ = dfJ.rename(columns={'Lineage': 'pango_lineage'})
    # print(dfJ)

    # Alias data
    url2 = 'https://raw.githubusercontent.com/cov-lineages/pango-designation/master/pango_designation/alias_key.json'
    resp2 = requests.get(url2)
    alias_data = resp2.json()

    special_lineages = {}
    inv_special = {}
    for l in alias_data:
        if not l.startswith('X'):
            if alias_data[l] != '':
                special_lineages[l] = alias_data[l]
                inv_special[alias_data[l]] = l

    # open output file
    df3 = pd.DataFrame(columns=['who_variant', 'pango_lineage'])
    for idx, row in df.iterrows():
        df2 = pd.DataFrame()
        who_name = df.loc[idx, 'who_variant']
        pango_name = df.loc[idx, 'cov_variant']
        if pango_name in dfJ['pango_lineage'].tolist():
            print('\nSublineages of ' + who_name + ' (' + pango_name + '):')

            # if pango_name.startswith('X'): # recombinant
            #     base = pd.DataFrame(columns=['pango_lineage', 'Description'])
            #     root = dfJ[dfJ['pango_lineage'].str.startswith(pango_name)]
            #
            #     frames = [base, root.sort_values(by='pango_lineage')]
            #     df2 = pd.concat(frames)
            #     df2['who_variant'] = who_name
            #
            #     df2 = df2[['pango_lineage', 'who_variant']]
            # else:
            root = dfJ.loc[dfJ['pango_lineage'] == pango_name] # root lineage
            desc = dfJ[dfJ['pango_lineage'].str.startswith(pango_name + '.')] # lineage descendants
            alias = pd.DataFrame(columns=['pango_lineage', 'Description'])

            pango_alias = '.'.join(pango_name.split('.')[0:-1])
            pango_sub = pango_name.split('.')[-1]

            prefix = ''
            if pango_alias in special_lineages.keys():
                prefix = special_lineages[pango_alias]
                alias = dfJ[dfJ['Description'].str.contains(prefix + '.' + pango_sub)]
                alias = alias[~alias['pango_lineage'].isin(root['pango_lineage'].tolist() + desc['pango_lineage'].tolist())]
            elif pango_name in inv_special: # inverted special_lineages
                prefix = inv_special[pango_name]
                alias = dfJ[dfJ['Description'].str.contains('Alias of ' + pango_name + '.')]
                print(alias['pango_lineage'].tolist())

            frames = [root, desc.sort_values(by='pango_lineage'), alias.sort_values(by='pango_lineage')]
            # print('root: ' + str(root['pango_lineage'].tolist()))
            # print('desc: ' + str(desc['pango_lineage'].tolist()))
            # print('alias: ' + str(alias['pango_lineage'].tolist()))

            df2 = pd.concat(frames)

            # add column
            df2['who_variant'] = who_name
            df2 = df2[['pango_lineage', 'who_variant']]
            df2 = df2[~df2['pango_lineage'].isin(df3['pango_lineage'].tolist())]
            print(', '.join(df2['pango_lineage'].tolist()))
        else:
            if pango_name.startswith('X'):  # recombinant
                print('\nSublineages of ' + who_name + ' (' + pango_name + '):')

                base = pd.DataFrame(columns=['pango_lineage', 'Description'])
                root = dfJ[dfJ['pango_lineage'].str.startswith(pango_name)]

                frames = [base, root.sort_values(by='pango_lineage')]
                df2 = pd.concat(frames)
                df2['who_variant'] = who_name

                df2 = df2[['pango_lineage', 'who_variant']]
                print(', '.join(df2['pango_lineage'].tolist()))

        df3 = pd.concat([df3, df2], ignore_index=True)


    df3 = df3.sort_values(by=['who_variant', 'pango_lineage'])
    df3.to_csv(output, sep='\t', index=False)
    print('\nLineage file successfully exported!\n')
