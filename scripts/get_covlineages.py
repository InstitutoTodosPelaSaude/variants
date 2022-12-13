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
    parser = argparse.ArgumentParser(
        description="Download data from",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="TSV file listing parental lineages of SARS-CoV-2 variants")
    parser.add_argument("--output", required=True, help="TSV containing the latest WHO variants and their respective pango lineages")
    args = parser.parse_args()

    input = args.input
    output = args.output
    path = os.path.abspath(os.getcwd()) + '/'

    # path = '/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/ITpS/projetos_itps/vigilanciagenomica/analyses/relatorioXX_20221110/'
    # input = path + 'config/cov-lineages_omicron.tsv'
    # output = path + 'config/who_variants_bs4.tsv'

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

    # open output file
    df3 = pd.DataFrame(columns=['pango_lineage', 'who_variant', 'Most common countries', 'Earliest date', '# designated', '# assigned', 'Description', 'WHO Name'])
    special_lineages = {'B.1.1.529': 'BA'}

    dfC = pd.DataFrame()
    for idx, row in df.iterrows():
        who_name = df.loc[idx, 'who_variant']
        pango_name = df.loc[idx, 'cov_variant']
        if len(pango_name) < 12:
            print('\nSublineages of ' + who_name + ' (' + pango_name + '):')

            # initiating the webdriver. Parameter includes the path of the webdriver.
            s = Service(path + 'config/geckodriver')
            browser = webdriver.Firefox(service=s)

            if pango_name in ['X', 'B.1.1.529']:
                url = "https://cov-lineages.org/lineage.html"
            else:
                url = "https://cov-lineages.org/lineage.html?lineage=%s" % pango_name
            browser.get(url)

            # this is just to ensure that the page is loaded
            time.sleep(5)
            html = browser.page_source
            soup = BS(html, 'html.parser')
            table = soup.find_all('table', id='myTable')[0]
            browser.close()

            # open HTML table as pandas dataframe
            df2 = pd.read_html(table.prettify())[0]
            df2 = df2.rename(columns={'Lineage': 'pango_lineage'})

            # Opening JSON file
            url = 'https://raw.githubusercontent.com/cov-lineages/lineages-website/master/_data/lineage_data.json'
            resp = requests.get(url)
            json_data = resp.json()

            # if pango_name in special_lineages.keys():
            if pango_name in special_lineages.keys():
                df2 = df2[df2['Description'].str.contains(pango_name)]

                df2['Description'] = df2['Description'].str.replace('Alias of ', '').str.replace(pango_name, special_lineages[pango_name])
                df2['parental_lineage'] = df2['Description'].str.split(',', expand=True)[0]

                def split_it(text):
                    rgx = re.search(r'([A-Z]+\.\d+)', text)
                    if rgx:
                        return rgx.group(0)
                df2['parental_lineage'] = df2['parental_lineage'].apply(split_it)
                df2 = df2[~df2['pango_lineage'].isin(df3['pango_lineage'].tolist() + [pango_name])]
                # print(df2['pango_lineage'].tolist())

                # crosscheck parental lineages from df2 with variants from df3
                dict_target = pd.Series(df3['who_variant'].values, index=df3['pango_lineage']).to_dict()
                # print(dict_target)

                # add column
                df2.insert(1, 'who_variant', '')
                df2['who_variant'] = df2['parental_lineage'].apply(lambda x: dict_target[x] if x in dict_target else '')
            elif pango_name == 'X':
                df2 = pd.DataFrame(columns=['pango_lineage', 'who_variant'])
                list_lineages = []
                for lineage in json_data:
                    if lineage.startswith('X'):
                        list_lineages.append(lineage)

                se = pd.Series(list_lineages)
                df2['pango_lineage'] = se.values
                df2['who_variant'] = who_name
            else:
                df2 = df2[~df2['pango_lineage'].isin(df3['pango_lineage'].tolist())]
                # dict_target = pd.Series(df3['who_variant'].values, index=df3['pango_lineage']).to_dict()
                df2['who_variant'] = who_name
                # df2['who_variant'] = df2['parental_lineage'].apply(lambda x: dict_target[x] if x in dict_target else '')

            df2 = df2[['who_variant', 'pango_lineage']]
            # print(df2)
            print(', '.join(df2['pango_lineage'].tolist()))
            df3 = pd.concat([df3, df2], ignore_index=True)
        else:
            data = {'who_variant': who_name, 'pango_lineage': pango_name}
            dfC = dfC.append(data, ignore_index=True)
    df3 = pd.concat([df3, dfC], ignore_index=True)


    df3 = df3[['who_variant', 'pango_lineage']]
    df3 = df3.sort_values(by=['who_variant', 'pango_lineage'])
    df3.to_csv(output, sep='\t', index=False)
    print('\nLineage file successfully exported!\n')
