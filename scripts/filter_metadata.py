# -*- coding: utf-8 -*-

# Created by: Anderson Brito
# Email: andersonfbrito@gmail.com
# Release date: 2020-03-24
# Last update: 2021-11-13


import pycountry_convert as pyCountry
import pycountry
from Bio import SeqIO
import pandas as pd
from epiweeks import Week
import time
from difflib import SequenceMatcher
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Filter nextstrain metadata files re-formmating and exporting only selected lines",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--genomes", required=True, help="FASTA file genomes to be used")
    parser.add_argument("--metadata1", required=True, help="Metadata file from NextStrain")
    parser.add_argument("--metadata2", required=False, help="Custom lab metadata file")
    parser.add_argument("--variants", required=True, help="Variant-lineage TSV file")
    parser.add_argument("--geography", required=True, help="TSV file with location names to be fixed")
    parser.add_argument("--pango", required=True, help="Pango lineage report output in CSV")
    parser.add_argument("--consortia", required=True, help="Lab-Consoritum TSV file")
    parser.add_argument("--filter", required=False, nargs='+', type=str,
                        help="List of filters for tagged rows in lab metadata")
    parser.add_argument("--output1", required=True, help="Filtered metadata file")
    parser.add_argument("--output2", required=True, help="Reformatted, final FASTA file")
    args = parser.parse_args()

    genomes = args.genomes
    metadata1 = args.metadata1
    metadata2 = args.metadata2
    variants_list = args.variants
    geo_file = args.geography
    pango_file = args.pango
    consortia_list = args.consortia
    filterby = args.filter
    output1 = args.output1
    output2 = args.output2

    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/dashboard/nextstrain/run9_20211115_itps6/'
    # genomes = path + 'pre-analyses/temp_sequences.fasta'
    # metadata1 = path + 'pre-analyses/metadata_nextstrain.tsv'
    # metadata2 = path + 'pre-analyses/sequencing_metadata.xlsx'
    # variants_list = path + 'config/who_variants.tsv'
    # geo_file = path + 'config/fix_geography.tsv'
    # pango_file = path + 'pre-analyses/lineage_report.csv'
    # consortia_list = path + 'config/consortia.tsv'
    # filterby = ['caribe', 'test']
    # output1 = path + 'pre-analyses/metadata_filtered.tsv'
    # output2 = path + 'pre-analyses/sequences.fasta'

    # temporal boundaries
    today = time.strftime('%Y-%m-%d', time.gmtime())
    min_date = '2019-12-15'


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


    variants = {}
    for line in open(variants_list, "r").readlines()[1:]:
        variant, lineage = line.strip().split('\t')
        varlin = variant + ' (' + lineage + ')'
        variants[lineage] = varlin

    lineages = {}
    if pango_file not in ['', None]:
        dfP = load_table(pango_file)
        dfP = dfP[['taxon', 'lineage']]
#         dfP['taxon'] = dfP['taxon'].str.replace('hCoV-19/', '')
        for idx, row in dfP.iterrows():
            strain_name = dfP.loc[idx, 'taxon']
            pango_lineage = dfP.loc[idx, 'lineage']
            if pango_lineage != 'None':
                lineages[strain_name] = pango_lineage

    # fix geography
    geography = {'division': {}, 'location': {}}
    if geo_file not in ['', None]:
        dfG = load_table(geo_file)
        for idx, row in dfG.iterrows():
            column_name = dfG.loc[idx, 'column']
            old_name = dfG.loc[idx, 'old']
            new_name = dfG.loc[idx, 'new']
            geography[column_name][old_name] = new_name

    consortia = {}
    for line in open(consortia_list, "r").readlines()[1:]:
        lab, initiative = line.strip().split('\t')
        consortia[lab] = initiative

    # get ISO alpha3 country codes
    isos = {}
    def get_iso(country):
        global isos
        if country not in isos.keys():
            try:
                isoCode = pyCountry.country_name_to_country_alpha3(country, cn_name_format="default")
                isos[country] = isoCode
            except:
                try:
                    isoCode = pycountry.countries.search_fuzzy(country)[0].alpha_3
                    isos[country] = isoCode
                except:
                    isos[country] = ''
        return isos[country]


    # create epiweek column
    def get_epiweeks(date):
        date = pd.to_datetime(date)
        epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
        epiweek = epiweek[:4] + '_' + 'EW' + epiweek[-2:]
        return epiweek


    # add state code
    br_state_abbrev = {
        'Acre': 'AC',
        'Alagoas': 'AL',
        'Amapá': 'AP',
        'Amapa': 'AP',
        'Amazonas': 'AM',
        'Amazonas BR': 'AM',
        'Bahia': 'BA',
        'Ceará': 'CE',
        'Distrito Federal': 'DF',
        'Espírito Santo': 'ES',
        'Espirito Santo': 'ES',
        'Goiás': 'GO',
        'Maranhão': 'MA',
        'Mato Grosso': 'MT',
        'Mato Grosso do Sul': 'MS',
        'Minas Gerais': 'MG',
        'Pará': 'PA',
        'Para': 'PA',
        'Paraíba': 'PB',
        'Paraiba': 'PB',
        'Paraná': 'PR',
        'Parana': 'PR',
        'Pernambuco': 'PE',
        'Piauí': 'PI',
        'Piaui': 'PI',
        'Rio de Janeiro': 'RJ',
        'Rio Grande do Norte': 'RN',
        'Rio Grande do Sul': 'RS',
        'Rondônia': 'RO',
        'Rondonia': 'RO',
        'Roraima': 'RR',
        'Santa Catarina': 'SC',
        'São Paulo': 'SP',
        'Sergipe': 'SE',
        'Tocantins': 'TO'
    }

    # nextstrain metadata
    # dfN = pd.read_csv(metadata1, encoding='utf-8', sep='\t', dtype='str')
    dfN = load_table(metadata1)
    dfN = dfN.rename(columns={'pangolin_lineage': 'pango_lineage'})
#     dfN['strain'] = dfN['strain'].str.replace('hCoV-19/', '')

    new_columns = ['code', 'who_variant', 'variant_lineage']
    for col in new_columns:
        if col in dfN.columns.tolist():
            dfN = dfN.drop(columns=[col])
    dfN.insert(4, 'code', '')
    dfN.insert(1, 'who_variant', '')
    dfN.insert(1, 'variant_lineage', '')
    dfN.insert(1, 'consortium', '')
    dfN.fillna('', inplace=True)

    # filter genomes based on date completeness
    dfN = dfN[dfN['date'].apply(lambda x: len(x.split('-')) == 3)]  # accept only full dates
    dfN = dfN[dfN['date'].apply(lambda x: 'X' not in x)]  # exclude -XX-XX missing dates

    # convert date column to datetime type
    dfN['date'] = pd.to_datetime(dfN['date'], errors='ignore')
    dfN['date_submitted'] = pd.to_datetime(dfN['date_submitted'], errors='ignore')

    # measure delays
    dfN['turnaround_time'] = dfN['date_submitted'].sub(dfN['date'], axis=0)
    dfN['turnaround_time'] = dfN['turnaround_time'].apply(lambda x: str(x).split()[0])  # reformat

    # fix lineages
    def fix_lineages(strain):
        new_lineage = dfN.loc[dfN['strain'] == strain, 'pango_lineage'].item()
        if strain in lineages:
            new_lineage = lineages[strain]
        return new_lineage

    # add tag of variant category
    def variant_category(lineage):
        var_category = 'Outras variantes'
        for name in variants.keys():
            if lineage == name:
                var_category = variants[lineage]
        return var_category


    def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

    def assign_consortia(name):
        consortium_name = 'Outras iniciativas'
        for lab in consortia.keys():
            if similar(name, lab) > 0.75:
                consortium_name = consortia[lab]
        return consortium_name

    # fix geography
    def fix_geography(column, location):
        geolocation = location
        if geolocation in [None, '']:
            pass
        for name in geography[column].keys():
            if location == name:
                geolocation = geography[column][location]
        return geolocation

    dfN['division'] = dfN['division'].apply(lambda x: fix_geography('division', x))
    dfN['location'] = dfN['location'].apply(lambda x: fix_geography('location', x))
    dfN['pango_lineage'] = dfN['strain'].apply(lambda x: fix_lineages(x))
    dfN['variant_lineage'] = dfN['pango_lineage'].apply(lambda x: variant_category(x))
    dfN['who_variant'] = dfN['variant_lineage'].apply(lambda x: x.split(' ')[0] if '(' in x else x)
    dfN['consortium'] = dfN['submitting_lab'].apply(lambda x: assign_consortia(x))

    list_columns = dfN.columns.values  # list of column in the original metadata file

    # Lab genomes metadata
    dfE = pd.read_excel(metadata2, index_col=None, header=0, sheet_name=0,
                        converters={'Sample-ID': str, 'Collection-date': str})
    dfE.fillna('', inplace=True)

    dfE = dfE.rename(
        columns={'Sample-ID': 'id', 'Collection-date': 'date', 'Country': 'country', 'Division (state)': 'division',
                 'Location (county)': 'location', 'Country of exposure': 'country_exposure',
                 'State of exposure': 'division_exposure', 'Lineage': 'pango_lineage', 'Source': 'originating_lab',
                 'Filter': 'filter'})
    dfE['epiweek'] = ''

    # exclude rows with no ID
    if 'id' in dfE.columns.to_list():
        dfE = dfE[~dfE['id'].isin([''])]

    lab_sequences = dfE['id'].tolist()
    # exclude unwanted lab metadata row
    if len(filterby) > 0:
        print('\nFiltering metadata by category: ' + ', '.join(filterby) + '\n')
    dfL = pd.DataFrame(columns=dfE.columns.to_list())
    for value in filterby:
        dfF = dfE[dfE['filter'].isin([value])]  # batch inclusion of specific rows
        dfL = pd.concat([dfL, dfF])  # add filtered rows to dataframe with lab metadata

    # list of relevant genomes sequenced
    keep_only = dfL['id'].tolist()
    excluded = [id for id in lab_sequences if id not in keep_only]

    # create a dict of existing sequences
    sequences = {}
    for fasta in SeqIO.parse(open(genomes), 'fasta'):  # as fasta:
        id, seq = fasta.description, fasta.seq
        if id not in sequences.keys() and id not in excluded:
            sequences[id] = str(seq)

    # add inexistent columns
    for col in list_columns:
        if col not in dfL.columns:
            dfL[col] = ''

    # output dataframe
    outputDF = pd.DataFrame(columns=list_columns)
    found = []
    lab_label = {}
    metadata_issues = {}
    # process metadata from excel sheet
    for idx, row in dfL.iterrows():
        id = dfL.loc[idx, 'id']
        if id in sequences:
            dict_row = {}
            for col in list_columns:
                dict_row[col] = ''
                if col in row:
                    dict_row[col] = dfL.loc[idx, col].strip()  # add values to dictionary

            # check for missing geodata
            geodata = ['country']  # column
            for level in geodata:
                if len(dict_row[level]) < 1:
                    if id not in metadata_issues:
                        metadata_issues[id] = [level]
                    else:
                        metadata_issues[id].append(level)

            if dict_row['location'] in ['', None]:
                dict_row['location'] = dfL.loc[idx, 'location']

            collection_date = ''
            if len(str(dict_row['date'])) > 1:
                collection_date = dict_row['date'].split(' ')[0].replace('.', '-').replace('/', '-')
                dict_row['date'] = collection_date
                # check is date is appropriate: not from the 'future', not older than 'min_date'
                if pd.to_datetime(today) < pd.to_datetime(collection_date) or pd.to_datetime(min_date) > pd.to_datetime(
                        collection_date):
                    if id not in metadata_issues:
                        metadata_issues[id] = ['date']
                    else:
                        metadata_issues[id].append('date')
            else:  # missing date
                if id not in metadata_issues:
                    metadata_issues[id] = ['date']
                else:
                    metadata_issues[id].append('date')

            # fix exposure
            columns_exposure = ['country_exposure', 'division_exposure']
            for level_exposure in columns_exposure:
                level = level_exposure.split('_')[0]
                dict_row[level_exposure] = dfL.loc[idx, level_exposure]
                if dict_row[level_exposure] in ['', None]:
                    if level_exposure == 'country_exposure':
                        dict_row[level_exposure] = dict_row[level]
                    else:
                        if dict_row['country_exposure'] != dfL.loc[idx, 'country']:
                            dict_row[level_exposure] = dict_row['country_exposure']
                        else:
                            dict_row[level_exposure] = dict_row[level]

            code = ''
            if dict_row['division'] in br_state_abbrev:
                code = br_state_abbrev[dict_row['division']] + '-'

            strain = dfL.loc[idx, 'country'].replace(' ', '') + '/' + code + dfL.loc[idx, 'id'] + '/' + \
                     collection_date.split('-')[0]

            # set the strain name
            dict_row['strain'] = strain
            dict_row['code'] = get_iso(dict_row['country'])
            dict_row['originating_lab'] = dfL.loc[idx, 'originating_lab']
            dict_row['submitting_lab'] = 'Lab name'
            dict_row['authors'] = 'Group members'

            # add lineage
            lineage = ''
            if strain in lineages:
                lineage = lineages[strain]
            else:
                if dfL.loc[idx, 'pango_lineage'] != '':
                    lineage = dfL.loc[idx, 'pango_lineage']
            dict_row['pango_lineage'] = lineage

            # variant classication (VOI, VOC, VHC)
            dict_row['category'] = variant_category(lineage)

            # assign epiweek
            if len(dict_row['date']) > 0:
                dict_row['epiweek'] = get_epiweeks(collection_date)
            else:
                dict_row['epiweek'] = ''

            # record sequence and metadata as found
            found.append(strain)
            if id not in metadata_issues.keys():
                lab_label[id] = strain
                outputDF = outputDF.append(dict_row, ignore_index=True)

    # process metadata from TSV
    dfN = dfN[dfN['strain'].isin(sequences.keys())]
    for idx, row in dfN.iterrows():
        strain = dfN.loc[idx, 'strain']
        if strain in sequences:
            if strain in outputDF['strain'].to_list():
                continue
            dict_row = {}
            date = ''
            for col in list_columns:
                if col == 'date':
                    date = dfN.loc[idx, col]
                dict_row[col] = ''
                if col in row:
                    dict_row[col] = dfN.loc[idx, col]

            # fix exposure
            columns_exposure = ['country_exposure', 'division_exposure']
            for level_exposure in columns_exposure:
                level = level_exposure.split('_')[0]
                if dict_row[level_exposure] in ['', None]:
                    dict_row[level_exposure] = dict_row[level]

            dict_row['code'] = get_iso(dict_row['country'])
            dict_row['epiweek'] = get_epiweeks(date)
            found.append(strain)

            outputDF = outputDF.append(dict_row, ignore_index=True)

    # write new metadata files
    root = [('Wuhan/Hu-1/2019', '2019-12-26'), ('Wuhan/WH01/2019', '2019-12-26')]
    for genome in root:
        strain, date = genome
        if strain not in outputDF['strain'].tolist():
            dict_row = {'strain': strain, 'date': date}
            outputDF = outputDF.append(dict_row, ignore_index=True)

    outputDF = outputDF.drop(columns=['region'])
    outputDF['date'] = pd.to_datetime(outputDF['date']).dt.floor('d')
#     outputDF['date'] = outputDF['date'].str.replace(' 00:00:00', '')
    outputDF.to_csv(output1, sep='\t', index=False)

    # write sequence file
    exported = []
    with open(output2, 'w') as outfile2:
        # export new metadata lines
        for id, sequence in sequences.items():
            if id in lab_label and id not in metadata_issues.keys():  # export lab generated sequences
                if lab_label[id] not in exported:
                    entry = '>' + lab_label[id] + '\n' + sequence + '\n'
                    outfile2.write(entry)
                    print('* Exporting newly sequenced genome and metadata for ' + id)
                    exported.append(lab_label[id])
            else:  # export publicly available sequences
                if id not in exported and id in outputDF['strain'].tolist():
                    entry = '>' + id + '\n' + sequence + '\n'
                    outfile2.write(entry)
                    exported.append(id)

    if len(metadata_issues) > 0:
        print('\n\n### WARNINGS!\n')
        print(
            '\nPlease check for metadata issues related to these samples and column (which will be otherwise ignored)\n')
        for id, columns in metadata_issues.items():
            print('\t- ' + id + ' (issues found at: ' + ', '.join(columns) + ')')

    print('\nMetadata file successfully reformatted and exported!\n')
