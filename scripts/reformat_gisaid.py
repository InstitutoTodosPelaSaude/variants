# -*- coding: utf-8 -*-

# Created by: Anderson Brito
# Email: andersonfbrito@gmail.com
# Release date: 2022-08-12
# Last update: 2022-09-23

import pycountry_convert as pyCountry
import pycountry
import pandas as pd
from epiweeks import Week
import time
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Reformat GISAID metadata files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--metadata", required=True, help="Metadata file from GISAID")
    parser.add_argument("--covlineages", required=True, help="CoV lineages TSV file")
    parser.add_argument("--variants", required=True, help="Variant-lineage TSV file")
    parser.add_argument("--geoscheme", required=True, help="TSV file with geographic classifications")
    parser.add_argument("--correction", required=False, help="TSV, CSV, or excel file containing new standards for column values")
    parser.add_argument("--date-column", required=True, type=str, help="Metadata column containing the collection dates")
    parser.add_argument("--weekasdate",required=False, nargs=1, type=str, default='no',
                        choices=['start', 'end', 'no'], help="If representing weeks as date, which day of the week should be used?")
    parser.add_argument("--start-date", required=False, type=str,  help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=False, type=str,  help="End date in YYYY-MM-DD format")
    parser.add_argument("--sortby", required=False, nargs='+', type=str, help="Columns to be used to sort the output file")
    parser.add_argument("--output", required=True, help="Reformatted metadata file")
    args = parser.parse_args()

    metadata = args.metadata
    covlineages = args.covlineages
    variants_list = args.variants
    geoscheme = args.geoscheme
    fix_file = args.correction
    date_col = args.date_column
    weekasdate = args.weekasdate[0]
    start_date = args.start_date
    end_date = args.end_date
    sortby = args.sortby
    output = args.output

    # # path = os.path.abspath(os.getcwd())
    # path = '/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/ITpS/projetos_itps/vigilanciagenomica/analyses/test/'
    # metadata = path + 'data/metadata_br.tsv'
    # covlineages = path + 'config/cov-lineages.tsv'
    # variants_list = path + 'config/who_variants_json.tsv'
    # geoscheme = path + 'config/geoscheme.tsv'
    # fix_file = path + 'config/fix_values.xlsx'
    # date_col = 'date'
    # weekasdate = 'end'
    # start_date = '2022-06-01' # start date above this limit
    # end_date = '2022-06-30' # end date below this limit
    # sortby = ['country', 'date']
    # output = path + 'data/metadata_modified.tsv'


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


    # nextstrain metadata
    df = load_table(metadata)

    print('\t- Keeping only sequences longer than 21,000 bp')
    df = df.rename(columns={'Sequence length': 'length'})
    df['length'] = df['length'].astype(int)
    df = df.loc[df['length'] >= 21000]

    if 'Last vaccinated' in df.columns.tolist():
        print('\t- Renaming columns')
        junk = ['Passage details/history', 'Type', 'Additional location information',
                'Sequence length', 'Host', 'Patient age', 'Gender', 'Clade', 'Pangolin version', 'Variant',
                'AA Substitutions', 'Is reference?', 'Is complete?', 'Is high coverage?', 'Is low coverage?',
                'N-Content', 'GC-Content']
        for col in junk:
            if col in df.columns.tolist():
                df = df.drop(columns=[col])

        df = df.rename(columns={'Virus name': 'strain', 'Accession ID': 'gisaid_epi_isl', 'Collection date': 'date',
                                  'Pango lineage': 'pango_lineage', 'Pangolin version': 'pango_version',
                                  'Submission date': 'date_submitted'})
    else:
        df = df.rename(columns={'pangolin_lineage': 'pango_lineage'})

    # filter by date
    if date_col not in ['', None]:
        print('\t- Removing rows with incomplete dates')
        # remove genomes with incomplete dates
        df = df[df[date_col].apply(lambda x: len(x.split('-')) == 3)]  # accept only full dates
        df = df[df[date_col].apply(lambda x: 'X' not in x)]  # exclude -XX-XX missing dates

        today = time.strftime('%Y-%m-%d', time.gmtime())
        df[date_col] = pd.to_datetime(df[date_col])  # converting to datetime format
        if start_date in ['', None]:
            start_date = df[date_col].min()
        if end_date in ['', None]:
            end_date = today

        print('\t- Adding rows with %s from %s to %s' % (date_col, start_date, end_date))

        # converting back to string
        df[date_col] = df[date_col].apply(lambda x: x.strftime('%Y-%m-%d'))

        # filter genomes based on sampling date
        def filter_bydate(df, date):
            df[date] = pd.to_datetime(df[date])  # converting to datetime format
            mask = (df[date] >= start_date) & (df[date] <= end_date)  # mask any lines with dates outside the start/end dates
            df = df.loc[mask]  # apply mask
            return df

        df = filter_bydate(df, date_col)
    else:
        # filter entries based on date completeness
        df = df[df['date'].apply(lambda x: len(x.split('-')) == 3)]  # accept only full dates
        df = df[df['date'].apply(lambda x: 'X' not in x)]  # exclude -XX-XX missing dates


    new_columns = ['code', 'who_variant', 'variant_lineage']
    for col in new_columns:
        if col in df.columns.tolist():
            df = df.drop(columns=[col])
    df.insert(1, 'code', '')
    df.insert(2, 'who_variant', '')
    # df.insert(3, 'variant_lineage', '')
    df.fillna('', inplace=True)


    # create epiweek column
    def get_epiweeks(date):
        try:
            date = pd.to_datetime(date)
            epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
            year, week = epiweek[:4], epiweek[-2:]

            if weekasdate in ['start', 'end']:
                if weekasdate == 'start':
                    epiweek = str(Week(int(year), int(week)).startdate())
                else:
                    epiweek = str(Week(int(year), int(week)).enddate())
            else:
                epiweek = year + '_' + 'EW' + week
        except:
            epiweek = ''
        return epiweek
    df['epiweek'] = df['date'].apply(lambda x: get_epiweeks(x))


    variants = {}
    for line in open(variants_list, "r").readlines()[1:]:
        variant, lineage = line.strip().split('\t')
        if '*' in variant:
            variants[lineage] = variant
            # print(variant, lineage)
        # elif variant == 'Recombinante':
        #     variants[lineage] = variant
        else:
            pass
    # print(variants)


    categories = {}
    for line in open(covlineages, "r").readlines()[1:]:
        variant, lineages = line.strip().split('\t')
        if '*' not in variant:
            # print(variant, lineages)
            categories[lineages] = variant
    # print(categories)


    # add tag of variant category
    print('\t- Adding WHO variant information')
    def variant_name(lineage):
        var_name = 'Outras'
        for name in variants.keys():
            if lineage == name:
                var_name = variants[lineage]
        return var_name

    def variant_category(variant):
        var_category = 'Outras'
        # if 'Recombinante' in variant:
        #     var_category = 'Recombinante'
        # else:
        # print(categories)
        for var, cat in categories.items():
            if variant == var:
                # var_category = categories[variant]
                var_category = cat
        return var_category


    # df['variant_lineage'] = df['pango_lineage'].apply(lambda x: variant_name(x))
    df['who_variant'] = df['pango_lineage'].apply(lambda x: variant_name(x))
    # df['who_variant'] = df['variant_lineage'].apply(lambda x: x.split(' ')[0] if '(' in x else x)
    df['variant_category'] = df['who_variant'].apply(lambda x: variant_category(x))

    # print(df.columns.tolist())

    # break location information into columns
    if 'Last vaccinated' in df.columns.tolist():
        print('\t- Splitting geographic data column in region, country, division and location')
        # df[['region', 'country', 'division', 'location']] = df['Location'].str.split(',', expand=True)
        df['Location'] = df['Location'].str.replace(' / ', '/')
        df['region'] = df['Location'].str.split('/', expand=True)[0]
        df['country'] = df['Location'].str.split('/', expand=True)[1]
        try:
            df['division'] = df['Location'].str.split('/', expand=True)[2]
        except:
            df['division'] = ''
        try:
            df['location'] = df['Location'].str.split('/', expand=True)[3]
        except:
            df['location'] = ''

        df.fillna('', inplace=True)

    # print(df.columns.tolist())


    # fix values
    fix_dict = {}
    if fix_file not in ['', None]:
        dfG = load_table(fix_file)
        for idx, row in dfG.iterrows():
            column_name = dfG.loc[idx, 'column']
            old_name = dfG.loc[idx, 'old']
            new_name = dfG.loc[idx, 'new']
            if column_name not in fix_dict:
                fix_dict[column_name] = {}
            fix_dict[column_name][old_name] = new_name


    def fix_values(column, value):
        if value in [None, '']:
            pass
        for name in fix_dict[column].keys():
            if value == name:
                value = fix_dict[column][value]
        return value

    for col_name in fix_dict.keys():
        df[col_name] = df[col_name].apply(lambda x: fix_values(col_name, x))


    # fix exposure
    if 'country_exposure' in df.columns.tolist():
        df.insert(1, 'location_exposure', '')
        geolevels = ['region', 'country', 'division', 'location']
        for level in geolevels:
            exposure_column = level + '_exposure'
            for idx, row in df.iterrows():
                if df.loc[idx, exposure_column].lower() in ['', 'unknown']:
                    df.loc[idx, exposure_column] = df.loc[idx, level]


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

    # print(df.columns.tolist())

    # add alpha code
    if 'Last vaccinated' in df.columns.tolist():
        df['code'] = df['country'].apply(lambda x: get_iso(x))
        # print('here')
    else:
        df['code'] = df['country'].apply(lambda x: get_iso(x))


    # parse subcontinental regions in geoscheme
    print('\t- Adding subcontinent data')
    scheme_list = open(geoscheme, "r").readlines()[1:]
    geoLevels = {}
    for line in scheme_list:
        if not line.startswith('\n'):
            id = line.split('\t')[2]
            type = line.split('\t')[0]
            members = line.split('\t')[5].split(',')  # elements inside the subarea

            if type in ['region_exposure', 'region']:
                for country in members:
                    iso = get_iso(country.strip())
                    if iso != '':
                        geoLevels[iso] = id

            for elem in members:
                if elem.strip() not in geoLevels.keys():
                    geoLevels[elem.strip()] = id

    if 'Last vaccinated' in df.columns.tolist():
        df['region'] = df['code'].map(geoLevels)
    else:
        df['region'] = df['code'].map(geoLevels)

    # print(df[['region', 'code', 'country', 'division', 'location']])

    # keep only key columns
    if 'Last vaccinated' in df.columns.tolist():
        keycols = ['gisaid_epi_isl', 'date', 'epiweek', 'region', 'code', 'country', 'division', 'location', 'pango_lineage', 'variant_category', 'who_variant', 'date_submitted']
    else:
        keycols = ['gisaid_epi_isl', 'date', 'epiweek', 'region', 'code', 'country', 'division', 'location', 'pango_lineage', 'variant_category', 'who_variant', 'date_submitted']

    for c in keycols:
        if c not in df.columns.tolist():
            df[c] = ''

    print('\t- Preparing output file')
    df = df[keycols]

    if sortby not in ['', None]:
        df = df.sort_values(by=sortby)

    df.to_csv(output, sep='\t', index=False)
    print('\nMetadata file successfully reformatted and exported!\n')
