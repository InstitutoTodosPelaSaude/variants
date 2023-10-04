import pandas as pd
import numpy as np
import argparse

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Perform mathematical operations with data matrices",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input1", required=True, help="Main matrix, used as the numerator")
    parser.add_argument("--input2", required=False, type=str,  help="Secondary matrix, with values used as denominators")
    parser.add_argument("--index1", nargs="+", required=True, type=str, help="Columns with unique identifiers in the numerator file")
    parser.add_argument("--index2", nargs="+", required=False, type=str, help="Columns with unique identifiers in the denominator file, at least one match index1")
    parser.add_argument("--extra-columns", required=False, nargs='+', type=str, help="Extra columns to export from input1 to output, guided by data in index1")
    parser.add_argument("--rolling-average", required=False, type=int,  help="Window for rolling average conversion")
    parser.add_argument("--norm-var", required=False, type=str,  help="Single column to be used for normalization of all columns (e.g. population)")
    parser.add_argument("--rate", required=False, type=int,  help="Rate factor for normalization (e.g. 100000 habitants)")
    parser.add_argument("--min-denominator", required=False, type=int, default=0, help="Value X of rolling average window (mean at every X data points in time series)")
    parser.add_argument("--multiply", required=False, nargs=1, type=str, default='no', choices=['yes', 'no'], help="Multiply values, matrix 1 X matrix 2?")
    parser.add_argument("--filter1", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--filter2", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--sortby", required=False, nargs='+', type=str, help="Columns to be used to sort the output file")
    parser.add_argument("--output", required=True, help="TSV matrix with normalized values")
    args = parser.parse_args()

    input1 = args.input1
    input2 = args.input2
    unique_id1 = args.index1
    unique_id2 = args.index2
    rolling_avg = args.rolling_average
    norm_variable = args.norm_var
    rate_factor = args.rate
    min_denominator = args.min_denominator
    multiply = args.multiply[0]
    filter1 = args.filter1
    filter2 = args.filter2
    sortby = args.sortby
    output = args.output


    # path = "/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/ITpS/projetos_itps/vigilanciagenomica/analyses/relatorio11_20230216/"
    # input1 = path + 'matrix_total_lineages_country.tsv'
    # input2 = path + 'matrix_total_allgenomes_country.tsv'
    # unique_id1 = ['pango_lineage', 'code']
    # unique_id2 = ['code']
    # norm_variable = ''
    # rate_factor = ''
    # rolling_avg = ''
    # multiply = 'no'
    # min_denominator = 50
    # filter1 = "~country:Curacao, ~country:Sint Maarten, ~country:Guernsey, ~country:Jersey, ~country:Canary Islands, ~country:Crimea"
    # filter2 = "~country:Curacao, ~country:Sint Maarten, ~country:Guernsey, ~country:Jersey, ~country:Canary Islands, ~country:Crimea"
    # sortby = 'code'
    # output = path + 'matrix_total_freqlin_country.tsv'


    def load_table(file):
        df = ''
        if str(file).split('.')[-1] == 'tsv':
            # print(file)
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

    # open dataframe
    df = load_table(input1)
    df.fillna('', inplace=True)

    for idx in unique_id1:
        df = df[~df[idx].isin([''])]

    # filter rows
    def filter_df(df, criteria):
        print('\nFiltering rows...')
        # print(criteria)
        new_df = pd.DataFrame()
        include = {}
        for filter_value in criteria.split(','):
            filter_value = filter_value.strip()
            if not filter_value.startswith('~'):
                col, val = filter_value.split(':')[0], filter_value.split(':')[1]
                if val == '\'\'':
                    val = ''
                if col not in include:
                    include[col] = [val]
                else:
                    include[col].append(val)
        # print('Include:', include)
        for filter_col, filter_val in include.items():
            print('\t- Including only rows with \'' + filter_col + '\' = \'' + ', '.join(filter_val) + '\'')

            # print(new_df.size)
            if new_df.empty:
                df_filtered = df[df[filter_col].isin(filter_val)]
                new_df = new_df._append(df_filtered)
            else:
                new_df = new_df[new_df[filter_col].isin(filter_val)]
            # print(new_df)#.head())

        exclude = {}
        for filter_value in criteria.split(','):
            filter_value = filter_value.strip()
            if filter_value.startswith('~'):
                # print('\t- Excluding all rows with \'' + col + '\' = \'' + val + '\'')
                filter_value = filter_value[1:]
                col, val = filter_value.split(':')[0], filter_value.split(':')[1]
                if val == '\'\'':
                    val = ''
                if col not in exclude:
                    exclude[col] = [val]
                else:
                    exclude[col].append(val)
        # print('Exclude:', exclude)
        for filter_col, filter_val in exclude.items():
            print('\t- Excluding all rows with \'' + filter_col + '\' = \'' + ', '.join(filter_val) + '\'')
            if new_df.empty:
                df = df[~df[filter_col].isin(filter_val)]
                new_df = new_df._append(df)
            else:
                new_df = new_df[~new_df[filter_col].isin(filter_val)]
            # print(new_df)#.head())
        return new_df

    # load data
    if filter1 not in ['', None]:
        df = filter_df(df, filter1)

    if input2 not in ['', None]:
        df2 = load_table(input2)
        df2.fillna('', inplace=True)
        if filter2 not in ['', None]:
            df2 = filter_df(df2, filter2)
    else:
        df2 = df[unique_id1]
        norm_variable = 'norm_variable'
        unique_id2 = unique_id1
        df2[norm_variable] = 1

    # print(df2.head)
    # print(df2.columns.tolist())

    # get date columns
    date_columns = []
    for column in df.columns.to_list():
        if column[0].isdecimal():
            if norm_variable in ['', None]:
                if column in df2.columns.tolist():
                    date_columns.append(column)
            else:
                date_columns.append(column)

    if len(date_columns) < 1:
        date_columns.append('total')

    # set new indices
    # print(unique_id1)
    # print(df['macsaud_code'].tolist())
    for columns, dataframe in zip([unique_id1, unique_id2], [df, df2]):
        for col in columns:
            if str(dataframe[col].tolist()[0][0]).isdigit(): # if first character is digit
                if str(dataframe[col].tolist()[0][-1]).isdigit(): # if last character is digit
                    dataframe[col] = dataframe[col].astype(str)
                    # print(dataframe[col].tolist()[0])
                    # print(dataframe.head())
        # else:
        #     print(dataframe[column][0])
        #     print('not found')

    # print(df[unique_id1[0]].tolist())
    # print(df2[unique_id2[0]].tolist())


    df.insert(0, 'unique_id1', '')
    # df['unique_id1'] = df[unique_id1].astype(str).sum(axis=1)
    df['unique_id1'] = df[unique_id1].agg(''.join, axis=1)

    df.insert(1, 'unique_id2', '')
    # df['unique_id2'] = df[unique_id2].astype(str).sum(axis=1)
    df['unique_id2'] = df[unique_id2].agg(''.join, axis=1)

    df2.insert(0, 'unique_id2', '')
    # df2['unique_id2'] = df2[unique_id2].astype(str).sum(axis=1)
    df2['unique_id2'] = df2[unique_id2].agg(''.join, axis=1)


    # print(df['unique_id1'].tolist())
    # print(df2['unique_id2'].tolist())

    # create empty dataframes
    # nondate_columns = [column for column in df.columns.to_list() if not column[0].isdecimal()]
    nondate_columns = [column for column in df.columns.to_list() if column not in date_columns]

    # print(date_columns)
    # print(nondate_columns)

    df3 = df.filter(nondate_columns + date_columns, axis=1)
    # print(df3.columns.tolist())



    # set new index
    # df.set_index(unique_id1, inplace=True)
    df2.set_index('unique_id2', inplace=True)
    df3.set_index('unique_id1', inplace=True)

    # print(df)
    # print(df2)
    # print(df3)

    # perform division
    notfound = [] # denominators not found
    for idx, row in df.iterrows():
        if rolling_avg not in [None, '']:
            rolling_window_obj = row.rolling(int(rolling_avg))
            rolling_average = rolling_window_obj.mean()
            df.loc[idx] = rolling_average

        # print('\n' + str(idx))
        id1 = str(df.loc[idx, 'unique_id1'])
        id2 = str(df.loc[idx, 'unique_id2'])
        # print(id1, id2)
        # print(df[date_columns].loc[idx])

        if id2 in df2.index.tolist():
            for time_col in date_columns:
                numerator = df.loc[idx, time_col]
                if numerator not in ['', None]:
                    numerator = float(numerator)
                # print(time_col, numerator)

                if rate_factor in ['', None]:
                    rate_factor = 1
                else:
                    rate_factor = float(rate_factor)
                    # print('\nNo rate factor provided. Using "1" instead.')

                if norm_variable in ['', None]:
                    # print(id2)
                    # print(df2.loc[id2, time_col])
                    denominator = float(df2.loc[id2, time_col])
                else:
                    # print(id2 + "\'" + df2.loc[id2, norm_variable] + "\'")
                    denominator = float(df2.loc[id2, norm_variable])

                if denominator > min_denominator and numerator not in ['', None]: # prevent division by zero
                    if multiply == 'yes':
                        normalized = '%.10f' % (numerator * denominator)
                    else:
                        normalized = '%.10f' % ((numerator * rate_factor) / denominator)
                else:
                    normalized = np.nan
                df3.at[id1, time_col] = normalized
        else:
            notfound.append(id2)

    df3 = df3.reset_index()

    if len(notfound) > 1:
        df3 = df3[~df3['unique_id1'].isin(notfound)]

        print('\nA total of ' + str(len(notfound)) + ' variables used as denominators were not found, and corresponding rows were excluded from the output:')
        reported = []
        for entry in notfound:
            if entry not in reported:
                print('\t- ' + entry)
                reported.append(entry)


    # if extra_cols in [None, '', ['']]:
    #     extra_cols = []
    #
    # if extra_cols not in [None, '', ['']]:
    #     for column in extra_cols:
    #         if column in df.columns.to_list():
    #             df2.insert(0, column, '')
    #
    #             for idx, row in df2.iterrows():
    #                 unique_id2 = df2.loc[idx, 'unique_id2']
    #                 value = df.loc[df['unique_id2'] == unique_id2, column].iloc[0]
    #                 # print(idx, column, value)
    #                 df2.at[idx, column] = value


    df3 = df3.drop(columns=['unique_id1', 'unique_id2'])
    # df3 = df3[nondate_columns, date_columns]

    if sortby not in ['', None]:
        df3 = df3.sort_values(by=sortby)

    # output converted dataframes
    df3.to_csv(output, sep='\t', index=False)
    print('\nOperations successfully completed.\n')
