import plotly.express as px
import numpy as np
import pandas as pd
import os
import matplotlib
import matplotlib.pyplot as plt
import argparse

plt.rcParams['font.family'] = 'Arial'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate treemap",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", required=True, help="Configuration file in TSV format")
    args = parser.parse_args()

    config = args.config

    # path = '/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/yale/ncov/ncov_impacc/nextstrain/run12_20210829_last2/'
    # os.chdir(path)
    #
    # # input = path + 'lineages_sites_treemap_dups_counts.tsv'
    # # colours = path + 'colors.tsv'
    # # colourby = 'pango_lineage'
    # # count = 'count'
    # # levels = [px.Constant("Pango Lineages")] + ['country', 'enrollment_site', 'pango_lineage']
    # # colours = 'Greens'
    # config = "config_treemap_countries.tsv"


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

    # load parameters
    params = load_table(config)
    params.fillna('', inplace=True)
    params = params.set_index('param')
    # print(params)

    backend = params.loc['backend', 'value']
    matplotlib.use(backend)

    # Load data
    input_file = params.loc['input', 'value']
    df1 = load_table(input_file)

    # filter rows
    def filter_df(df, criteria):
        print('\nFiltering rows...')
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
                new_df = new_df.append(df_filtered)
            else:
                new_df = new_df[new_df[filter_col].isin(filter_val)]

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
                new_df = new_df.append(df)
            else:
                new_df = new_df[~new_df[filter_col].isin(filter_val)]
            # print(new_df)#.head())
        return new_df


    filters = params.loc['filter', 'value']
    dfD = pd.DataFrame()
    if filters not in ['', None]:
        dfD = filter_df(df1, filters)
    else:
        dfD = df1


    # column with counts to be displayed in treemap
    count = params.loc['count_col', 'value']
    dfD[count] = dfD[count].astype('float')

    # color scheme
    colours = params.loc['colour_scheme', 'value']
    if colours in ['', None]:
        colours = 'rainbow'
    colourby = params.loc['colour_by', 'value']


    top_levels = params.loc['top_levels', 'value']
    last_level = params.loc['last_level', 'value']
    levels = [i.strip() for i in top_levels.split(',')] + [last_level]
    # print(levels)
    # levels = [px.Constant("Pango Lineages")] + ['country', 'enrollment_site', 'pango_lineage']

    hover_data = params.loc['hover_data', 'value']
    if hover_data not in ['', None]:
        hover_data = [hover_data]
    else:
        hover_data = [last_level]

    title_text = params.loc['title', 'value']

    # plotting
    if '.' in colours:
        dict_colours = {'(?)':'lightgrey'}
        color_scheme = load_table(colours)
        color_scheme = color_scheme[color_scheme['field'].isin([colourby])]
        for idx, row in color_scheme.iterrows():
            id = color_scheme.loc[idx, 'value']
            hex = color_scheme.loc[idx, 'hex_color']
            dict_colours[id] = hex

        fig = px.treemap(dfD,
                         path=levels,
                         values=count,
                         color=colourby,
                         hover_data=hover_data,
                         color_discrete_map=dict_colours,
                         title=title_text)
    else:
        fig = px.treemap(dfD,
                         path=levels,
                         values=count,
                         color=count,
                         hover_data=hover_data,
                         color_continuous_scale=colours,
                         color_continuous_midpoint=np.average(dfD[count], weights=dfD[count]),
                         title=title_text)

    fig.update_layout(uniformtext=dict(minsize=14),
                      margin=dict(t=25, l=25, r=25, b=25)) # , mode='hide'
    fig.show()


