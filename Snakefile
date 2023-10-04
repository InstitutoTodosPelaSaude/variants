# Wildcards setting
LOCATIONS = ["region", "country", "division", "regiao"]
METRICS = ["cases", "deaths"]


rule arguments:
	params:
		metadata = "data/metadata.tsv",
		lineage_file = "config/cov-lineages.tsv",
		geoscheme = "config/geoscheme.tsv",
		colscheme = "config/name2hue.tsv",
		population = "config/UN_country_population_2020_iso.tsv",
		ibge = "config/tabela_municipio_macsaud_estado_regiao.tsv",
		correction_file = "config/fix_values.xlsx",
		filters = "config/filters.tsv",
		date_column = "date",
		start_date = "2023-07-23",
		end_date = "2023-09-30",
		unit = "week"

arguments = rules.arguments.params

rule all:
	shell:
		"""
		snakemake --cores all global_epidata
		snakemake --cores all brazil_epidata
		snakemake --cores all collapse
		snakemake --cores all reformat_gisaid
		snakemake --cores all variant_matrix
		snakemake --cores all lininc_global
		snakemake --cores all lininc_brazil
		snakemake --cores all colors
		snakemake --cores all copy_files
		"""


rule global_epidata:
	input:
		expand("results/epi_data/matrix_covid19_{met}_global.tsv", met=METRICS),

rule global_epidata_run:
	message:
		"""
		Download global epidemiological data ({wildcards.met}) from public repositories
		"""
	input:
		unpop = arguments.population,
	params:
		start_date = arguments.start_date,
		end_date = arguments.end_date,
		index = "code",
		targets = "population#3",
		action = "add",
		mode = "columns",
		format = "integer",
		time_unit = arguments.unit,
		week_format = "end",
	output:
		matrix = temp("results/epi_data/matrix_covid19_{met}_global_temp.tsv"),
		matrix2 = "results/epi_data/matrix_covid19_{met}_global.tsv",
		matrix3 = "results/epi_data/matrix_weeks_covid19_{met}_global.tsv",
		matrix4 = "results/epi_data/matrix_weeks_{met}100k_global.tsv",
		stacked = "results/epi_data/stacked_{met}100k_global.tsv",
	shell:
		"""
		python3 scripts/get_daily_matrix_global.py \
			--type {wildcards.met} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix}

		python3 scripts/reformat_dataframe.py \
			--input1 {output.matrix} \
			--input2 {input.unpop} \
			--index {params.index} \
			--action {params.action} \
			--mode {params.mode} \
			--targets {params.targets} \
			--output {output.matrix2}

		python3 scripts/aggregator.py \
			--input {output.matrix2} \
			--unit {arguments.unit} \
			--format {params.format} \
			--weekasdate {params.week_format} \
			--output {output.matrix3}

		python3 scripts/matrix_operations.py \
			--input1 {output.matrix3} \
			--input2 {output.matrix3} \
			--index1 {params.index} \
			--index2 {params.index} \
			--norm-var population \
			--rate 100000 \
			--output {output.matrix4}

		python3 scripts/stacked_matrices.py \
			--input1 {output.matrix4} \
			--index {params.index} \
			--xvar week_incid100k \
			--extra-columns country \
			--output {output.stacked}
		"""


rule covid_saude:
	message:
		"""
		Download epidemiological data from covid.saude.gov.br
		"""
	output:
		matrix_brazil = "data/matrix_full_brazil_epidata.tsv",

	shell:
		"""
		python3 scripts/get_brazil_epidata.py \
			--output {output.matrix_brazil}
		"""




METRICAS = ["casosNovos", "obitosNovos"]
rule brazil_epidata:
	input:
		expand("results/epi_data/matrix_covid19_{mtc}_brazil.tsv", mtc=METRICAS),

# def parameters1(metric):
# 	print(metric)
# 	return(metric)


rule brazil_epidata_run:
	message:
		"""
		Process epidemiological data ({wildcards.mtc}) from covid.saude.gov.br
		"""
	input:
		matrix = "data/matrix_full_brazil_epidata.tsv",
		ibge_table = arguments.ibge,
	params:
		xvar = "data",
		xtype = "time",
		yvar = "codmun",
		index = "codmun",
		extra_columns = "regiao estado coduf municipio populacaoTCU2019",
		filters = "~codRegiaoSaude:0",
		format = "integer",
		start_date = arguments.start_date,
		end_date = arguments.end_date,
		sortby = "codmun",
		# metric = lambda wildcards: parameters1(wildcards.mtc),
		targets = "DS_NOME#3, CO_MACSAUD#5, DS_NOMEPAD_macsaud#6",
		action = "add",
		mode = "columns",
	output:
		matrix = temp("results/epi_data/matrix_covid19_{mtc}_brazil_temp.tsv"),
		matrix2 = "results/epi_data/matrix_covid19_{mtc}_brazil.tsv",
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.matrix} \
			--xvar {params.xvar} \
			--target {wildcards.mtc} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--filters "{params.filters}" \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--sortby {params.sortby} \
			--output {output.matrix}

		python3 scripts/reformat_dataframe.py \
			--input1 {output.matrix} \
			--input2 {input.ibge_table} \
			--index {params.index} \
			--action {params.action} \
			--mode {params.mode} \
			--targets \"{params.targets}\" \
			--output {output.matrix2}

		sed -i 's/estado/uf_sigla/' {output.matrix2}
		sed -i 's/DS_NOMEPAD_macsaud/macsaud/' {output.matrix2}
		sed -i 's/CO_MACSAUD/macsaud_code/' {output.matrix2}
		sed -i 's/DS_NOME/estado/' {output.matrix2}
		"""



METRICAS2 = ["casosNovos", "obitosNovos"]
GEOLEVELS = ["regiao", "macsaud_code", "estado", "codmun"]
#GEOLEVELS = ["regiao", "estado"]

rule collapse:
	input:
		expand("results/epi_data/matrix_days_covid19_{mtc}_x_{geo}.tsv", mtc=METRICAS2, geo=GEOLEVELS)


dic_params2 = {
"regiao": ["\'\'", "uf_sigla coduf codmun estado macsaud_code macsaud municipio"],
"macsaud_code": ["macsaud regiao coduf estado uf_sigla", "codmun municipio"],
"estado": ["coduf uf_sigla regiao", "codmun municipio macsaud_code macsaud"],
"codmun": ["municipio uf_sigla regiao populacaoTCU2019", "macsaud_code macsaud estado coduf"],
}

def parameters2(loc):
	geocol = loc
	extracol = dic_params2[loc][0]
	ignore = dic_params2[loc][1]
	index = loc
	return([geocol, extracol, ignore, index])

rule collapse_run:
	message:
		"""
		Collapse city level data into a lower level of resolution ({wildcards.geo})
		"""
	input:
		matrix = "results/epi_data/matrix_covid19_{mtc}_brazil.tsv"
	params:
		index = lambda wildcards: parameters2(wildcards.geo)[0],
		extracol = lambda wildcards: parameters2(wildcards.geo)[1],
		ignore = lambda wildcards: parameters2(wildcards.geo)[2],
		index2 = lambda wildcards: parameters2(wildcards.geo)[3],
		format = "integer",
		time_unit = arguments.unit,
		week_format = "end",
	output:
		matrix_collapsed_days = "results/epi_data/matrix_days_covid19_{mtc}_x_{geo}.tsv",
		matrix_collapsed_weeks = "results/epi_data/matrix_weeks_covid19_{mtc}_{geo}.tsv",
		matrix_incidence_weeks = "results/epi_data/matrix_weeks_incidence_{mtc}_{geo}.tsv",
		stacked_incidence_weeks = "results/epi_data/stacked_weeks_incidence_{mtc}_{geo}.tsv",
	shell:
		"""
		python3 scripts/collapser.py \
			--input {input.matrix} \
			--index {params.index} \
			--unique-id {params.index} \
			--extra-columns {params.extracol} \
			--ignore {params.ignore} \
			--format {params.format} \
			--sortby {params.index} \
			--output {output.matrix_collapsed_days}
		
		python3 scripts/aggregator.py \
			--input {output.matrix_collapsed_days} \
			--unit {arguments.unit} \
			--format {params.format} \
			--weekasdate {params.week_format} \
			--output {output.matrix_collapsed_weeks}

		python3 scripts/matrix_operations.py \
			--input1 {output.matrix_collapsed_weeks} \
			--input2 {output.matrix_collapsed_weeks} \
			--index1 {params.index2} \
			--index2 {params.index2} \
			--norm-var populacaoTCU2019 \
			--rate 100000 \
			--output {output.matrix_incidence_weeks}

		python3 scripts/stacked_matrices.py \
			--input1 {output.matrix_incidence_weeks} \
			--index {params.index2} \
			--xvar rate_100k \
			--extra-columns {params.extracol} \
			--output {output.stacked_incidence_weeks}
		"""



rule get_lineages:
	message:
		"""
		Web scrap the latest SARS-CoV-2 lineages (VOCs) from cov-lineages.org
		"""
	input:
		covlineages = arguments.lineage_file,
	output:
		variants = "config/who_variants.tsv"
	shell:
		"""
		python3 scripts/get_covlineages.py \
			--input {input.covlineages} \
			--output {output.variants}
		"""


rule reformat_gisaid:
	message:
		"""
		Reformat GISAID metadata
		"""
	input:
		metadata = arguments.metadata,
		variants = rules.get_lineages.output.variants,
		covlineages = arguments.lineage_file,
		geoscheme = arguments.geoscheme,
		corr_file = arguments.correction_file,
		extracols = "results/epi_data/matrix_weeks_covid19_casosNovos_estado.tsv"
	params:
		datecol = arguments.date_column,
		weekasdate = "end",
		start_date = arguments.start_date,
		end_date = arguments.end_date,
		sortby = "country date",
	output:
		metadata_temp = temp("results/metadata_temp.tsv"),
		metadata = "results/metadata.tsv",
	shell:
		"""
		python3 scripts/reformat_gisaid.py \
			--metadata {input.metadata} \
			--covlineages {input.covlineages} \
			--variants {input.variants} \
			--geoscheme {input.geoscheme} \
			--correction {input.corr_file} \
			--date-column {params.datecol} \
			--weekasdate {params.weekasdate} \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--sortby {params.sortby} \
			--output {output.metadata_temp}

		sed -i 's/division/estado/' {output.metadata_temp}

		python3 scripts/reformat_dataframe.py \
			--input1 {output.metadata_temp} \
			--input2 {input.extracols} \
			--index estado \
			--action add \
			--mode columns \
			--targets "coduf#8, uf_sigla#9, regiao#10" \
			--output {output.metadata}

		sed -i 's/estado/division/' {output.metadata}
		"""



rule variant_matrix:
	input:
		expand("results/variant_data/matrix_variants_{geo}.tsv", geo=LOCATIONS),
7
dic_params = {
"region": [["\'\'"], [""]],
"country": [["code region"], [""]], # remove country:Brazil
"division": [["country code"], ["country:Brazil"]],
"regiao": [["code"], ["country:Brazil"]]
}

def parameters(loc):
	geocol = loc
	extracol = dic_params[loc][0][0]
	filters = dic_params[loc][1][0]
	return([geocol, extracol, filters])

rule variant_matrix_run:
	message:
		"""
		Generate matrix of variant counts per day, for each element in column="{wildcards.geo}"
		"""
	input:
		metadata = rules.reformat_gisaid.output.metadata,
	params:
		yvar = lambda wildcards: parameters(wildcards.geo)[0] + " who_variant",
		yvar2 = lambda wildcards: parameters(wildcards.geo)[0] + " pango_lineage",
		index = lambda wildcards: parameters(wildcards.geo)[0],
		xvar = arguments.date_column,
		xtype = "time",
		start_date = arguments.start_date,
		end_date = arguments.end_date,
		extra_columns = lambda wildcards: parameters(wildcards.geo)[1],
		filters = lambda wildcards: parameters(wildcards.geo)[2],
		format = "integer",
		time_unit = arguments.unit,
		week_format = "end",
	output:
		matrix_lin = "results/variant_data/matrix_lineages_{geo}.tsv",
		matrix_var = "results/variant_data/matrix_variants_{geo}.tsv",
		matrix_gen = "results/variant_data/matrix_allgenomes_{geo}.tsv",
		matrix_lin2 = "results/variant_data/matrix_weeks_lineages_{geo}.tsv",
		matrix_var2 = "results/variant_data/matrix_weeks_variants_{geo}.tsv",
		matrix_gen2 = "results/variant_data/matrix_weeks_allgenomes_{geo}.tsv",
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.metadata} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--filters "{params.filters}" \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_var}
			
		python3 scripts/rows2matrix.py \
			--input {input.metadata} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar2} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--filters "{params.filters}" \
			--start-date {params.start_date} \
			--end-date {params.end_date} \
			--output {output.matrix_lin}

		python3 scripts/collapser.py \
			--input {output.matrix_var} \
			--index {params.index} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--format {params.format} \
			--output {output.matrix_gen} \
		
		python3 scripts/aggregator.py \
			--input {output.matrix_var} \
			--unit {arguments.unit} \
			--format {params.format} \
			--weekasdate {params.week_format} \
			--output {output.matrix_var2}

		python3 scripts/aggregator.py \
			--input {output.matrix_lin} \
			--unit {arguments.unit} \
			--format {params.format} \
			--weekasdate {params.week_format} \
			--output {output.matrix_lin2}

		python3 scripts/aggregator.py \
			--input {output.matrix_gen} \
			--unit {arguments.unit} \
			--format {params.format} \
			--weekasdate {params.week_format} \
			--output {output.matrix_gen2}

		sed -i 's/division/estado/' {output.matrix_var2}
		sed -i 's/division/estado/' {output.matrix_lin2}
		sed -i 's/division/estado/' {output.matrix_gen2}
		"""




# rule nowcasting:
# 	message:
# 		"""
# 		Perform nowcasting of SARS-CoV-2 variants
# 		"""
# 	input:
# 		xxxxx = arguments.xxxxx,
# 		xxxxx = arguments.xxxxx,
# 	params:
# 		xxxxx = arguments.xxxxx,
# 		xxxxx = arguments.xxxxx,
# 		xxxxx = arguments.xxxxx,
# 	output:
# 		matrix = "xxxxx"
# 	shell:
# 		"""
# 		python3 scripts/xxxx.py \
# 			--metadata {input.xxxx} \
# 			--filters {input.xxxx} \
# 			--index-column {params.xxxx} \
# 			--extra-columns {params.xxxx} \
# 			--date-column {params.xxxx} \
# 			--output {output.xxxx}
# 		"""




rule lininc_global:
	message:
		"""
		Estimate the incidence of SARS-CoV-2 lineages circulating globally
		"""
	input:
		incidence = "results/epi_data/matrix_weeks_cases100k_global.tsv",
		lineages = "results/variant_data/matrix_weeks_lineages_country.tsv",
		genomes = "results/variant_data/matrix_weeks_allgenomes_country.tsv"
	params:
		index1 = "pango_lineage code",
		index2 = "code",
		unit = "total",
		format = "integer",
	output:
		gentotal = "results/variant_data/matrix_total_allgenomes_country.tsv",
		lintotal = "results/variant_data/matrix_total_lineages_country.tsv",
		linfreq = "results/variant_data/matrix_total_freqlin_country.tsv",
		inctotal = "results/epi_data/matrix_total_cases100k_global.tsv",
		lininc = "results/epi_data/matrix_total_100klin_global.tsv"
	shell:
		"""
		python3 scripts/aggregator.py \
			--input {input.lineages} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.lintotal}

		python3 scripts/aggregator.py \
			--input {input.genomes} \
			--unit {params.unit} \
			--output {output.gentotal}

		python3 scripts/aggregator.py \
			--input {input.incidence} \
			--unit {params.unit} \
			--output {output.inctotal}

		python3 scripts/matrix_operations.py \
			--input1 {output.lintotal} \
			--input2 {output.gentotal} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--filter1 "~country:Curacao, ~country:Sint Maarten, ~country:Guernsey, ~country:Jersey, ~country:Canary Islands, ~country:Crimea" \
			--filter2 "~country:Curacao, ~country:Sint Maarten, ~country:Guernsey, ~country:Jersey, ~country:Canary Islands, ~country:Crimea" \
			--output {output.linfreq}

		python3 scripts/matrix_operations.py \
			--input1 {output.linfreq} \
			--input2 {output.inctotal} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--filter1 "~country:Curacao, ~country:Sint Maarten, ~country:Guernsey, ~country:Jersey, ~country:Canary Islands, ~country:Crimea" \
			--filter2 "~country:Curacao, ~country:Sint Maarten, ~country:Guernsey, ~country:Jersey, ~country:Canary Islands, ~country:Crimea" \
			--multiply "yes" \
			--output {output.lininc}
		"""




rule lininc_brazil:
	message:
		"""
		Estimate the incidence of SARS-CoV-2 lineages circulating in regions of Brazil
		"""
	input:
		metadata = rules.reformat_gisaid.output.metadata,
		lineages = "results/variant_data/matrix_weeks_lineages_division.tsv",
		casedata = "results/epi_data/matrix_weeks_covid19_casosNovos_estado.tsv",
		incidence = "results/epi_data/matrix_weeks_incidence_casosNovos_regiao.tsv",
		who_var = "config/who_variants.tsv",
	params:
		index1 = "pango_lineage regiao",
		index2 = "regiao",
		unit = "total",
		format = "integer",
	output:
		lineages = temp("results/variant_data/matrix_weeks_lineages_division2.tsv"),
		linweek = "results/variant_data/matrix_weeks_lineages_regionbr.tsv",
		gentotal = "results/variant_data/matrix_total_allgenomes_regionbr.tsv",
		lintotal = "results/variant_data/matrix_total_lineages_regionbr.tsv",
		linfreq = "results/variant_data/matrix_total_freqlin_regionbr.tsv",
		inctotal = "results/epi_data/matrix_total_incidence_regiao.tsv",
		lininc = "results/epi_data/matrix_total_inclin_regiao.tsv"
	shell:
		"""
		python3 scripts/reformat_dataframe.py \
			--input1 {input.lineages} \
			--input2 {input.casedata} \
			--index "estado" \
			--action "add" \
			--mode "columns" \
			--targets "coduf#3, uf_sigla#4, regiao#5" \
			--sortby "coduf" \
			--output {output.lineages}

		python3 scripts/collapser.py \
			--input {output.lineages} \
			--index {params.index1} \
			--unique-id {params.index2} \
			--ignore estado coduf uf_sigla country code \
			--format {params.format} \
			--sortby {params.index2} \
			--output {output.linweek}

		python3 scripts/aggregator.py \
			--input {output.linweek} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.lintotal}

		python3 scripts/collapser.py \
			--input {output.lintotal} \
			--index {params.index2} \
			--unique-id {params.index2} \
			--ignore pango_lineage \
			--format {params.format} \
			--output {output.gentotal}

		python3 scripts/matrix_operations.py \
			--input1 {output.lintotal} \
			--input2 {output.gentotal} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--output {output.linfreq}

		python3 scripts/aggregator.py \
			--input {input.incidence} \
			--unit {params.unit} \
			--output {output.inctotal}

		python3 scripts/matrix_operations.py \
			--input1 {output.linfreq} \
			--input2 {output.inctotal} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--multiply yes \
			--output {output.lininc}
		"""



rule colors:
	message:
		"""
		Create colour scheme for variants and subvariantes
		"""
	input:
		matrix = rules.reformat_gisaid.output.metadata,
		colors = arguments.colscheme,
	params:
		levels = "variant_category who_variant pango_lineage",
	output:
		matrix = "config/colors.tsv",
	shell:
		"""
		python3 scripts/colour_maker.py \
			--input {input.matrix} \
			--colors {input.colors} \
			--levels {params.levels} \
			--output {output.matrix}
		"""



rule copy_files:
	message:
		"""
		Copy files for plotting
		"""
	shell:
		"""
		cp "./config/colors.tsv" ./figures
		cp "./results/epi_data/stacked_cases100k_global.tsv" ./figures/maps/global
		cp "./results/epi_data/stacked_weeks_incidence_casosNovos_macsaud_code.tsv" ./figures/maps/brazil
		cp "./results/epi_data/matrix_weeks_incidence_casosNovos_estado.tsv" ./figures/heatmap/brazil
		cp "./results/epi_data/matrix_weeks_cases100k_global.tsv" ./figures/heatmap/global
		cp "./results/epi_data/matrix_total_100klin_global.tsv" ./figures/treemap/global
		cp "./results/epi_data/matrix_total_inclin_regiao.tsv" ./figures/treemap/brazil
		cp "./results/variant_data/matrix_weeks_variants_regiao.tsv" ./figures/barplot/brazil/raw_data
		"""


rule dataviz:
	message:
		"""
		Generate plots
		"""
	shell:
		"""
		cd ./figures/barplot/brazil/raw_data
		python3 pandas_multibar.py --config config_variants_weeks.tsv

		cd ../../../../figures/heatmap/brazil
		python3 pandas_heatmap.py --config config_incidence_states.tsv

		cd ../../../figures/treemap/brazil
		python pandas_treemap.py --config config_lineages_regionbr.tsv
		"""



rule remove_figs:
	message: "Removing figures"
	shell:
		"""
		rm ./figures/colors.tsv
		rm -r ./figures/relatorio*
		rm figures/*/*/*.pdf
		rm figures/*/*/matrix*
		rm figures/*/*/stacked*
		rm figures/*/*/*/matrix*
		rm figures/*/*/*/nowcasting*
		"""


rule clean:
	message: "Removing directories: {params}"
	shell:
		"""
		rm -rfv results
		rm -rfv temp
		rm data/matrix_full_brazil_epidata.tsv
		rm data/metadata.tsv
		rm config/who_variants.tsv
		rm config/colors.tsv
		"""




# rule xxxxx:
# 	message:
# 		"""
# 		xxx
# 		"""
# 	input:
# 		xxxxx = arguments.xxxxx,
# 		xxxxx = arguments.xxxxx,
# 	params:
# 		xxxxx = arguments.xxxxx,
# 		xxxxx = arguments.xxxxx,
# 		xxxxx = arguments.xxxxx,
# 	output:
# 		matrix = "xxxxx"
# 	shell:
# 		"""
# 		python3 scripts/xxxx.py \
# 			--metadata {input.xxxx} \
# 			--filters {input.xxxx} \
# 			--index-column {params.xxxx} \
# 			--extra-columns {params.xxxx} \
# 			--date-column {params.xxxx} \
# 			--output {output.xxxx}
# 		"""



# rule variant_frequency:
# 	input:
# 		expand("results/variant_data/matrix_variants_{geo}.tsv", geo=LOCATIONS),

# dic_params = {
# "country": [["code region"], [""]], # remove country:Brazil
# "estado": [["pango_lineage estado"], ["estado"]]
# }

# def parameters3(loc):
# 	geocol = loc
# 	index1 = dic_params[loc][0][0]
# 	index2 = dic_params[loc][1][0]
# 	return([geocol, index1, index2])


# rule variant_frequency_run:
# 	message:
# 		"""
# 		Generate matrix of lineage frequency and case proportions per state
# 		"""
# 	input:
# 		matrix_var = "results/variant_data/matrix_weeks_lineages_division.tsv",
# 		matrix_gen = "results/variant_data/matrix_weeks_allgenomes_country.tsv",
# 		matrix_cases = "results/epi_data/matrix_weeks_covid19_casosNovos_estado.tsv"
# 	params:
# 		index1 = "pango_lineage estado",
# 		index2 = "estado",
# 	output:
# 		matrix_freq = "outputs/results/variant_data/matrix_weeks_linfreq_estado.tsv",
# 		matrix_prop = "outputs/results/variant_data/matrix_weeks_caseprop_estado.tsv",
# 	shell:
# 		"""
# 		python3 scripts/matrix_operations.py \
# 			--input1 {input.matrix_var} \
# 			--input2 {input.matrix_gen} \
# 			--index1 {params.index1} \
# 			--index2 {params.index2} \
# 			--output {output.matrix_freq}

# 		python3 scripts/matrix_operations.py \
# 			--input1 {output.matrix_freq} \
# 			--input2 {input.matrix_cases} \
# 			--index1 {params.index1} \
# 			--index2 {params.index2} \
# 			--multiply "yes" \
# 			--output {output.matrix_prop}
# 		"""