## Cleaning the ambient
rm(list = ls())
gc()

## Loading Libraries
packs = c("nnet", "foreign", "tidyverse", "vroom", "ggforce", "effects", "geofacet")
lapply(packs,require, character.only = TRUE)

## Reading the database
## Microdata whole world
variants_microdata<-vroom("Data/metadata/metadata_2023_01_09.tsv.zip")

## Filtering for Brazil
variants_microdata_br<-variants_microdata |> 
  filter(country == "Brazil")

## Saving the microdata for Brazil
vroom_write(x = variants_microdata_br, 
            file = "Data/metadata/metadata_2023-01-09_br.tsv")

## Reading microdata variants for Brazil
variants_microdata_br<-vroom(file = paste0("Data/metadata/metadata_2023-01-09_br.tsv"))

## Loading functions
source("Scripts/functions.R")

## Cleaning the dataset and creating columns to the Multinomial Regression
variants_multinomial<-variants_microdata_br |>  
  rename(date_collect = date) |> 
  mutate(name_state = case_when(division == 'Porto Alegre' ~ 'Rio Grande do Sul',
                                division == 'Federal District' ~ 'Distrito Federal',
                                TRUE ~ division),
         is_omicron = if_else(grepl(x = who_variant, pattern = "^Omicron"), "Omicron", "Non-Omicron")) |>
  mutate(week_collect = end.of.epiweek(date_collect),
         week_submitted = end.of.epiweek(date_submitted)) |>
  dplyr::select(date_collect, week_collect, 
                who_variant, country, name_state) |> 
  filter(!is.na(name_state),
         grepl(pattern = 'Omicron|Recombinante|Outras',
               x = who_variant))

# This change the results over regions, 
# as only we use data from this year the estimates consider new variants rising more
# rapidly

variants_multinomial<-regiao(variants_multinomial)

## Frequency table
# ## WHO Variant
variants_freq<-table(variants_multinomial$who_variant,
                     variants_multinomial$week_collect) |>
  as.data.frame() |>
  rename(who_variant = Var1, week_collect = Var2) |>
  mutate(week_collect = as.Date(week_collect)) |>
  filter(week_collect == max(week_collect)) |>
  arrange(desc(Freq))

## Regions
regions_freq<-table(variants_multinomial$regiao, 
                    variants_multinomial$week_collect) |> 
  as.data.frame() |> 
  rename(regiao = Var1, week_collect = Var2) |> 
  mutate(week_collect = as.Date(week_collect)) |> 
  filter(week_collect == max(week_collect)) |>
  arrange(desc(Freq))

# ## Setting the reference to 'Sudeste' region
variants_multinomial<-variants_multinomial %>%
  mutate(regiao = relevel(factor(regiao),
                          ref = "Sudeste"),
         # name_state = relevel(factor(name_state),
         #                      ref = as.character(states_freq$name_state[1])),
         who_variant = relevel(factor(who_variant),
                               ref = "Omicron-BA.5*"))

## Multinomial Logistic Regression models

## Regiões
# Logistic regression model for the growth rate of variants, 
# Y ~ week_collect + region, where Y is the probability of dominance of variant,
# or the frequency of variant over the totality of variants sequenced, 
# predicted by week of submission of the genome and per region of Brazil
# Regions is a factor, with reference on Southeast region, region with greater amount of sequences
# 10 weeks*5 regions = 50 predictors

# y<-variants_multinomial$who_variant
# X<-variants_multinomial[,c("week_collect", "regiao")]
nrows<-NROW(variants_multinomial)

multinomial_regions<-nnet::multinom(data = variants_multinomial, 
                                    formula = who_variant ~ week_collect + regiao, 
                                    weights = DirichletReg::rdirichlet(n = nrows,
                                                                       alpha = c(1)),
                                    Hess = TRUE,
                                    maxit = 250)

## Vectors of unique classes of each predictor, to construct predicts
regioes<-unique(variants_multinomial$regiao)
# states<-unique(variants_multinomial$name_state)
week_collect<-unique(variants_multinomial$week_collect)
## Push the weeks forward 4 weeks = 4*7 days = 28 days
# weeks_extended<-week_collect+28

## Nowcasting per Region
newdata_regions<-data.frame(regiao = rep(regioes, length(week_collect)), 
                            week_collect = rep(week_collect, length(regioes)))

pp.regions<-cbind(newdata_regions, predict(multinomial_regions, 
                                           newdata = newdata_regions, 
                                           type = "probs", 
                                           se = TRUE))

## Function to pviot into longer format, easier to plot
pivot_longer_fun<-function(x, week_col, aggreg_col = NULL){
  x<-x |> 
    pivot_longer(cols = c(-{{week_col}}, -{{aggreg_col}}), 
                 names_to = "who_variants", 
                 values_to = "probability")
}

### Regions long
pp.regions_long<-pp.regions |>
  pivot_longer_fun(week_col = week_collect, aggreg_col = regiao)

## Brasil estimate from the regional estimates
brasil<-pp.regions_long |> 
  group_by(week_collect, who_variants) |> 
  summarise(probability = sum(probability)/5) |> 
  mutate(regiao = "Brasil")

## Altogether
pp.regions_long<-rbind(pp.regions_long, brasil) 

### Regions transpose
pp.regions_transpose<-pp.regions_long |> 
  pivot_wider(names_from = week_collect, values_from = probability)

## Saving the predicts
## Nowcasting per Region long
vroom_write(x = pp.regions_long, 
            file = "Outputs/Tables/nowcasting_regions_variants_nnet_multinomial.tsv")

## Nowcasting per Region transpose
vroom_write(x = pp.regions_transpose, 
            file = "Outputs/Tables/nowcasting_regions_transpose_multinomial.tsv")

#