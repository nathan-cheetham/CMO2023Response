# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 11:30:37 2023

@author: k2143494

Analysis of Census 2021 and population projection data for response to CMO annual report 2023
"""

#%% Load packages
import pandas as pd

#%% Define functions
# -----------------------------------------------------------------------------
def sum_cols(data, col_list, new_col):
    """Function to sum together values in list of columns"""
    data[new_col] = 0
    for col in col_list:        
        data[new_col] += data[col]
    return data

#%% Load data
# -----------------------------------------------------------------------------
# Census 2021, Lower Super Output Area population by age, 5-year bands. https://www.nomisweb.co.uk/datasets/c2021ts007a
census_ageband = pd.read_csv(r"census2021-ts007a-lsoa.csv")

# -----------------------------------------------------------------------------
# Census 2021, General health by age, sex and deprivation. Local authority table, Table 6 from: https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthandwellbeing/datasets/generalhealthbyagesexanddeprivationenglandandwales
data_healthbyage = pd.read_csv(r"HealthByAgeSexDeprivation_Census20212011_LocalAuthority.csv")
data_healthbyage_cols = data_healthbyage.columns.to_list()

# -----------------------------------------------------------------------------
# Subnational population projections. "Population projections for local authorities: Table 2" https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationprojections/datasets/localauthoritiesinenglandtable2
population_projections = pd.read_csv(r"subnationalprojections_mid2018_LAD19version.csv")

# -----------------------------------------------------------------------------
# Area-level Deprivation lower super output area (LSOA) level data
# England 2019 https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019
deprivation_LSOA_lookup_england = pd.read_csv(r"File_2_-_IoD2019_Domains_of_Deprivation.csv")
deprivation_LSOA_lookup_england_cols = deprivation_LSOA_lookup_england.columns.to_list()
# Wales 2019 https://statswales.gov.wales/Catalogue/Community-Safety-and-Social-Inclusion/Welsh-Index-of-Multiple-Deprivation
deprivation_LSOA_lookup_wales = pd.read_csv(r"welsh-index-multiple-deprivation-2019-index-and-domain-ranks-by-small-area.csv")
deprivation_LSOA_lookup_wales_cols = deprivation_LSOA_lookup_wales.columns.to_list()

# -----------------------------------------------------------------------------
# LSOA Rural-Urban Classification, 2011 England and Wales. https://www.gov.uk/government/collections/rural-urban-classification
RUC_LSOA_lookup_englandwales = pd.read_csv(r"Rural_Urban_Classification_2011_lookup_tables_for_small_area_geographies_EnglandWales.csv")
RUC_LSOA_lookup_englandwales_cols = RUC_LSOA_lookup_englandwales.columns.to_list()

# -----------------------------------------------------------------------------
# Local authority level Rural-Urban classification 2011, England only. https://www.gov.uk/government/collections/rural-urban-classification
# Mapped to Local Authority 2021 boundaries
RUC_LAD21 = pd.read_csv(r"RUC11_LAD21CD_level.csv")
RUC_LAD21_cols = RUC_LAD21.columns.to_list()
# Mapped to Local Authority 2019 boundaries
RUC_LAD19 = pd.read_csv(r"RUC11_LAD19CD_level.csv")
RUC_LAD19_cols = RUC_LAD19.columns.to_list()

# -----------------------------------------------------------------------------
# Geodata lookup files
# Mapping from output area (OA) based on 2021 OA codes, OA to LSOA, MSOA, LA. https://geoportal.statistics.gov.uk/datasets/ons::output-area-to-lower-layer-super-output-area-to-middle-layer-super-output-area-to-local-authority-district-december-2021-lookup-in-england-and-wales-v2-1/about
geocode_mapping_2021 = pd.read_csv(r"OA21_LSOA21_MSOA21_LAD22_EW_LU.csv")
geocode_mapping_2021_cols = geocode_mapping_2021.columns.to_list()
# Mapping from OA to Region based on 2021 OA codes, OA to Region. https://geoportal.statistics.gov.uk/datasets/efda0d0e14da4badbd8bdf8ae31d2f00/about
geocode_mapping_2021_OAtoRegion = pd.read_csv(r"OA21_RGN22_LU.csv")


#%% PROCESSING: Geodata
# merge together on OA, so region is added
geocode_mapping_2021 = pd.merge(geocode_mapping_2021, geocode_mapping_2021_OAtoRegion, how = 'left', on = 'oa21cd')
# Group by LSOA, take first row
geocode_mapping_2021_LSOA = geocode_mapping_2021.groupby('lsoa21cd').nth(0).reset_index()
geocode_mapping_2021_LSOA_cols = geocode_mapping_2021_LSOA.columns.to_list()
# Group by LAD, take first row
geocode_mapping_2021_LAD = geocode_mapping_2021.groupby('lad22cd').nth(0).reset_index()
geocode_mapping_2021_LAD_cols = geocode_mapping_2021_LAD.columns.to_list()

# # Mapping from OA based on 2011 OA codes - IF NEEDED
# # geocode_mapping_2011 = pd.read_csv(r"~\Geodata\Area lookups\Output_Area_to_LSOA_to_MSOA_to_Local_Authority_District_(December_2017)_Lookup_with_Area_Classifications_in_Great_Britain.csv")
# # geocode_mapping_2011_cols = geocode_mapping_2011.columns.to_list()

# Mapping from LSOA 2011 to 2021
geocode_mapping_2011to2021_LSOA = pd.read_csv(r"LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Lookup_for_England_and_Wales_(Version_2).csv")
# Group by LSOA 2011 to get one row by 2011 code. Where LSOA 2011 matches to multiple 2021 codes, take first LSOA 2021 code. ALTERNATIVE - Aggregate based on random sample, so that where LSOA 2011 matches to multiple 2021 codes, a 2021 code is chosen at random
geocode_mapping_2011to2021_LSOA_group2011 = geocode_mapping_2011to2021_LSOA.groupby('LSOA11CD')['LSOA21CD'].nth(0).reset_index()

# Group by LSOA 2021 to get one row by 2021 code. Where LSOA 2021 matches to multiple 2011 codes, take first LSOA 2011 code. ALTERNATIVE - Aggregate based on random sample, so that where LSOA 2021 matches to multiple 2011 codes, a 2011 code is chosen at random
geocode_mapping_2011to2021_LSOA_group2021 = geocode_mapping_2011to2021_LSOA.groupby('LSOA21CD')['LSOA11CD'].nth(0).reset_index()

# Lat and Long positions of LSOAs, 2021 boundaries
LSOA2021_positions = pd.read_csv(r"Lower_layer_Super_Output_Areas_2021_EW_BSC_v2_7982568775378104300.csv")

# LSOA 2011 and LAD 2022 to 2022 NHS ICBs and sub-ICB locations
geocode_mapping_LSOA2011toNHS = pd.read_csv(r"LSOA11_LOC22_ICB22_LAD22_EN_LU.csv")
geocode_mapping_LSOA2011toNHS_cols = geocode_mapping_LSOA2011toNHS.columns.to_list()


#%% PROCESSING: Census 2021, Lower Super Output Area population by age, 5-year bands dataset
# -----------------------------------------------------------------------------
# Rename columns 
dictionary = {}
dictionary['var_rename'] = {#
                # Age band
                'Age: Total':'Age_Total_N',
                'Age: Aged 4 years and under':'Age_Under5',
                'Age: Aged 5 to 9 years':'Age_5to9', 
                'Age: Aged 10 to 14 years':'Age_10to14',
                'Age: Aged 15 to 19 years':'Age_15to19',
                'Age: Aged 20 to 24 years':'Age_20to24',
                'Age: Aged 25 to 29 years':'Age_25to29',
                'Age: Aged 30 to 34 years':'Age_30to34', 
                'Age: Aged 35 to 39 years':'Age_35to39',
                'Age: Aged 40 to 44 years':'Age_40to44',
                'Age: Aged 45 to 49 years':'Age_45to49',
                'Age: Aged 50 to 54 years':'Age_50to54',
                'Age: Aged 55 to 59 years':'Age_55to59',
                'Age: Aged 60 to 64 years':'Age_60to64', 
                'Age: Aged 65 to 69 years':'Age_65to69', 
                'Age: Aged 70 to 74 years':'Age_70to74',
                'Age: Aged 75 to 79 years':'Age_75to79',
                'Age: Aged 80 to 84 years':'Age_80to84', 
                'Age: Aged 85 years and over':'Age_85plus',
                }

census_ageband = census_ageband.rename(columns = dictionary['var_rename'])
census_ageband = census_ageband.rename(columns = {'geography code':'lsoa21cd'})
census_ageband_cols = census_ageband.columns.to_list()

# -----------------------------------------------------------------------------
# Sum columns where relevant
census_ageband = sum_cols(data = census_ageband, col_list = ['Age_65to69','Age_70to74','Age_75to79','Age_80to84','Age_85plus'], new_col = 'Age_65plus')
census_ageband = sum_cols(data = census_ageband, col_list = ['Age_75to79','Age_80to84','Age_85plus'], new_col = 'Age_75plus')


#%% PROCESSING: Index of Multiple deprivation
# -----------------------------------------------------------------------------
# England 2019 - https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019
deprivation_LSOA_lookup_england = deprivation_LSOA_lookup_england.rename(columns = {'LSOA code (2011)': 'LSOA_England',
        'Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)': 'IMD_Decile_England2019',
        'Index of Multiple Deprivation (IMD) Rank (where 1 is most deprived)':'IMD_Rank_England2019',
        'Income Decile (where 1 is most deprived 10% of LSOAs)': 'IncomeDeprivation_Decile_England2019',
        'Health Deprivation and Disability Decile (where 1 is most deprived 10% of LSOAs)': 'HealthDeprivation_Decile_England2019'})

# Convert rank column from string to numeric
deprivation_LSOA_lookup_england['IMD_Rank_England2019'] = deprivation_LSOA_lookup_england['IMD_Rank_England2019'].str.replace(',', '').astype(float)

# Bin area deprivation rank into percentiles
deprivation_LSOA_lookup_england['IMD_Percentile_England2019'] = pd.qcut(deprivation_LSOA_lookup_england['IMD_Rank_England2019'], 100, labels = False)
deprivation_LSOA_lookup_england['IMD_Percentile_England2019'] = deprivation_LSOA_lookup_england['IMD_Percentile_England2019'] + 1 # Add 1 to start at 1

# -----------------------------------------------------------------------------
# Wales 2019 - downloaded from https://gov.wales/welsh-index-multiple-deprivation-full-index-update-ranks-2019
# LSOA to IMD
deprivation_LSOA_lookup_wales = deprivation_LSOA_lookup_wales.rename(columns = {'LSOA Code':'LSOA_Wales',
                                                                                'WIMD 2019 Overall Decile':'IMD_Decile_Wales2019'})

# Bin area deprivation rank into percentiles
deprivation_LSOA_lookup_wales['IMD_Percentile_Wales2019'] = pd.qcut(deprivation_LSOA_lookup_wales['WIMD 2019 Overall Rank '], 100, labels = False)
deprivation_LSOA_lookup_wales['IMD_Percentile_Wales2019'] = deprivation_LSOA_lookup_wales['IMD_Percentile_Wales2019'] + 1 # Add 1 to start at 1

# -----------------------------------------------------------------------------
# Join IMD to LSOA 2011 to 2021 mapping
# England
deprivation_LSOA21CD = pd.merge(geocode_mapping_2011to2021_LSOA_group2021,deprivation_LSOA_lookup_england[['LSOA_England','IMD_Decile_England2019','IMD_Percentile_England2019']], how = 'left', left_on = 'LSOA11CD', right_on = 'LSOA_England')
test1 = deprivation_LSOA21CD[['LSOA21CD','LSOA11CD','LSOA_England','IMD_Decile_England2019']] 

# Wales
deprivation_LSOA21CD = pd.merge(deprivation_LSOA21CD,deprivation_LSOA_lookup_wales[['LSOA_Wales','IMD_Decile_Wales2019','IMD_Percentile_Wales2019']], how = 'left', left_on = 'LSOA11CD', right_on = 'LSOA_Wales')
test1 = deprivation_LSOA21CD[['LSOA21CD','LSOA11CD','LSOA_Wales','IMD_Decile_Wales2019']] 

# -----------------------------------------------------------------------------
# Combine IMD deciles from different nations into single column
deprivation_LSOA21CD['IMD_Percentile_2019'] = deprivation_LSOA21CD['IMD_Percentile_England2019'].fillna(deprivation_LSOA21CD['IMD_Percentile_Wales2019'])
test_all = deprivation_LSOA21CD[['LSOA21CD','LSOA11CD','IMD_Percentile_2019']] 
deprivation_LSOA21CD['IMD_Decile_2019'] = deprivation_LSOA21CD['IMD_Decile_England2019'].fillna(deprivation_LSOA21CD['IMD_Decile_Wales2019'])
test_all = deprivation_LSOA21CD[['LSOA21CD','LSOA11CD','IMD_Decile_2019']] 

# Add quintile
deprivation_LSOA21CD.loc[deprivation_LSOA21CD['IMD_Decile_2019'].isin([1,2]), 'IMD_Quintile_2019'] = 1
deprivation_LSOA21CD.loc[deprivation_LSOA21CD['IMD_Decile_2019'].isin([3,4]), 'IMD_Quintile_2019'] = 2
deprivation_LSOA21CD.loc[deprivation_LSOA21CD['IMD_Decile_2019'].isin([5,6]), 'IMD_Quintile_2019'] = 3
deprivation_LSOA21CD.loc[deprivation_LSOA21CD['IMD_Decile_2019'].isin([7,8]), 'IMD_Quintile_2019'] = 4
deprivation_LSOA21CD.loc[deprivation_LSOA21CD['IMD_Decile_2019'].isin([9,10]), 'IMD_Quintile_2019'] = 5


#%% PROCESSING: Rural-Urban Classification 
# -----------------------------------------------------------------------------
# LSOA to Rural-Urban Classification 
RUC_LSOA_lookup_englandwales = RUC_LSOA_lookup_englandwales.rename(columns = {'Lower Super Output Area 2011 Code':'LSOA11CD',
                                                                              'Rural Urban Classification 2011 code':'RUC2011_code',
                                                                              'Rural Urban Classification 2011 (10 fold)':'RUC2011_cat10',
                                                                              'Rural Urban Classification 2011 (2 fold)':'RUC2011_cat2'})

# -----------------------------------------------------------------------------
# Join IMD to LSOA 2011 to 2021 mapping
RUC_LSOA_englandwales = pd.merge(geocode_mapping_2011to2021_LSOA_group2021,RUC_LSOA_lookup_englandwales[['LSOA11CD','RUC2011_cat10','RUC2011_cat2']], how = 'left', left_on = 'LSOA11CD', right_on = 'LSOA11CD')


#%% Merge LSOA level datasets, 1 row per LSOA
# -----------------------------------------------------------------------------
# Start with Census LSOA level age band data
data_combined_LSOA = census_ageband.copy()

# -----------------------------------------------------------------------------
# Add 2019 IMD (mapped to 2021 LSOA code)
col_list = ['LSOA21CD', 'IMD_Percentile_2019', 'IMD_Quintile_2019']
data_combined_LSOA = pd.merge(data_combined_LSOA, deprivation_LSOA21CD, how = 'left', left_on = 'lsoa21cd', right_on = 'LSOA21CD')

# -----------------------------------------------------------------------------
# Add geocode lookups for mapping
col_list = ['lsoa21cd', 'lsoa21nm', 'msoa21cd', 'msoa21nm', 'lad22cd', 'lad22nm', 'lad22nmw', 'rgn22cd', 'rgn22nm', 'rgn22nmw']
data_combined_LSOA = pd.merge(data_combined_LSOA, geocode_mapping_2021_LSOA[col_list], how = 'left', left_on = 'lsoa21cd', right_on = 'lsoa21cd')

# -----------------------------------------------------------------------------
# Add LSOA 2021 geo data - latitude and longitude for mapping
data_combined_LSOA = pd.merge(data_combined_LSOA, LSOA2021_positions, how = 'left', left_on = 'lsoa21cd', right_on = 'LSOA21CD')

# -----------------------------------------------------------------------------
# Add mapping to NHS regions
data_combined_LSOA = pd.merge(data_combined_LSOA, geocode_mapping_LSOA2011toNHS[['LSOA11CD', 'LOC22CD', 'LOC22CDH', 'LOC22NM', 'ICB22CD', 'ICB22CDH', 'ICB22NM']], how = 'left', left_on = 'LSOA11CD', right_on = 'LSOA11CD')

# -----------------------------------------------------------------------------
# Add 2011 LSOA level RUC (mapped to 2021 LSOA code)
col_list = ['LSOA21CD', 'RUC2011_cat10', 'RUC2011_cat2']
data_combined_LSOA = pd.merge(data_combined_LSOA, RUC_LSOA_englandwales[col_list], how = 'left', left_on = 'lsoa21cd', right_on = 'LSOA21CD')

# -----------------------------------------------------------------------------
# Add 2011 Local authority level RUC (mapped to 2021 Local authority code)
data_combined_LSOA = pd.merge(data_combined_LSOA, RUC_LAD21, how = 'left', left_on = 'lad22cd', right_on = 'Local Authority District Area 2021 Code')
data_combined_LSOA = data_combined_LSOA.drop(columns = ['Local Authority District Area 2021 Code','Local Authority District Area 2021 Name'])



#%% FIGURE 1 - LSOA level Census 2021 Population aged 65+ by rural-urban classification of LSOA
# -----------------------------------------------------------------------------
# FIGURE 1 (a) TABLE - 65+ population by LSOA level rural-urban
figure1a_65plus_byRUC_Overall = data_combined_LSOA.groupby('RUC2011_cat2').agg({'Age_65plus':'sum'}).reset_index()

# -----------------------------------------------------------------------------
# FIGURE 1 (b) TABLE - 65+ population by LSOA level rural-urban classification
figure1b_65plus_byRUCandRegion = data_combined_LSOA.groupby(['rgn22cd','RUC2011_cat2']).agg({'rgn22nm':'first',
                                                                                       'Age_65plus':'sum'}).reset_index()

# -----------------------------------------------------------------------------
# FIGURE 1 (c) - Map of - read field 'Age_65plus' as-is into Power BI software for mapping with mapbox visual, circle map option


#%% FIGURE 2 - Estimated (2018) and projected (2043) population aged 65+ by rural urban classification of local authority
# -----------------------------------------------------------------------------
# Filter for aged 65+ and Local authority regions 
age_group_list = ['65-69','70-74','75-79','80-84','85-89','90+']
population_projections_filter = population_projections[(population_projections['CODE'].str.contains('E06|E07|E08|E09'))
                                                       & (population_projections['AGE GROUP'].isin(age_group_list))]

# -----------------------------------------------------------------------------
# Group by local authority regions to get totals across ages
population_projections_filter_grouped = population_projections_filter.groupby('CODE').agg({'AREA':'first',
                                                                                            '2018':'sum',
                                                                                            '2043':'sum',
                                                                                            }).reset_index()

# -----------------------------------------------------------------------------
# Join 2011 local authority level rural-urban classification to Local authority level population estimates and projections
population_projections_filter_RUCmerge = pd.merge(population_projections_filter_grouped, RUC_LAD19, how = 'left', left_on = 'CODE', right_on = 'Local Authority District Area 2019 Code')

# -----------------------------------------------------------------------------
# Group by local authority to get totals for visualisation
# FIGURE 2 TABLE
figure2_populationprojections_byRUC = population_projections_filter_RUCmerge.groupby('Rural Urban Classification 2011 (3 fold)').agg({'2018':'sum',
             '2043':'sum',})


#%% FIGURE 3 - Population aged 65+ by rural-urban classification and index of multiple deprivation quintile of LSOA
figure3_65plus_byRUCandIMD = data_combined_LSOA.groupby(['RUC2011_cat2','IMD_Quintile_2019']).agg({'Age_65plus':'sum'}).reset_index()


#%% FIGURE 4 - Proportion of population aged 65+ with very bad and bad health by local authority area
# -----------------------------------------------------------------------------
# Filter general health data for over 65s
data_healthbyage_filter = data_healthbyage[(data_healthbyage['Sex'] == 'Persons')
                                           & (data_healthbyage['Year'] == 2021)
                                           & (data_healthbyage['Age'].isin(['65 to 69','70 to 74','75 to 79','80 to 84','85 to 89', '90+']))]

# Group by area and health status
data_healthbyage_filter_grouped = data_healthbyage_filter.groupby(['Area Code', 'Health Status']).agg({'Local Authority':'first',
                                                                                    'Count':'sum',
                                                                                    'Population':'sum',
                                                                                    }).reset_index()

# Calculate proportions for areas
data_healthbyage_filter_grouped['proportion'] = data_healthbyage_filter_grouped['Count'] / data_healthbyage_filter_grouped['Population']

# Rename to be alphabetical for mapping
data_healthbyage_filter_grouped['Health Status'] = data_healthbyage_filter_grouped['Health Status'].replace({'Very good':'5. Very good',
                                                                                                             'Good':'4. Good',
                                                                                                             'Fair':'3. Fair',
                                                                                                             'Bad':'2. Bad',
                                                                                                             'Very bad':'1. Very bad',
                                                                                                             })


# -----------------------------------------------------------------------------
# Edit area code of City of London and Westminster to match Westminster, and Cornwall and Isles of Scilly to match Cornwall for mapping
# Westminster E09000033, Cornwall E06000052
data_healthbyage_filter_grouped.loc[(data_healthbyage_filter_grouped['Local Authority'] == 'Cornwall and Isles of Scilly'),'Area Code'] = 'E06000052'
data_healthbyage_filter_grouped.loc[(data_healthbyage_filter_grouped['Local Authority'] == 'City of London and Westminster'),'Area Code'] = 'E09000033' 


# -----------------------------------------------------------------------------
# Join 2011 local authority level rural-urban classification to Local authority level general health population size and proportions
data_healthbyage_RUCmerge = pd.merge(data_healthbyage_filter_grouped, RUC_LAD21, how = 'left', left_on = 'Area Code', right_on = 'Local Authority District Area 2021 Code')

# Fill gaps where groupings don't match due to aggregations
data_healthbyage_RUCmerge.loc[(data_healthbyage_RUCmerge['Local Authority'] == 'Cornwall and Isles of Scilly'),'Rural Urban Classification 2011 (3 fold)'] = 'Predominantly Rural'
data_healthbyage_RUCmerge.loc[(data_healthbyage_RUCmerge['Local Authority'] == 'City of London and Westminster'),'Rural Urban Classification 2011 (3 fold)'] = 'Predominantly Urban'

# Export for mapping
# data_healthbyage_RUCmerge.to_csv('Census2021GeneralHealthbyLAD21CDandRUC2011.csv')

# -----------------------------------------------------------------------------
# Group by Local authority and combine Very bad and Bad
figure4_badhealth_bylocalauthority = data_healthbyage_RUCmerge[data_healthbyage_RUCmerge['Health Status'].isin(['1. Very bad','2. Bad'])].groupby('Area Code').agg({'Count':'sum',
            'Population':'first',})
figure4_badhealth_bylocalauthority['proportion'] = figure4_badhealth_bylocalauthority['Count'] / figure4_badhealth_bylocalauthority['Population']

# Group into ~ equal percentage bins
figure4_badhealth_bylocalauthority.loc[(figure4_badhealth_bylocalauthority['proportion'] < 0.09),'proportion_bin'] = '1: 7 to 9 %'
figure4_badhealth_bylocalauthority.loc[(figure4_badhealth_bylocalauthority['proportion'] >= 0.09) 
                       & (figure4_badhealth_bylocalauthority['proportion'] < 0.11),'proportion_bin'] = '2: 9 to 11 %'
figure4_badhealth_bylocalauthority.loc[(figure4_badhealth_bylocalauthority['proportion'] >= 0.11) 
                       & (figure4_badhealth_bylocalauthority['proportion'] < 0.13),'proportion_bin'] = '3: 11 to 13 %'
figure4_badhealth_bylocalauthority.loc[(figure4_badhealth_bylocalauthority['proportion'] >= 0.13) 
                       & (figure4_badhealth_bylocalauthority['proportion'] < 0.15),'proportion_bin'] = '4: 13 to 15 %'
figure4_badhealth_bylocalauthority.loc[(figure4_badhealth_bylocalauthority['proportion'] >= 0.15) 
                       & (figure4_badhealth_bylocalauthority['proportion'] < 0.17),'proportion_bin'] = '5: 15 to 17 %'
figure4_badhealth_bylocalauthority.loc[(figure4_badhealth_bylocalauthority['proportion'] >= 0.17),'proportion_bin'] = '6: 17 to 24 %'

# -----------------------------------------------------------------------------
# Merge in geodata area lookups so can map local authority to region
figure4_badhealth_bylocalauthority = figure4_badhealth_bylocalauthority.reset_index()
figure4_badhealth_bylocalauthority = pd.merge(figure4_badhealth_bylocalauthority, geocode_mapping_2021_LAD, how = 'left', left_on = 'Area Code', right_on = 'lad22cd')

# Export for mapping visualisation
# figure4_badhealth_bylocalauthority.to_csv('Census2021GeneralHealthbyLAD21CDandRUC2011_combinedbad.csv')


