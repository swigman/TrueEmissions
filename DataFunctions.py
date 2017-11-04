#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 11:55:03 2017

@author: stefanwigman
"""

import pandas as pd
import wbdata
import datetime


# WORLD BANK DATA
# In this section, the functions that are used to retrieve and format the 
# World Bank data are defined. 

def GetRegionIncomeDataWB(file = 'Regions.xlsx', sheetname=0, header=0, 
                          skiprows=None, skip_footer=0, index_col='country',
                          names=['Country Data', 'Region', 'IncomeGroup']):
    """
    This function reads the regional data from the specified file, and returns 
    a Pandas Dataframe containing this data. 
    
    ------
    Inputs
    ------
    
    file:   The file that contains the region and income data for the countries
            (default: 'Regions.xlsx').
            
    names:  The column headers 
            (default: ['Country Data', 'Region', 'IncomeGroup']).
    
    """
    countries=pd.read_excel(file, sheetname=sheetname, header=header, 
                          skiprows=skiprows, skip_footer=skip_footer, 
                          index_col=index_col, names=names)
    return countries



def GetIndicatorsWB(file="Selected_Indicators.xlsx", sheet="Blad1" ):
    """
    This function imports the selected indicators, and their corresponding
    tabnames from the Excel-file Selected_Indicators.xlsx. 
    
    ------
    Inputs
    ------
    file:   the Excel-file that contains the indicators and their tabnames
    sheet:  the sheet that contains the indicators and their tabnames
    
    -------
    Outputs
    -------
    indicator_dataframe:    the dataframe that contains the indicators and 
                            their tabnames
    indicators:             a dictionary with the indicator code and names
    tabnames:               a dictionary that returns the tabnames of 
                            the indicators
    """
    
    indicator_dataframe = pd.ExcelFile(file).parse(sheet)
    indicators=indicator_dataframe.set_index('Indicator').to_dict()['Description']
    tabnames=indicator_dataframe.set_index('Description').to_dict()['Tabname']
    
    return indicator_dataframe, indicators, tabnames
    


def GetDataWB(indicators, year1=2000, year2=2016):
    """
    This function first retrieves World Bank data from the latest year, and then
    fills any missing data in the dataframe with data from previous years (in the specified range).
    
    ------
    Inputs
    ------
    indicators:     The indicator dataframe that was constructed with the 
                    function GetIndicatorsWB()
    year1:          The lower bound for time-period (default=2000)
    year2:          The upper bound for the time-period (default=2016)
    
    -------
    Outputs
    -------
    dataframe:      The resulting dataframe
    """
    
    data_date=(datetime.datetime(year2,1,1), datetime.datetime(year2,1,1))
    
    df_filled = wbdata.get_dataframe(indicators, data_date=data_date)
    
    for column in df_filled:
        column_source= column + ' source'
        df_filled[column_source] = None
        df_filled[column_source][df_filled[column].notnull()] = 'WB data ' + str(year2)                 
                         
    year2range=year2-1
                         
    for year in range(year2range, year1, -1):
        data_date = (datetime.datetime(year,1,1), datetime.datetime(year,1,1))
        df_year = wbdata.get_dataframe(indicators, data_date=data_date)
        
        for column in df_year:
            column_source = column + ' source'
            df_year[column_source] = None
            df_year[column_source][df_year[column].notnull()] = 'WB data ' + str(year)
        df_filled = df_filled.combine_first(df_year)
            
    return df_filled


def FillByRegionAndIncomeWB(dataframe):
    """
    This function fills missing values in the dataframe by taking the mean of 
    countries in the same region with the same income level. 
    
    ------
    Inputs
    ------
    dataframe:      a dataframe with the countries as index and a region and
                    income column. 
                    
    -------
    Outputs
    -------
    df_complete:    the resulting dataframe
    """
    
    source_cols=[col for col in dataframe.columns if 'source' in col]
    data=dataframe.drop(source_cols, axis=1)
    
    filled_data=pd.DataFrame()
    
    # actions on data frame
    for region in set(data['Region']):
        for incomelevel in set(data['IncomeGroup']):
            regional_data=data.loc[(data["Region"]==region) & (data["IncomeGroup"]==incomelevel)]
            regional_data=regional_data.fillna(regional_data.mean())
            filled_data=filled_data.append(regional_data)
            
    dont_include = ['Country Data', 'Region', 'IncomeGroup']
            
    for column in filled_data.columns[~filled_data.columns.isin(dont_include)]:
            column_source = column + ' source'
            filled_data[column_source] = None
            filled_data[column_source][filled_data[column].notnull()] = 'Estimation based on region and income' 
        
        
    df_complete = dataframe.combine_first(filled_data)
            
    return df_complete


def FillByIncomeWB(dataframe):
    """
    This function fills missing values in the dataframe by taking the mean of 
    countries with the same income level. 
    
    ------
    Inputs
    ------
    dataframe:      a dataframe with the countries as index and an
                    income column. 
                    
    -------
    Outputs
    -------
    df_complete:    the resulting dataframe
    """
    
    source_cols=[col for col in dataframe.columns if 'source' in col]
    
    data=dataframe.drop(source_cols, axis=1)
    
    filled_data=pd.DataFrame()
    
    for incomelevel in set(data['IncomeGroup']):
        income_data=data.loc[(data["IncomeGroup"]==incomelevel)]
        income_data=income_data.fillna(income_data.mean())
        filled_data=filled_data.append(income_data)
    
    dont_include = ['Country Data', 'Region', 'IncomeGroup']
            
    for column in filled_data.columns[~filled_data.columns.isin(dont_include)]:
            column_source = column + ' source'
            filled_data[column_source] = None
            filled_data[column_source][filled_data[column].notnull()] = 'Estimation based on income' 
    
    df_complete = dataframe.combine_first(filled_data)
        
    return df_complete

def FillByRegionWB(dataframe):
    """
    This function fills missing values in the dataframe by taking the mean of 
    countries in the same region. 
    
    ------
    Inputs
    ------
    dataframe:      a dataframe with the countries as index and a region and
                    income column. 
                    
    -------
    Outputs
    -------
    df_complete:    the resulting dataframe
    """
    
    source_cols=[col for col in dataframe.columns if 'source' in col]
    data=dataframe.drop(source_cols, axis=1)
    
    filled_data=pd.DataFrame()
    
    for region in set(data['Region']):
        regional_data=data.loc[(data["Region"]==region)]
        regional_data=regional_data.fillna(regional_data.mean())
        filled_data=filled_data.append(regional_data)
    
    dont_include = ['Country Data', 'Region', 'IncomeGroup']
    
    for column in filled_data.columns[~filled_data.columns.isin(dont_include)]:
            column_source = column + ' source'
            filled_data[column_source] = None
            filled_data[column_source][filled_data[column].notnull()] = 'Estimation based on region'
    
    df_complete = dataframe.combine_first(filled_data)
    
    return df_complete

def FillWithMeanWB(dataframe):
    """
    
    This function fills any missing data with the mean of all the other countries. 
    ------
    Inputs
    ------
    dataframe: The dataframe with missing data. 
    
    -------
    Outputs
    -------
    df_complete:    The resulting dataframe
    
    """

    
    source_cols=[col for col in dataframe.columns if 'source' in col]
    
    data=dataframe.drop(source_cols, axis=1)
    
    filled_data=pd.DataFrame()
    
    filled_data=data.fillna(data.mean())
    
    dont_include = ['Country Data', 'Region', 'IncomeGroup']
    
    for column in filled_data.columns[~filled_data.columns.isin(dont_include)]:
            column_source = column + ' source'
            filled_data[column_source] = None
            filled_data[column_source][filled_data[column].notnull()] = 'Estimation based on region'
    
    
    df_complete=dataframe.combine_first(filled_data)
    
    return df_complete

def PopulationRangesWB(dataframe, tabnames):
    """
    This function translates the WB population data to the correct ranges needed
    for the model. 
    
    ------
    Inputs
    ------
    dataframe:  The dataframe that contains the population data from WB
    tabnames:   The dictionary that contains the tabnames for each indicator
    
    -------
    Outputs
    -------
    new_df:     The dataframe with the correct population ranges
    tabnames:   The updated dictionary with the tabnames for each indicator
    """
    

    
    new_df=dataframe.copy()
    new_df["Population0to14"]=dataframe["Population, ages 0-14, total"]
    male15to34columns=["Population ages 15-19, male (% of male population)",
                       "Population ages 20-24, male (% of male population)",
                       "Population ages 25-29, male (% of male population)",
                       "Population ages 30-34, male (% of male population)"]
    
    female15to34columns=["Population ages 15-19, female (% of female population)",
                         "Population ages 20-24, female (% of female population)",
                         "Population ages 25-29, female (% of female population)",
                         "Population ages 30-34, female (% of female population)"]
    
    male35to64columns=["Population ages 35-39, male (% of male population)",
                      "Population ages 40-44, male (% of male population)",
                      "Population ages 45-49, male (% of male population)",
                      "Population ages 50-54, male (% of male population)",
                      "Population ages 55-59, male (% of male population)",
                      "Population ages 50-64, male (% of male population)"] # Note: the indicatorcode says 60-64
    
    female35to64columns=["Population ages 35-39, female (% of female population)",
                      "Population ages 40-44, female (% of female population)",
                      "Population ages 45-49, female (% of female population)",
                      "Population ages 50-54, female (% of female population)",
                      "Population ages 55-59, female (% of female population)",
                      "Population ages 50-64, female (% of female population)"] # Note: the indicatorcode says 60-64

    malepop15to34percentage=dataframe[male15to34columns].sum(axis=1)/100
    femalepop15to34percentage=dataframe[female15to34columns].sum(axis=1)/100
    malepop35to64percentage=dataframe[male35to64columns].sum(axis=1)/100
    femalepop35to64percentage=dataframe[female35to64columns].sum(axis=1)/100

    for country in new_df.index:
        if (dataframe["Population, male"].loc[country]+dataframe["Population, female"].loc[country])-dataframe["Population, total"].loc[country]>10000:
            print("Rescaling male and female populations for ",country)
            new_df["Population, male"].loc[country]=dataframe["Population, male"].loc[country]*dataframe["Population, total"].loc[country]/(dataframe["Population, male"].loc[country]+dataframe["Population, female"].loc[country])
            new_df["Population, female"].loc[country]=dataframe["Population, female"].loc[country]*dataframe["Population, total"].loc[country]/(dataframe["Population, male"].loc[country]+dataframe["Population, female"].loc[country])
            
    new_df["Population0to14"]=new_df["Population, ages 0-14, total"]
    new_df["Population15to34"]=malepop15to34percentage*new_df["Population, male"]+femalepop15to34percentage*new_df["Population, female"]
    new_df["Population35to64"]=malepop35to64percentage*new_df["Population, male"]+femalepop35to64percentage*new_df["Population, female"]
    new_df["PopulationOver65"]=(dataframe["Population ages 65 and above (% of total)"]/100)*dataframe["Population, total"]

    columns_to_drop=male15to34columns+female15to34columns+male35to64columns+female35to64columns+["Population, ages 0-14, total"]+["Population ages 65 and above (% of total)"]
    source_list=[]
    
    for item in columns_to_drop:
        source_list.append(item+" source")
    columns_to_drop=columns_to_drop+source_list
    
    new_df.drop(columns_to_drop, axis=1, inplace=True)

    totalpop=new_df[["Population0to14","Population15to34","Population35to64","PopulationOver65"]].sum(axis=1)
    
    for country in new_df.index:
        if totalpop[country]-dataframe["Population, total"].loc[country]>10000:
            print("Rescaling population ranges for ", country)
            new_df["Population0to14"].loc[country]=new_df["Population0to14"].loc[country]/(totalpop[country]/new_df["Population, total"].loc[country])
           # new_df["Population15to34"].loc[country]=new_df["Population15to34"].loc[country]/(totalpop[country]/new_df["Population, total"].loc[country])
           # new_df["Population35to64"].loc[country]=new_df["Population35to64"].loc[country]/(totalpop[country]/new_df["Population, total"].loc[country])
            new_df["PopulationOver65"].loc[country]=new_df["PopulationOver65"].loc[country]/(totalpop[country]/new_df["Population, total"].loc[country])
            
    newtotalpop=new_df[["Population0to14","Population15to34","Population35to64","PopulationOver65"]].sum(axis=1)        
    tabnames['Population0to14']='Population0to14'
    tabnames['Population15to34']='Population15to34'
    tabnames['Population35to64']='Population35to64'
    tabnames['PopulationOver65']='PopulationOver65'
    diff=newtotalpop-dataframe["Population, total"]
    x=diff>10000
    print(x.sum(), "countries have their populations deviate significantly.")
    #print(diff)

    return new_df, tabnames
    
def SortData(dataframe):
    
    """
    Sorts the data by region and country code. 
    
    ------
    Inputs
    ------
    dataframe:      The dataframe that needs to be sorted
    -------
    Outputs
    -------
    data_sorted:    The sorted dataframe
    """
    
    data_sorted = dataframe.sort_values(['Region', 'Country Data'], axis=0)
    return data_sorted

def DataCompleteness(dataframe):
    """
    Function to check how complete the dataset is. 
    
     ------
    Inputs
    ------
    dataframe:      The dataframe to check
    -------
    Outputs
    -------
    Prints the percentage of data available in the dataframe for each indicator
    
    """
    percentage = 100-(dataframe.isnull().sum()/len(dataframe))*100         # percentage of available data
    print(percentage)


def WriteToExcelWB(dataframe, tabnames, filename='testdata1.xlsx'):
    """
    Function that writes the (complete) dataframe to Excel in the correct
    format, with a separate tab for each indicator. 
    
    
    ------
    Inputs
    ------
    dataframe:  The dataframe to write to Excel
    tabnames:   The dictionary that contains the tabnames of the indicators
    filename:   The name of the resulting Excel file
    
    -------
    Outputs
    -------
    An Excelfile with a separate sheet for each indicator. 
    Each sheet contains a list of the countries with their values, and 
    a column containing the source of each value. 
    
    """
    
    writer = pd.ExcelWriter(filename)
    dont_include = ['Country Data', 'Region', 'IncomeGroup', 'Population0to14',
                    'Population15to34','Population35to64','PopulationOver65']
    
    source_cols=[col for col in dataframe.columns if 'source' in col]
    
    dataframe1=dataframe.drop(source_cols, axis=1)
    
    regional_data=pd.DataFrame(index=list(set(dataframe['Region'])), columns=['Region'])
    regional_data.to_excel(writer, 'Regional Data')
    
    for column in dataframe1.columns:
        if column in dont_include:
            df_to_write=pd.DataFrame(dataframe1[column])
            df_to_write.to_excel(writer, tabnames[column][:31])
        else:
            df_to_write = dataframe[[column,column+" source"]]
            df_to_write.to_excel(writer, tabnames[column][:31])
            

# WORLD BANK DATA
# In this section, the functions that are used to retrieve and format the 
# World Bank data are defined.  
            
            
           
def GetDataFromExcelEIA(file, indicatorfile):
    
    data=pd.read_excel(file,  sheetname='TimeSeries_1971-2015', header=0, skiprows=1, skip_footer=0, index_col=None, 
                    parse_cols=None, parse_dates=False, date_parser=None, 
                    na_values=None, thousands=None, convert_float=True, has_index_names=None, converters=None, dtype=None, 
                    true_values=None, false_values=None, engine=None, squeeze=False)
    
    data_needed=pd.read_excel(indicatorfile, sheetname=0, header=None, skiprows=None, skip_footer=0, 
                        parse_cols=None, parse_dates=False, date_parser=None, na_values=None, thousands=None, 
                        convert_float=True, has_index_names=None, converters=None, dtype=None, true_values=None, 
                        false_values=None, engine=None, squeeze=False)
    return data, data_needed
            
def CollectDataYearEIA(data, data_needed, countries, year=2014):
    """
    This function retrieves the indicators (specified in data_needed) from the IEA dataframe (data). 
    The data is retrieved for all countries in countries that are also in the IEA data. 
    Default year is 2014. 
    ------
    Inputs
    ------
    data:             Pandas Dataframe from the IEA Excel file
    data_needed:      The indicators from IEADATA.xlsx
    countries:        The list of countries we want data for (from regions.xlsx)
    year:             The initial year we want the data from (default = 2014)
    """
    filled_dataframe=countries.copy()
    for i in data_needed.index:
        df=data.loc[(data['Product'] == data_needed[0][i]) & (data['Flow']== data_needed[1][i])]
        df=df.replace("People's Republic of China", "China")
        df=df.replace("Korea", "Korea, Rep.")
        df=df.set_index('Country')
        column_name=data_needed[2][i]
        filled_dataframe[column_name]=None
        for index in df.index:    
            for index2 in countries.index:
                if index==index2:
                    filled_dataframe[column_name][index2]=df[year][index]
    return filled_dataframe

def GetRegionsEIA(data, countries):
    regions=[]
    for index in set(data['Country']):    
        if index in countries.index:
            continue
        else:
            if index!="People's Republic of China" and index!="Korea":
                regions.append(index)
            else:
                continue
    regions1=pd.DataFrame()
    regions1['Region']=regions
    regions1.set_index('Region', inplace=True)
    return regions1

def CollectRegionDataYearEIA(data, data_needed, regions, year=2014):
    """
    This function retrieves the indicators (specified in data_needed) from the IEA dataframe (data). 
    The data is retrieved for all countries in countries that are also in the IEA data. 
    Default year is 2014. 
    ------
    Inputs
    ------
    data:             Pandas Dataframe from the IEA Excel file
    data_needed:      The indicators from IEADATA.xlsx
    countries:        The list of countries we want data for (from regions.xlsx)
    year:             The initial year we want the data from (default = 2014)
    """
    filled_dataframe=regions.copy()
    for i in data_needed.index:
        df=data.loc[(data['Product'] == data_needed[0][i]) & (data['Flow']== data_needed[1][i])]
        df=df.replace("People's Republic of China", "China")
        df=df.replace("Korea", "Korea, Rep.")
        df=df.set_index('Country')
        column_name=data_needed[2][i]
        filled_dataframe[column_name]=None
        for index in df.index:    
            for index2 in regions.index:
                if index==index2:
                    filled_dataframe[column_name][index2]=df[year][index]
    return filled_dataframe

def FillWithPreviousYearsEIA(dataframe, data, data_needed, countries, year1=1990, year2=2014):
    """
    This function fills any missing data in the dataframe with data from previous years 
    (in the specified range).
    --------------
    Inputs:
    dataframe
    year1 (default=2010)
    year2 (default=2014)
    --------------
    """
    df_filled=dataframe
    for year in range(year2, year1, -1):
        df_year=pd.DataFrame(index=countries.index)
        for i in range(len(data_needed)):
            df=data.loc[(data['Product'] == data_needed[0][i]) & (data['Flow']== data_needed[1][i])]
            df=df.replace("People's Republic of China", "China")
            df=df.replace("Korea", "Korea, Rep.")
            df=df.set_index('Country')
            column_name=data_needed[2][i]
            df_year[column_name]=None
            for index in df.index:    
                for index2 in countries.index:
                    if index==index2:
                        df_year[column_name][index2]=df[year][index]
        df_filled=df_filled.combine_first(df_year)
    return df_filled



