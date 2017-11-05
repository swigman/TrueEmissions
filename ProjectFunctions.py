#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 12:50:11 2017

@author: Patrick Steinmann and Stefan Wigman
"""

import pandas as pd
from geopy.geocoders import Nominatim
import plotly
import datetime
import wbdata

plotly.offline.init_notebook_mode(connected=True)
geolocator = Nominatim()

def build_multi_index_df(years, countries):
    
    """
    -------
    
    Function which builds multi index dataframe suitable for this analysis.
    The MultiIndex array is three-dimensional: year*country*country. 
    
    -------
    Inputs:
        - years     : A list of the years to include in
                      the multi-index dataframe.
                      
        - countries : A list of the countries to include 
                      in the multi-index dataframe.
    -------
    Ouputs:
        - dataframe : The resulting multi-index dataframe.
    -------
        
    """ 

    #build MultiIndex array
    rows_array = []
    for year in years:
        for country in countries:
            rows_array.append([year,country])
        
    multi_index = pd.MultiIndex.from_tuples(rows_array, names=['year', 'exporter'])
    dataframe = pd.DataFrame(columns=countries, index=multi_index)
    return dataframe

def FillWithTradeData(data, multi_index_dataframe, years):
    """
    -------
    
    This function fills the multi-index dataframe with the data
    contained in the dataset. 
    
    -------
    Inputs:
        - data (the dataframe containing trade data)
        - dataframe (the empty multi-index dataframe)
    -------
    Outputs:
        - dataframe: a multi-index dataframe filled with the data
    -------
    """
    for index, row in data.iterrows():
        for year in years:
           year_key=str(year)+" in 1000 USD "
           multi_index_dataframe.loc[year][row['ReporterName']][row['PartnerName']]=row[year_key]
    return multi_index_dataframe

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
    -------
    Outputs
    -------
    dataframe:      The resulting dataframe
    
    """
    countries=pd.read_excel(file, sheetname=sheetname, header=header, 
                          skiprows=skiprows, skip_footer=skip_footer, 
                          index_col=index_col, names=names)
    return countries

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

def CheckDictionaries(dic1, dic2):
    """
    
    This function takes two dictionaries as input and prints values that are
    contained in one dictionary, but not in the other and vice versa. 
    
    """
    print('---------------------------')
    print('Items in dic1 but not in dic2:')
    for item in dic1:
        if item in dic2:
            continue
        else:
            print(item, dic1[item])
        
    print('---------------------------')
    print('Items in dic2 but not in dic1:')
    for item in dic2:
        if item in dic1:
            continue
        else:
            print(item, dic2[item])

def MergeDataFrames(dataframe_WB, dataframe_trade, country_dic_wb, country_dic_trade, conversion_dic):         
    
    """
    
    This function merges the World Bank dataframe and the Trade dataframe.
    
    """
    
    
    filled_dataframe=dataframe_trade.copy(deep=False)
    for column in dataframe_WB:
        column_name=column
        filled_dataframe[column_name]=None
        for index in dataframe_trade.index:
            for index2 in dataframe_WB.index:    
                if index==index2 or country_dic_trade[index]==country_dic_wb[index2]:
                    filled_dataframe[column_name][index]=dataframe_WB[column][index2]
                else:
                    try:
                        if conversion_dic[index]==country_dic_wb[index2]:
                            filled_dataframe[column_name][index]=dataframe_WB[column][index2]
                    except KeyError: 
                        continue
    return filled_dataframe

def GetCountryCoordinates(country):
    '''
    Inputs country. Returns the lat/long coordinates the center of the country.
    '''
    loc = geolocator.geocode(country)
    try:
        return (loc.latitude, loc.longitude)
    except:
        print('No location found for '+country)
        return (0,0)
    
def AddCoordinatesColumn(dataframe):
    """
    
    This function adds a coordinates column to a dataframe with countries as index. 
   
    """
    dataframe['Latitude']=None
    dataframe['Longitude']=None
    for country in dataframe.index:
        coords=GetCountryCoordinates(country)
        dataframe['Latitude'][country]=coords[0]
        dataframe['Longitude'][country]=coords[1]
    return dataframe

def RemoveEmissionColumns(dataframe):
    """
    This function removes emission columns for countries not in the index. 
    """
    columns_to_remove=[]
    for column in dataframe:
        country_name = column.replace('Emissions to ', '')
        if country_name not in dataframe.index:
            print('Removing', country_name)
            columns_to_remove.append('Emissions to '+country_name)
    dataframe.drop(columns_to_remove, axis=1, inplace=True)
    return dataframe

def EmissionFlowDataFrame(dataframe, data):
    """
    Input:
        - Emission Dataframe (emission_data)
        - data (complete dataframe)
    """
    export_country=[]
    import_country=[]
    emissions_transferred=[]

    for country in dataframe.index:
        for column in dataframe.columns:
            if dataframe[column][country]>0.1:
                exp_long=data['Longitude'][country]
                exp_lat=data['Latitude'][country]
                exp_tup=(exp_long, exp_lat)
                column_country = column.replace("Emissions to ", "")
                imp_long=data['Longitude'][column_country]
                imp_lat=data['Latitude'][column_country]
                imp_tup=(imp_long, imp_lat)
            
                emissions_to_country=dataframe[column][country]
            
                export_country.append(exp_tup)
                import_country.append(imp_tup)
                emissions_transferred.append(emissions_to_country)
    
    coords_df_export=pd.DataFrame(export_country)
    coords_df_import=pd.DataFrame(import_country)
    coords_df_emissions=pd.DataFrame(emissions_transferred)
    coords_df=pd.concat([coords_df_export, coords_df_import, coords_df_emissions], axis=1)
    coords_df.columns=['start_lon', 'start_lat', 'end_lon', 'end_lat', 'emissions']
    return coords_df

def VisualizeFlowsFromCountry(country, emissions_dataframe, filled_dataframe):
    
    emission_country=emissions_dataframe.loc[country].reset_index().T
    emission_country.columns = emission_country.iloc[0]
    emission_country=emission_country.drop('index')
    df_country=EmissionFlowDataFrame(emission_country, filled_dataframe)
    filename="EmissionFlows"+country+".html"
    title="Emission Flows from " +country+ " to other countries"
    url=EmissionFlowPlot(df_country, filename=filename, title=title)
    return url

def VisualizeFlowsToCountry(country, emissions_dataframe, filled_dataframe):
    emission_to_country=pd.DataFrame(emissions_dataframe['Emissions to '+country])
    df_country=EmissionFlowDataFrame_TO(emission_to_country, filled_dataframe)
    filename="EmissionFlows"+country+".html"
    title="Emission Flows to " +country+"from other countries"
    url=EmissionFlowPlot(df_country, filename=filename, title=title)
    return url

def EmissionFlowDataFrame_TO(dataframe, data):
    """
    Input:
        - Emission Dataframe (emission_data)
        - data (complete dataframe)
    """
    export_country=[]
    import_country=[]
    emissions_transferred=[]

    for country in dataframe.index:
        for column in dataframe.columns:
            if dataframe[column][country]>0.1:
                imp_long=data['Longitude'][country]
                imp_lat=data['Latitude'][country]
                imp_tup=(imp_long, imp_lat)
                column_country = column.replace("Emissions to ", "")
                exp_long=data['Longitude'][column_country]
                exp_lat=data['Latitude'][column_country]
                exp_tup=(exp_long, exp_lat)
            
                emissions_to_country=dataframe[column][country]
            
                export_country.append(exp_tup)
                import_country.append(imp_tup)
                emissions_transferred.append(emissions_to_country)
    
    coords_df_export=pd.DataFrame(export_country)
    coords_df_import=pd.DataFrame(import_country)
    coords_df_emissions=pd.DataFrame(emissions_transferred)
    coords_df=pd.concat([coords_df_export, coords_df_import, coords_df_emissions], axis=1)
    coords_df.columns=['start_lon', 'start_lat', 'end_lon', 'end_lat', 'emissions']
    return coords_df


def EmissionFlowPlot(coords_df, filename='EmissionFlows.html' ,title):
    """
    
    This function plots the transferred emissions from country to country. 
    Larger transfers are represented by a thicker line. 
    
    """
    emission_transfers = []
    for i in range( len( coords_df ) ):
        emission_transfers.append(
                dict(
                    type = 'scattergeo',
                    locationmode = 'country names',
                    lon = [ coords_df['start_lon'][i], coords_df['end_lon'][i] ],
                    lat = [ coords_df['start_lat'][i], coords_df['end_lat'][i] ],
                    mode = 'lines',
                    line = dict(
                    width = 5*float(coords_df['emissions'][i])/float(coords_df['emissions'].max()),
                    color = 'red',
                    ),
                    opacity = float(coords_df['emissions'][i])/float(coords_df['emissions'].max()),
                    )
                                )
    layout = dict(
        title = title,
        showlegend = False,
        geo = dict(
            showlakes = True,
            showcountries = True,
            showocean = True,
            countrywidth = 0.5,
            landcolor = 'rgb(230, 145, 56)',
            lakecolor = 'rgb(0, 255, 255)',
            oceancolor = 'rgb(0, 255, 255)',
            projection=dict( type='orthographic' ),
            showland = True,
            countrycolor = 'rgb(204, 204, 204)',
        ),
    )

    fig = dict( data=emission_transfers, layout=layout )
    url = plotly.offline.plot( fig, filename=filename)
    return url
        

def CalculatePercentages(dataframe, years):
    """
    
    This function calculates the percentages of the emissions that should be 
    transferred from one country to another. 
    
    """
    percentages=dataframe.copy()
    for year in years:
        df1 = percentages.loc[year].div(percentages.loc[year].sum(axis=1), axis=0)
        df2 = df1.fillna(0)
        percentages.loc[year].update(df2)
        
    return percentages

def DataPointsPerExporter(dataframe, years):
    """
    This function calculates how many datapoints there 
    are for each exporting country.
    Input: 
    - dataframe (percentages)
    - years
    """
    data_points = (dataframe.loc[1995] != 0).sum(axis=1).to_frame()
    data_points.columns = ['1995']

    #skip first, as it is used above to create dataframe
    #must overwrite column names each loop cycle, as df.assign() interprets column name as literal
    i=1
    for year in years[1:]:
        i=i+1
        this = (dataframe.loc[year] != 0).sum(axis=1)
        data_points = data_points.assign(temp = this)
        data_points.columns = [years[:i]]

    return data_points

