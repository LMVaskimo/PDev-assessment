# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 14:33:00 2024

@author: Lotta Vaskimo
Deze package zorgt voor het opschonen, filteren en samenvoegen van datasets
over aardgasverbruik in speciale onderzoekswijken.
input:
    bag_fictief.csv
    gas_fictief.csv
    onderzoekswijken_fictief.csv
output:
    gemiddeld_aardgasverbruik.csv
    Drops/drop_dup_onderzoekswijken.csv
    Drops/drop_imputed.csv
    Drops/drop_nostock_BAG.csv
    Drops/dropped_NaNs_OWZ_BAG.csv
    Drops/dropped_NaNs_OWZ_BAG_GAS.csv
"""
import os
import pandas as pd


class Aardgasonderzoek:
    
    def __init__(self, jaar, bag, onderzoekswijken, gas):
        self.jaar = jaar
        self.bag = bag
        self.onderzoekswijken = onderzoekswijken
        self.gas = gas
    
    def get_uniques(self):
        """
        Vind unieke adressen in het onderzoekswijken bestand
        input: self: Het Python Object jaarlijksonderzoek
        output: onderzoekswijken_nodupes: Dataframe met alle unieke adressen 
                uit het onderzoekswijken bestand
        """         
        onderzoekswijken_nodupes = self.onderzoekswijken.drop_duplicates()
        return onderzoekswijken_nodupes
        
    def find_duplicates(self):
        """
        Vind duplicaten in het onderzoekswijken bestand
        input: self : Het Python Object jaarlijksonderzoek
        output: een bestand met de gedropte regels
        """      
        try:
            os.mkdir("Results\\"+ str(self.jaar))
            os.mkdir("Results\\"+ str(self.jaar) +"\\Drops")
        except:
            pass
        onderzoekswijken_dupes = \
            self.onderzoekswijken[self.onderzoekswijken.duplicated() == True]
        print("Er zijn in het onderzoekswijken bestand", 
              len(onderzoekswijken_dupes), 
              "duplicaten gevonden. Deze zijn verwijderd en verplaatst naar:",
                  ".\\Results\\"+str(self.jaar)+
                  "\\Drops\\dropped_dupes_onderzoekswijken.csv")
        onderzoekswijken_dupes.to_csv(path_or_buf = ".\\Results\\"+ 
                                      str(self.jaar) +
                                      "\\Drops\\drop_dup_onderzoekswijken.csv", 
                                      sep=';')

    def find_nostock(self):
        """
        Vind objecten/woningen die niet op voorraad zijn in het
        bag bestand
        input: self : Het Python Object jaarlijksonderzoek
        output: een bestand met de gedropte regels
        """    
        bag_nostock = self.bag.loc[self.bag["VBOvoorraadtype"] == 0]
        bag_nostock.to_csv(path_or_buf = ".\\Results\\"+ str(self.jaar) +
                           "\\Drops\\drop_nostock_BAG.csv",
                           sep=';')
        
    def get_instock(self):
        """
        Vind objecten/woningen die wel op voorraad zijn in het
        bag bestand
        input: self : Het Python Object jaarlijksonderzoek
        output: bag_instock : Een dataframe met objecten/woningen die op 
                              voorraad zijn in het bag bestand
        """    
        bag_instock = self.bag.loc[self.bag["VBOvoorraadtype"] == 1]
        return bag_instock
    
    def connect(self, dataframe1, dataframe2, connectorlist):
        """
        Verbind twee dataframes met elkaar door middel van overeenkomende 
        kolommen
        input: self : Het Python Object jaarlijksonderzoek
               dataframe1 : Het eerste dataframe waaraan een tweede gevoegd 
               word
               dataframe2 : De tweede dataframe waaraan een tweede gevoegd 
               word
               connectorlist : Een lijst met kolomnamen waarop de twee 
               dataframes verbonden worden
        output: merged_dataframes : Een nieuwe dataframe waar de dataframe1 en 
                dataframe2 met gemerged zijn
        """    
        merged_dataframes = pd.merge(dataframe1, dataframe2,
                                 how = 'left', 
                                 on = connectorlist)
        return merged_dataframes
    
    def remove_nans(self, toclean):
        """
        Verwijdert regels uit een dataframe waar missende waarden voorkomen
        input: self : Het Python Object jaarlijksonderzoek
               toclean : Dataframe waaruit missende waarden verdwijdert moeten
               worden
        output: cleaned : Dataframe waaruit de missende waarden verwijdert zijn
        """    
        cleaned = toclean.dropna()
        return cleaned
    
    def find_nans_and_clean(self, dataframe, filesuffix):
        """ 
        Vind regels uit een dataframe met missende waarden en schrijft deze weg
        naar een apart bestand. Roept functie remove_nans aan om een dataframe
        te verkrijgen die gefiltert is op deze missende waarden.
        input: self : Het Python Object jaarlijksonderzoek
               dataframe : Dataframe waaruit missende waarden gefiltert en 
               verdwijdert moeten worden
               filesuffix : een naam voor het weg te schrijven bestand
        output: een bestand met de gedropte regels
                cleaned : Dataframe waaruit de missende waarden verwijdert zijn
        """    
        nans =  dataframe[dataframe.isnull().any(axis=1)] 
        nans.to_csv(path_or_buf = ".\\Results\\"+ str(self.jaar) +
                                  "\\Drops\\dropped_NaNs_"+ 
                                  filesuffix +
                                  ".csv",
                                  sep=';')  
        cleaned =  self.remove_nans(dataframe)
        return cleaned
    
    def get_imputed(self, compleet_aardgasverbruik):
        """
        Vind de geimputeerde waarden uit het gas bestand. Filtert het gas
        bestand zodast alle geimputeerde waarden verwijdert worden en weg
        geschreven worden naar een apart bestand.
        input: self : Het Python Object jaarlijksonderzoek
               compleet_aardgasverbruik : dataframe waar het gas,
               bag en onderzoekswijken bestand gecombineert zijn. Hierin zitten
               geen missende of duplicate waarden / regels
        output: ogdata : dataframe waar het alleen de echte metingen van gas in
        zitten. Deze dataframe is een combinatie van de gas,
        bag en onderzoekswijken dataframes. Hierin zitten
        geen missende of duplicate waarden / regels
        """    
        ogdata = compleet_aardgasverbruik\
            .loc[compleet_aardgasverbruik["imputeren_gas_2020"] == 0]
        imputed = compleet_aardgasverbruik\
            .loc[compleet_aardgasverbruik["imputeren_gas_2020"] == 1]
        print("De hoeveelheid geimputeerde en dus verloren data is:"
              , len(imputed), 
              "\nVolledige lijst is te vinden in:",
              ".\\Result"+ str(self.jaar)+
                  "\\Drops\\dropped_imputed_merge_OZW_BAG_GAS.csv")

        imputed.to_csv(path_or_buf = ".\\Results\\"+ str(self.jaar) +
                                      "\\Drops/drop_imputed.csv", 
                                      sep=';')
        return ogdata
    
    def bereken_gemiddeld_verbruik(self, ogdata, wijknamen):
        """
        Berekent het gemiddelde verbruik per onderzoekswijk en print dit naar
        de gebruiker. Ook word het geschreven naar een apart bestand.
        input: self : Het Python Object jaarlijksonderzoek
               ogdata : dataframe waar het alleen de echte metingen van gas in
               zitten. Deze dataframe is een combinatie van de gas,
               bag en onderzoekswijken dataframes. Hierin zitten
               geen missende of duplicate waarden / regels
               wijknamen : een lijst met uniekewijknamen uit de 
               onderzoekswijken dataframe
        output: Een bestand waarin het gemiddelde gasverbruik en aantal 
                woningen per wijk staat(alle huizen die gasverbruik hoger dan 0
                hebben)
        """    
        verbruik = []
        woningen = []
        for wijk in wijknamen:
            gemiddelde_verbruik = ogdata\
                .loc[ogdata['Wijknaam'] == wijk, 'G_def'].mean()
            aantal_woningen = len(ogdata.loc[ogdata['Wijknaam'] == wijk])
            print("In de wijk", wijk, "zijn", aantal_woningen, 
                  "woningen, welke totaal een gemiddeld gebruik hebben van", 
                  round(gemiddelde_verbruik, 2), "m3")
            verbruik.append(round(gemiddelde_verbruik, 2))
            woningen.append(aantal_woningen)
        gemiddelden = pd.DataFrame({
            "Wijken": wijknamen,
            "Gemiddeld verbruik in m3": woningen,
            "Aantal woningen op aardgas in de wijk": woningen
            })
        gemiddelden.to_csv(path_or_buf = ".\\Results\\"+ str(self.jaar) +
                                      "\\gemiddeld_aardgasverbruik.csv", 
                                      sep=';')
        print("Een samenvatting van deze data is te vinden in:",
              "./Results/gemiddeld_aardgasverbruik.csv")
            
    def get_gemiddeld_verbruik(self, ogdata):
        """
        Roept de functie bereken_gemiddeld_verbruik. zorgt dat er alleen
        huizen met aardgas verbruik gebruikt worden voor het berekenen van het
        gemiddelde. Maakt een lijst van de unieke wijknamen uit het onderzoeks-
        wijken bestand.
        input: self : Het Python Object jaarlijksonderzoek
               ogdata : dataframe waar het alleen de echte metingen van gas in
               zitten. Deze dataframe is een combinatie van de gas,
               bag en onderzoekswijken dataframes. Hierin zitten
               geen missende of duplicate waarden / regels
        output: -
        """    
        ogdata = ogdata.loc[ogdata['G_def'] != 0]
        wijknamen = ogdata["Wijknaam"].unique()
        self.bereken_gemiddeld_verbruik(ogdata, wijknamen)
        
    def run_volledig_onderzoek(self):
        """
        Roept alle functies aan zodat het onderzoek altijd op de juiste manier
        verloopt. Deze functie dient aangeroepen te worden als het onderzoek
        gedaan moet worden.
        input: self : Het Python Object jaarlijksonderzoek
        output: -
        """    
        onderzoekswijken_nodupes = self.get_uniques()
        self.find_duplicates()
        instock =  self.get_instock()
        self.find_nostock()
        OWZ_BAG = self.connect(onderzoekswijken_nodupes, 
                               instock, ['a_pc_num',
                                          'a_pc_alf', 
                                          'a_huisnum',  
                                          'Toev_new'])
        cleaned_OWZ_BAG =  self.find_nans_and_clean(OWZ_BAG, "OWZ_BAG")
        OWZ_BAG_GAS =  self.connect(cleaned_OWZ_BAG, self.gas, ["VBOid"])
        compleet_aardgasverbruik =  self.find_nans_and_clean(OWZ_BAG_GAS, 
                                                       "OWZ_BAG_GAS")
        ogdata =  self.get_imputed(compleet_aardgasverbruik)
        self.get_gemiddeld_verbruik(ogdata)
