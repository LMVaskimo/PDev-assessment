# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 14:00:25 2024

@author: Lotta Vaskimo
"""
from Aardgas_verbruik import Aardgasonderzoek
import os
import pandas as pd


def main():
    """
    De main functie maakt een map voor de resultaten en leest de benodigde
    bestanden in. Deze bestanden kunnen verandert worden voor het volgende
    onderzoek. De package Aardgasonderzoek word aangeroepen en een nieuw 
    onderzoek word gestart. Resultaten kunnen gevonden worden in de map met 
    het ingevulde jaar van het onderzoek. Resultaten uit opeenvolgende jaren
    zullen niet overschreven worden. Roept functie start_aardgasonderzoek
    aan om het onderzoek te starten    
    """
    try:
        os.mkdir("./Results")
    except:
        pass
    bag = pd.read_csv('ProefOntwikkelaar//bag_fictief.csv', sep=';')
    gas = pd.read_csv('ProefOntwikkelaar//gas_fictief.csv', sep=';')
    onderzoekswijken = \
        pd.read_csv('ProefOntwikkelaar//onderzoekswijken_fictief.csv', sep=';')
    
    jaarlijksonderzoek2023 = Aardgasonderzoek(2023, bag, onderzoekswijken, gas)
    jaarlijksonderzoek2023.run_volledig_onderzoek()
   
    
main()
