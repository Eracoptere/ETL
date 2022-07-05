# -*- coding: utf-8 -*-
"""
Extraction des données
"""

def extract_foncier():
    
    """Extrait un csv depuis une url data.gouv les données foncières de l'année publiée la plus récente puis transforme les données brutes en
    un dataframe conforme à la volonté du client"""

    from bs4 import BeautifulSoup
    import requests
    import os
    
    #url de la source de données 
    url_foncier="https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/"
    
    #Extraction du code html de la page/Obtention du dernier lien en date
    response = requests.get(url_foncier)
    soup=BeautifulSoup(response.text, features="lxml")
    target=soup.find("li", {"class":"fr-col-auto"}).find('a')
    source_foncier=target.get('href')
    
    #Extraction du csv et import des données
    foncier_csv_link=requests.get(source_foncier)
    
    local_path_csv='C:/Users/utilisateur/Documents/csv'
    os.makedirs(local_path_csv,exist_ok=True)
    
    foncier_csv=local_path_csv+('/foncier_base.csv')
    with open(foncier_csv, "wb") as f:
        f.write(foncier_csv_link.content)
        
        

def extract_bank():
    import pandas as pd
    
    #Url source
    url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
    
    #Extraction du code html
    dfs = pd.read_html(url)
    
    #Conversion en csv de la table qui nous interesse (market capitalization)
    df = dfs[3]
    df.to_csv('C:/Users/utilisateur/Documents/csv/banque_base.csv')
    
    
def extract_taux_change():
    import requests
    import pandas as pd
    
    # Extraction des données taux de change via une API
    url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=ngVF0Ysuxr6dV03od7TpSm37lF3F30yC"
    reponse = requests.get(url)
    
    df_exchange_rates = pd.DataFrame(reponse.json())
    
    # Suppression des colonnes non necessaire
    exchange_rates = df_exchange_rates[['date','rates']]
    exchange_rates['monnaie'] = list(exchange_rates.index)
    
    taux = exchange_rates[['monnaie','rates','date']]

    # Conversion du dataframe en csv
    taux.to_csv("C:/Users/utilisateur/Documents/csv/taux_de_change_base.csv", index = False)
    
    
def main():
    extract_foncier()
    extract_bank()
    extract_taux_change()
    
main()