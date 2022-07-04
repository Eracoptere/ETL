#!/usr/bin/env python
# coding: utf-8

"""Extrait un csv depuis une url data.gouv les données foncières de l'année publiée la plus récente puis transforme les données brutes en
un dataframe conforme à la volonté du client"""

import pandas as pd
from bs4 import BeautifulSoup
import requests

#url de la source de données 
url_foncier="https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/"

#Extraction du code html de la page/Obtention du dernier lien en date
response = requests.get(url_foncier)
soup=BeautifulSoup(response.text)
target=soup.find("li", {"class":"fr-col-auto"}).find('a')
source_foncier=target.get('href')

#Extraction du csv et import des données
foncier_csv_link=requests.get(source_foncier)
local_path_foncier='C:/Users/utilisateur/Documents/ETL/foncier_base.csv'
with open(local_path_foncier, "wb") as f:
    f.write(foncier_csv_link.content)
    
df0_foncier=pd.read_csv(local_path_foncier, sep='|')
print("Voici les données extraites:", df0_foncier)

#Copie du fichier d'origine
df_foncier=df0_foncier.copy()

#print(df_foncier.shape)
#print(df_foncier.isna().sum())

#Colonne à supprimer
df_foncier=df_foncier[['Date mutation', 'Nature mutation', 'Valeur fonciere', 'No voie',
       'B/T/Q', 'Type de voie', 'Voie', 'Code postal', 'Commune', 'Type local',
       'Surface reelle bati', 'Nombre pieces principales', 'Surface terrain']]

#Liste des valeurs à convertir en float
int_list=["Valeur fonciere","No voie","Code postal","Surface reelle bati","Nombre pieces principales","Surface terrain"]

#Remplacement des virgules par des points pour la colonne "Valeurs fonciere"
df_foncier['Valeur fonciere']=df_foncier['Valeur fonciere'].str.replace(',','.')


#Conversion de object vers float pour les colonnes concernées
for colonne in int_list:
    df_foncier[colonne]=df_foncier[colonne].astype(float)

#Changement de format de date
df_foncier['Date mutation']=pd.to_datetime(df_foncier['Date mutation'])


#Sauvegarde des données transformées 
df_foncier.to_csv('C:/Users/utilisateur/Documents/ETL/foncier_transform.csv')