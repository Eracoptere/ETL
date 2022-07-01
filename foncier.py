#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


#url de la source de données 
#url_foncier="https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/"


# In[3]:


#Import des données
df0_foncier=pd.read_csv('./Simplon/PG DE/valeursfoncieres-2021.txt', sep='|')


# In[4]:



df_foncier=df0_foncier.copy()
print(df_foncier.shape)
print(df_foncier.isna().sum())


# In[5]:


#Colonne à supprimer
df_foncier.drop(columns=["Code service CH", "Reference document","1 Articles CGI","2 Articles CGI","3 Articles CGI",
                         "4 Articles CGI","5 Articles CGI","Code type local","No Volume","1er lot","Surface Carrez du 1er lot",
                         "2eme lot","Surface Carrez du 2eme lot","3eme lot","Surface Carrez du 3eme lot","4eme lot","Surface Carrez du 4eme lot",
                         "5eme lot","Surface Carrez du 5eme lot","Section","No plan","Nombre de lots",
                         "Section","No plan","Identifiant local","No disposition","Code departement","Code commune",
                         "Nature culture","Nature culture speciale","Code voie","Prefixe de section"], axis=1, inplace=True)


# In[6]:


#Liste des valeurs à convertir en float
int_list=["Valeur fonciere","No voie","Code postal","Surface reelle bati","Nombre pieces principales","Surface terrain"]


# In[7]:


#Remplacement des virgules par des points pour la colonne "Valeurs fonciere"
df_foncier['Valeur fonciere']=df_foncier['Valeur fonciere'].str.replace(',','.')


# In[8]:


#Conversion de object vers float pour les colonnes concernées
for colonne in int_list:
    df_foncier[colonne]=df_foncier[colonne].astype(float)


# In[12]:


#Changement de format de date
df_foncier['Date mutation']=pd.to_datetime(df_foncier['Date mutation'])
print(df_foncier)

