# -*- coding: utf-8 -*-
"""
Transformation des csv pour ne conserver que les variables voulues dans un 
format plus lisible et exploitable
"""

def transform():
    import pandas as pd
    import re
    local_path_csv='C:/Users/utilisateur/Documents/csv'
    
    ##Transformation du fichier contenant les informations foncières
    
    #Déclaration d'une variable contenant les données
    fic=local_path_csv+"/foncier_base.csv"
    df_foncier=pd.read_csv(fic, sep='|')

    #Colonne à supprimer
    df_foncier=df_foncier[["Date mutation","Nature mutation","Valeur fonciere","Code postal",
    			   "Commune","Type local","Surface reelle bati","Nombre pieces principales","Surface terrain"]]
    
    #Liste des valeurs à convertir en float
    int_list=["Valeur fonciere","Code postal","Surface reelle bati","Nombre pieces principales","Surface terrain"]

    #Remplacement des virgules par des points pour la colonne "Valeurs fonciere"
    df_foncier['Valeur fonciere']=df_foncier['Valeur fonciere'].str.replace(',','.')
    
    
    #Conversion de object vers float pour les colonnes concernées
    for colonne in int_list:
        df_foncier[colonne]=df_foncier[colonne].astype(float)
        
    #Changement de format de date
    df_foncier['Date mutation']=pd.to_datetime(df_foncier['Date mutation'])
        
        
    #Sauvegarde des données transformées 
    df_foncier.to_csv(local_path_csv+"foncier_transformed.csv")
    
    
    
    ##Transformation du fichier contenant les informations sur le taux de change
    
    #Déclaration d'une variable contenant les données
    fic=local_path_csv+"/taux_de_change_base.csv"
    df_exchange_rates=pd.read_csv(fic)
    
    # Suppression des colonnes non necessaire
    df_exchange_rates = df_exchange_rates[['date','rates']]
    df_exchange_rates['monnaie'] = list(df_exchange_rates.index)

    taux = df_exchange_rates[['monnaie','rates','date']]

    # Conversion du dataframe en csv
    taux.to_csv("taux_de_change.csv", index = False)
    
    
    
    ##Transformation du fichier contenant les informations sur les banques
    
    #Déclaration d'une variable contenant les données
    fic=local_path_csv+"/banque_base.csv"
    df_classement=pd.read_csv(fic)
    
    # Récupération du taux de change du dollar vers l'euro
    tx_change_dollar_to_euros = df_exchange_rates['rates'][149]
    
    # Création nouvelle colonne avec $ convertis en €, ainsi que str en float
    df_classement['market_cap_€'] = df_classement['Market cap(US$ billion)']
    df_classement['market_cap_€'] = [re.split("\[",i)[0] for i in df_classement['market_cap_€']]
    df_classement['market_cap_€'] = df_classement['market_cap_€'].astype(float)
    for value in range(len(df_classement['market_cap_€'])):
        df_classement['market_cap_€'][value] = df_classement['market_cap_€'][value]/tx_change_dollar_to_euros
        df_classement['market_cap_€'][value] = round(df_classement['market_cap_€'][value], 2)
    
    # Suppression colonnes inutiles
    df_classement = df_classement.drop(columns={'Market cap(US$ billion)', 'Unnamed: 0'})
    
    # Conversion du dataframe en csv
    classement = df_classement
    classement.to_csv("classement_banque.csv", index = False)