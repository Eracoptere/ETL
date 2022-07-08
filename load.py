
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  8 10:05:03 2022

@author: Ahmed
"""
# import des librairies utiles
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date
import pandas as pd

# Initialisation du moteur de connection à la base de données
engine = create_engine("mysql+pymysql://root:root@localhost:3306/banque",pool_size=10, max_overflow=20)
Base = declarative_base()

# Creation de la structure des tables dans sqlalchemy
# Class de la table taux
def load_taux() :
    class Taux(Base) :
        __tablename__ = "taux"
        id = Column(Integer, primary_key = True)
        monnaie = Column(String(3))
        rate = Column(Float)
        date = Column(Date)   
    
    # Chargement de la table taux
    df_taux = pd.read_csv("C:/Users/Ahmed/Documents/csv/taux_de_change_transformed.csv")
    ligne_monnaie = list(df_taux['monnaie'])
    ligne_rate = list(df_taux['rates'])
    ligne_date = list(df_taux['date'])
    
    session = Session(engine)
    ligne_table_taux = [Taux(id = i+1,monnaie = ligne_monnaie[i] ,rate=ligne_rate[i],date = ligne_date[i],) for i in range(len(ligne_monnaie))]
    session.bulk_save_objects(ligne_table_taux)
    session.commit()
    session.close()



def load_banque() :
    # Class de la table classement de la banque
    class Banque(Base) :
        __tablename__ = "banque"
        id = Column(Integer, primary_key = True)
        rang = Column(Integer)
        nom = Column(String)
        market_cap = Column(Float)
        
    
    # Chargement de la table classement banque
    df_classement = pd.read_csv("C:/Users/Ahmed/Documents/csv/classement_banque_transformed.csv")
    ligne_rang = list(df_classement['Rank'])
    ligne_nom = list(df_classement['Bank name'])
    ligne_market_cap = list(df_classement['market_cap_€'])
    
    session = Session(engine)
    ligne_table_classement = [Banque(id = i+1,rang = ligne_rang[i] ,nom=ligne_nom[i],market_cap = ligne_market_cap[i],) for i in range(len(ligne_rang))]
    session.bulk_save_objects(ligne_table_classement)
    session.commit()
    session.close()


def load_foncieres() :
    # Class de la table des valeurs foncieres 
    class Foncieres(Base) :
        __tablename__ = "foncieres"
        id = Column(Integer, primary_key = True)
        date_mutation = Column(String)
        nature_mutation = Column(String)
        valeur_fonciere = Column(String)
        code_postal = Column(String)
        commune = Column(String)
        type_local = Column(String)
        surface_reelle_batie = Column(String)
        nombre_pieces_principales = Column(String)
        surface_terrain = Column(String)
        
    # Chargement de la table foncieres
    df_foncier = pd.read_csv("C:/Users/Ahmed/Documents/csv/foncier_transformed.csv")
    
    df_foncier['Surface reelle bati'] = df_foncier['Surface reelle bati'].fillna('0.123456789')
    df_foncier['Valeur fonciere'] = df_foncier['Valeur fonciere'].fillna('0.123456789')
    df_foncier['Nombre pieces principales'] = df_foncier['Nombre pieces principales'].fillna('0.123456789')
    df_foncier['Surface terrain'] = df_foncier['Surface terrain'].fillna('0.123456789')
    df_foncier = df_foncier.fillna('inconnu')
    
    ligne_date_mutation = list(df_foncier['Date mutation'])
    ligne_nature_mutation = list(df_foncier['Nature mutation'])
    ligne_valeur_fonciere = list(df_foncier['Valeur fonciere'])
    ligne_code_postal = list(df_foncier['Code postal'])
    ligne_commune = list(df_foncier['Commune'])
    ligne_type_local = list(df_foncier['Type local'])
    ligne_surface_reelle_batie = list(df_foncier['Surface reelle bati'])
    ligne_nombre_pieces = list(df_foncier['Nombre pieces principales'])
    ligne_surface_terrain = list(df_foncier['Surface terrain'])
    
    session = Session(engine)
    ligne_table_foncieres = [Foncieres(id = i+1, date_mutation = ligne_date_mutation[i], nature_mutation=ligne_nature_mutation[i], \
                            valeur_fonciere = ligne_valeur_fonciere[i], code_postal=ligne_code_postal[i], commune=ligne_commune[i],\
                            type_local = ligne_type_local[i], surface_reelle_batie = ligne_surface_reelle_batie[i], \
                            nombre_pieces_principales = ligne_nombre_pieces[i], surface_terrain = ligne_surface_terrain[i],) \
                            for i in range(len(ligne_nature_mutation))]
    session.bulk_save_objects(ligne_table_foncieres)
    session.commit()
    session.close()
    

def main():
    load_taux()
    load_banque()
    load_foncieres()
    
main()