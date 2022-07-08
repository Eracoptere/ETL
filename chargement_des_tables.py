# -*- coding: utf-8 -*-
"""
Created on Thu Jul  7 09:39:36 2022

@author: Ahmed
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date

# creation
engine = create_engine("mysql+pymysql://root:password@localhost:3306/banque",pool_size=10, max_overflow=20)

session = Session(engine)
Base = declarative_base()


class Taux(Base) :
    __tablename__ = "taux"
    id = Column(Integer, primary_key = True)
    monnaie = Column(String(3))
    rate = Column(Float)
    date = Column(Date)   
    
    

class Foncieres(Base) :
    __tablename__ = "foncieres"
    id = Column(Integer, primary_key = True)
    date_mutation = Column(Date)
    nature_mutation = Column(String,nullable = True)
    valeur_fonciere = Column(Float,nullable = True)
    code_postal = Column(String,nullable = True)
    commune = Column(String,nullable = True)
    type_local = Column(String,nullable = True)
    surface_reelle_batie = Column(Float,nullable = True)
    nombre_pieces_principales = Column(Integer,nullable = True)
    surface_terrain = Column(Float,nullable = True)


class Banque(Base) :
    __tablename__ = "banque"
    id = Column(Integer, primary_key = True)
    rang = Column(Integer)
    nom = Column(String)
    market_cap = Column(Float)
    
    
import pandas as pd
print('debut des tables')
df_taux = pd.read_csv("C:/Users/utilisateur/Documents/csv/taux_de_change.csv")
ligne_monnaie = list(df_taux['monnaie'])
ligne_rate = list(df_taux['rates'])
ligne_date = list(df_taux['date'])

session = Session(engine)
ligne_table_taux = [Taux(id = i+1,monnaie = ligne_monnaie[i] ,rate=ligne_rate[i],date = ligne_date[i],) for i in range(len(ligne_monnaie))]
session.bulk_save_objects(ligne_table_taux)
session.commit()
session.close()

print('table taux faite')


df_classement = pd.read_csv("C:/Users/utilisateur/Documents/csv/classement_banque.csv")
ligne_rang = list(df_classement['Rank'])
ligne_nom = list(df_classement['Bank name'])
ligne_market_cap = list(df_classement['market_cap_€'])

session = Session(engine)
ligne_table_classement = [Banque(id = i+1,rang = ligne_rang[i] ,nom=ligne_nom[i],market_cap = ligne_market_cap[i],) for i in range(len(ligne_rang))]
session.bulk_save_objects(ligne_table_classement)
session.commit()
session.close()

print('table banque faite')

df_foncier = pd.read_csv("C:/Users/utilisateur/Documents/csv/foncier_transformed.csv")

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
print('table fonciere faite')
print('ok')

