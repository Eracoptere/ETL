# -*- coding: utf-8 -*-
"""
Created on Fri Jul  8 10:05:03 2022

@author: utilisateur
"""
# import des librairies utiles
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import column_property
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import Column, Integer, String
import pandas as pd

# Initialisation du moteur de connection à la base de données
<<<<<<< HEAD
engine = create_engine("mysql+pymysql://root:root@localhost:3306/banque_final",pool_size=10, max_overflow=20)
=======
engine = create_engine("mysql+pymysql://root:password@localhost:3306/banque",pool_size=10, max_overflow=20)
>>>>>>> 7cd4e70a5984aab484d3b8feee37b5c8a6c2ae84
Base = declarative_base()

# Creation de la structure des tables dans sqlalchemy
# Class de la table taux temporaire
def load_taux() :
    class Taux(Base) :
        __tablename__ = "taux"
        id = Column(Integer, primary_key = True)
        monnaie = Column(String(3))
<<<<<<< HEAD
        rate = Column(String)
        date = Column(String)
        colonne_prop = column_property(monnaie + " " + rate + " " + date )
     
    # Chargement de la table taux temporaire
    df_taux = pd.read_csv("C:/Users/Ahmed/Documents/csv/taux_de_change_transformed.csv")
=======
        rate = Column(Float)
        date = Column(Date)   
    
    # Chargement de la table taux
    df_taux = pd.read_csv("C:/Users/utilisateur/Documents/csv/taux_de_change.csv")
>>>>>>> 7cd4e70a5984aab484d3b8feee37b5c8a6c2ae84
    ligne_monnaie = list(df_taux['monnaie'])
    ligne_rate = list(df_taux['rates'])
    ligne_date = list(df_taux['date'])
    
    session = Session(engine)
    ligne_table_taux = [Taux(monnaie = ligne_monnaie[i] , rate=ligne_rate[i], date = ligne_date[i],) for i in range(len(ligne_monnaie))]
    session.bulk_save_objects(ligne_table_taux)
    session.commit()
    session.close()
    
    #definition de la table taux final :
    class Taux_final(Base) :
        __tablename__ = "taux_final"
        id = Column(Integer, primary_key = True)
        monnaie = Column(String(3))
        rate = Column(String)
        date = Column(String)  
        colonne_prop = column_property(monnaie + " " + rate + " " + date )

    #load_taux_bis()
    #load_taux_bis_final()
    # Insertion des lignes de la table taux temporaire qui changent dans la table taux final
    session = Session(engine)        
    clean_taux=session.query(Taux_final.colonne_prop)
    changes_to_insert = session.query(Taux.monnaie, Taux.rate, Taux.date).filter(~Taux.colonne_prop.in_(clean_taux)) 
    stm = insert(Taux_final).from_select (['monnaie','rate', 'date'], changes_to_insert)
    session.execute(stm)
    session.commit()
    session.close()
    
    # suppression des ligne de la table taux final que n'existent pas dans la table taux temporaire
    session = Session(engine)
    Taux_ids = session.query(Taux.id)
    session.query(Taux_final).filter(~Taux_final.id.in_(Taux_ids)).delete(synchronize_session = False)
    
    session.commit()
    session.close() 
    
    # Ici on vide la table taux temporaire
    session = Session(engine)
    session.query(Taux).delete(synchronize_session = False)
    session.commit()
    session.close() 



# Class de la table banque temporaire  de la banque (classement des banques)
def load_banque() :
    # Class de la table classement de la banque
    class Banque(Base) :
        __tablename__ = "banque"
        id = Column(Integer, primary_key = True)
        rang = Column(String)
        nom = Column(String)
        market_cap = Column(String)
        colonne_prop = column_property(rang + " " + nom + " " + market_cap )
        
    
<<<<<<< HEAD
    # Chargement de la table banque temporaire
    df_classement = pd.read_csv("C:/Users/Ahmed/Documents/csv/classement_banque_transformed.csv")
=======
    # Chargement de la table classement banque
    df_classement = pd.read_csv("C:/Users/utilisateur/Documents/csv/classement_banque.csv")
>>>>>>> 7cd4e70a5984aab484d3b8feee37b5c8a6c2ae84
    ligne_rang = list(df_classement['Rank'])
    ligne_nom = list(df_classement['Bank name'])
    ligne_market_cap = list(df_classement['market_cap_€'])
    
    session = Session(engine)
    ligne_table_classement = [Banque(rang = ligne_rang[i] ,nom=ligne_nom[i],market_cap = ligne_market_cap[i],) for i in range(len(ligne_rang))]
    session.bulk_save_objects(ligne_table_classement)
    session.commit()
    session.close()
    
    # Class de la table banque final  de la banque (classement des banques)
    class Banque_final(Base) :
        __tablename__ = "banque_final"
        id = Column(Integer, primary_key = True)
        rang = Column(String)
        nom = Column(String)
        market_cap = Column(String)
        colonne_prop = column_property(rang + " " + nom + " " + market_cap )
        
    # Insertion des lignes modifiées dans la table banque final   
    session = Session(engine)        
    clean_taux=session.query(Banque_final.colonne_prop)
    changes_to_insert = session.query(Banque.rang, Banque.nom, Banque.market_cap).filter(~Banque.colonne_prop.in_(clean_taux)) 
    stm = insert(Banque_final).from_select (['rang','nom', 'market_cap'], changes_to_insert)
    session.execute(stm)
    session.commit()
    session.close()
    
    # Suppression des lignes de la tables banque final qui n'existe pas dans la table banque temporaire
    session = Session(engine)
    Banque_ids = session.query(Banque.id)
    session.query(Banque_final).filter(~Banque_final.id.in_(Banque_ids)).delete(synchronize_session = False)
    
    session.commit()
    session.close()
    
    # Ici on vide la table banque temporaire
    session = Session(engine)
    session.query(Banque).delete(synchronize_session = False)
    session.commit()
    session.close() 


# definition de la classe table Foncieres temporaire
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
        colonne_prop = column_property(date_mutation + " " + nature_mutation + " " + valeur_fonciere + " " + code_postal + " " + commune + " " + type_local + " " + surface_reelle_batie + " " + nombre_pieces_principales + " " + surface_terrain)
        
<<<<<<< HEAD
    # import des données de la table foncieres
    df_foncier = pd.read_csv("C:/Users/Ahmed/Documents/csv/foncier_transformed.csv")
    df_foncier = df_foncier.head(10)
=======
    # Chargement de la table foncieres
    df_foncier = pd.read_csv("C:/Users/utilisateur/Documents/csv/foncier_transformed.csv")
>>>>>>> 7cd4e70a5984aab484d3b8feee37b5c8a6c2ae84
    
    
    # traitement des données manquantes
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
    
    # Remplissage de la table Foncieres temporaire
    session = Session(engine)
    ligne_table_foncieres = [Foncieres(date_mutation = ligne_date_mutation[i], nature_mutation=ligne_nature_mutation[i], \
                            valeur_fonciere = ligne_valeur_fonciere[i], code_postal=ligne_code_postal[i], commune=ligne_commune[i],\
                            type_local = ligne_type_local[i], surface_reelle_batie = ligne_surface_reelle_batie[i], \
                            nombre_pieces_principales = ligne_nombre_pieces[i], surface_terrain = ligne_surface_terrain[i],) \
                            for i in range(len(ligne_nature_mutation))]
    session.bulk_save_objects(ligne_table_foncieres)
    session.commit()
    session.close()
    
    # Class de la table des valeurs foncieres_final 
    class Foncieres_final(Base) :
        __tablename__ = "foncieres_final"
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
        colonne_prop = column_property(date_mutation + " " + nature_mutation + " " + valeur_fonciere + " " + code_postal + " " + commune + " " + type_local + " " + surface_reelle_batie + " " + nombre_pieces_principales + " " + surface_terrain)
    
    
    # Insertion des nouvelles lignes dans la table Foncieres final
    session = Session(engine)        
    clean_taux=session.query(Foncieres_final.colonne_prop)
    changes_to_insert = session.query(Foncieres.date_mutation, Foncieres.nature_mutation, Foncieres.valeur_fonciere, Foncieres.code_postal, 
                                      Foncieres.commune, Foncieres.type_local, Foncieres.surface_reelle_batie, Foncieres.nombre_pieces_principales, 
                                      Foncieres.surface_terrain).filter(~Foncieres.colonne_prop.in_(clean_taux)) 
    stm = insert(Foncieres_final).from_select (['date_mutation','nature_mutation', 'valeur_fonciere', 'code_postal', 'commune', 'type_local', 'surface_reelle_batie', 'nombre_pieces_principales','surface_terrain'], changes_to_insert)
    session.execute(stm)
    session.commit()
    session.close()
    
    
    # Suppression des lignes de la table Foncieres final qui sont modifiées
    session = Session(engine)
    Foncieres_ids = session.query(Foncieres.id)
    session.query(Foncieres_final).filter(~Foncieres_final.id.in_(Foncieres_ids)).delete(synchronize_session = False)
    
    session.commit()
    session.close()
    
    # Ici on vide la table Foncieres temporaire
    session = Session(engine)
    session.query(Foncieres).delete(synchronize_session = False)
    session.commit()
    session.close() 
    


load_taux()

load_banque()

load_foncieres()


    

