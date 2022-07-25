# ETL pour une banque 
***
Projet d'ETL pour une banque dans le but d'utiliser les données pour mettre au point un modèle d'estimation du prix de vente d'un bien immobilier via du Machine Learning

# Sommaire :
1. [Introduction](#introduction)
2. [Fichiers par fichiers](#fichiers_par_fichiers)
3. [Automatisation](#automatisation)


<a name="introduction"><h3>Introduction: </h3></a>

Lancer fichier creation_bd_projet_ETL.sql pour créer la base de données “banque_final” <br>
Il faudra exécuter les fichiers un par uns pour modifier les différents paramètres qui ont besoin d’être ajusté et ensuite exécuter le fichier principal 

<a name="fichiers_par_fichiers"><h3>Fichiers par fichiers: </h3></a>

1. [Utilisateurs Windows](#windows)
2. [Utilisateurs Mac](#mac)

<a name="windows"><h4>Utilisateurs Windows: </h4></a>
- Lancer fichier <i>extract.py</i> <br>
  - On extrait des données de 3 sources différentes et on les exporte en csv<br><br>
- Lancer fichier <i>transform.py</i><br>
  - On transforme les csv pour garder seulement les informations qui nous intéressent et pour homogénéiser les données<br><br>
- Modifier le fichier <i>load.py</i> : remplacer password par votre mot de passe mysql<br>
Lancer <i>load.py</i><br>
  - On insère les données dans la bdd banques

<a name="mac"><h4>Utilisateurs Mac: </h4></a>
- Modifier fichier <i>extract.py</i> : Remplacer la variable local_path_csv avec le chemin relatif au dossier ETL_main et en remplaçant par votre nom d’utilisateur <br>
Lancer fichier <i>extract.py</i><br>
  - On extrait des données de 3 sources différentes et on les exporte en csv<br><br>
- Modifier fichier <i>transform.py</i> : Modifier encore la variable local_path_csv avec le chemin relative au dossier ETL_main et en remplaçant par votre nom d’utilisateur<br>
Lancer fichier <i>transform.py</i><br>
  - On transforme les csv pour garder seulement les informations qui nous intéressent et pour homogénéiser les données<br><br>
- Modifier le fichier <i>load.py</i> : <br><br>
Remplacer password par votre mot de passe mysql
Remplacer les différentes variables df_taux, df_classement et df_foncier par le chemin qui mène au fichier respectif créée et transformé avec extract et transform<br>
Lancer <i>load.py</i><br>
  - On insère les données dans la bdd banque_finale<br>

<a name="automatisation"><h3>Automatisation: </h3></a>
Pour automatiser l’ETL et qu’il s’exécute automatiquement tout les jours à 12h :<br>
Lancer fichier main_ETL (pour les utilisateurs windows, bien exécuter une fois tout les autres fichiers séparément pour être sur qu'ils fonctionnent) et laisser le fichier tourner<br><br>
Version des différents packages utilisés : <br>
bs4 : 4.11.1<br>
requests : 2.27.1<br>
pandas : 1.4.2<br>
re : 2.2.1<br>
sqlalchemy : 1.4.32<br>
<br><br><br>
Fait par Allan, Mohamed, Randolf et Eric
