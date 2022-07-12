drop database if exists banque_final;
create database banque_final;

use banque_final;

create table taux(id integer primary key auto_increment, 
	monnaie varchar(3), 
	rate float, date date);

create table taux_final(id integer primary key auto_increment, 
	monnaie varchar(3), 
	rate float, date date);


create table foncieres(id integer primary key auto_increment, 
	date_mutation varchar(50), 
	nature_mutation varchar(50), 
	valeur_fonciere varchar(50), 
	code_postal varchar(50), 
	commune varchar(50), 
	type_local varchar(50),
	surface_reelle_batie varchar(50), 
	nombre_pieces_principales varchar(50), 
	surface_terrain varchar(50));
    
create table foncieres_final(id integer primary key auto_increment, 
	date_mutation varchar(50), 
	nature_mutation varchar(50), 
	valeur_fonciere varchar(50), 
	code_postal varchar(50), 
	commune varchar(50), 
	type_local varchar(50),
	surface_reelle_batie varchar(50), 
	nombre_pieces_principales varchar(50), 
	surface_terrain varchar(50));


create table banque(id integer primary key auto_increment,
	rang integer,
    nom varchar(50),
    market_cap float);
    
create table banque_final(id integer primary key auto_increment,
	rang integer,
    nom varchar(50),
    market_cap float);