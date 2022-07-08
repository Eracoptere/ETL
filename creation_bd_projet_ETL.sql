drop database if exists banque;
create database banque;

use banque;

create table taux(id integer primary key, 
	monnaie varchar(3), 
	rate float, date date);

/*
create table foncieres(id integer primary key, 
	date_mutation date, 
	nature_mutation varchar(50), 
	valeur_fonciere float, 
	code_postal varchar(50), 
	commune varchar(50), 
	type_local varchar(50),
	surface_reelle_batie float, 
	nombre_pieces_principales float, 
	surface_terrain float);
*/



create table foncieres(id integer primary key, 
	date_mutation varchar(50), 
	nature_mutation varchar(50), 
	valeur_fonciere varchar(50), 
	code_postal varchar(50), 
	commune varchar(50), 
	type_local varchar(50),
	surface_reelle_batie varchar(50), 
	nombre_pieces_principales varchar(50), 
	surface_terrain varchar(50));

create table banque(id integer primary key,
	rang integer,
    nom varchar(50),
    market_cap float);
