drop database if exists banque_bis;
create database banque_bis;

use banque_bis;

create table taux_bis(id integer primary key auto_increment, 
	monnaie varchar(3), 
	rate float, date date);
    
create table taux_bis_final(id integer primary key auto_increment, 
	monnaie varchar(3), 
	rate float, date date);
    
select * from taux_bis;
select * from taux_bis_final; 