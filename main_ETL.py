# -*- coding: utf-8 -*-
"""
Execute quotidiennement l'extraction, la transformation et le chargement des 
données dans une BDD MySQL nommé "banques"'
"""

import schedule
import time

def ETL():
    import runpy
    runpy.run_path(path_name='./Documents/ETL/extract.py')
    runpy.run_path(path_name='./Documents/ETL/transform.py')
    runpy.run_path(path_name='./Documents/ETL/load.py')
    
    
schedule.every.day.at("12:00").do(ETL)
  
while True:
    schedule.run_pending()
    time.sleep(1)