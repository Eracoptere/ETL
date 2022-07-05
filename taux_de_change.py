import requests
import pandas as pd

import requests
# Extraction des données taux de change via une API
url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=ngVF0Ysuxr6dV03od7TpSm37lF3F30yC"
reponse = requests.get(url)

df_exchange_rates = pd.DataFrame(reponse.json())

# Suppression des colonnes non necessaire
exchange_rates = df_exchange_rates[['date','rates']]
exchange_rates['monnaie'] = list(exchange_rates.index)

taux = exchange_rates[['monnaie','rates','date']]

# Conversion du dataframe en csv
taux.to_csv("taux_de_change.csv", index = False)