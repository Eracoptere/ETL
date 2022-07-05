#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
dfs = pd.read_html(url)
df = dfs[3]
df.to_csv('donnees_banque.csv')