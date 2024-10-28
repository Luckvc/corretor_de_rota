import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_city_data(city_name):
  streets = []

  res = requests.get("https://ruas-brasil.openalfa.com/" + city_name)

  soup = BeautifulSoup(res.content, 'html.parser')
  for street in soup.find('div', 'street-columns').find_all('label'):
    streets.append(street.text)

  for i in range(2, 1000):
    res = requests.get("https://ruas-brasil.openalfa.com/" + city_name + "?pg=" + str(i))
    if (len(res.content) == 0): break

    soup = BeautifulSoup(res.content, 'html.parser')
    for street in soup.find('div', 'street-columns').find_all('label'):
      streets.append(street.text)

  df = pd.DataFrame(streets)

  df.to_csv('ceps_db/cidades_cep_unico/' + city_name + '.csv', header=False, index=False, encoding='utf8')