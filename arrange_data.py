import pandas as pd
import json
import requests
from io import BytesIO
import code

def open_ceps_json():
  with open('ceps.json', 'r', encoding='utf-8') as file:
    return json.load(file)

def save_ceps_json(data):
  with open('ceps.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=4)

def get_street_name(cep):
  internal_storage = open_ceps_json()
  street_name = internal_storage.get(cep)
  if street_name is not None:
    return street_name

  street_name = requests.get('https://viacep.com.br/ws/' + cep + '/json/').json()['logradouro']
  if street_name != '':
    internal_storage[cep] = street_name
    save_ceps_json(internal_storage)

  return street_name

def remove_name_abbreviations(street_name):
    words = street_name.split()
    filtered_words = [word for word in words if len(word) > 1]

    # Join the filtered words back into a string
    filtered_street_name = ' '.join(filtered_words)

    return filtered_street_name

def correct_street_name(street_name):
  abbr_mapping = {
  'Av': 'Avenida',
  'R': 'Rua',
  'Al': 'Alameda'
  }

  for abbr, full_form in abbr_mapping.items():
    if street_name.startswith(abbr + ' '):
      return remove_name_abbreviations(full_form + street_name[len(abbr):])
  return remove_name_abbreviations(street_name)

def package_count(sequence):
   return len(sequence.split(','))
   

def process_data(file_path):
  df = pd.read_excel(file_path)

  df.drop(['Latitude', 'Longitude'], axis=1, inplace=True)
  df[['Address line 1', 'Address line 2', 'Complemento']] = df['Destination Address'].str.split(',', n=2, expand=True)
  df.drop(['Destination Address', 'AT ID', 'Stop', 'SPX TN'], axis=1, inplace=True)
    
  viacep_streets = df['Zipcode/Postal code'].apply(get_street_name)

  df['Address line 1'] = viacep_streets.mask(viacep_streets == '', df['Address line 1'].apply(correct_street_name))

  def aggregate_sequences(sequences):
    return ', '.join(map(str, sequences))

  grouped_df = df.groupby(['Address line 1', 'Address line 2'], sort=False).agg({'Complemento': 'first', 'Bairro': 'first', 'City': 'first', 'Zipcode/Postal code': 'first', 'Sequence': aggregate_sequences}).reset_index()

  grouped_df['Qtd. Pacotes'] = grouped_df['Sequence'].apply(package_count)
  grouped_df.rename(columns={"Sequence": "N° dos Pacotes", "Bairro": "Neighborhood"}, inplace=True)

  cols = ['Qtd. Pacotes', 'N° dos Pacotes', 'Address line 1', 'Address line 2', 'Complemento',  'Neighborhood', 'City', 'Zipcode/Postal code']
  grouped_df = grouped_df[cols]

  return grouped_df
