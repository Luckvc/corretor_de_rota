import polars as pl
import glob
from thefuzz import fuzz
from thefuzz import process
import unidecode
import ruas_crawler

def process_data(file_path):
  df = pl.read_excel(file_path)

  df = df.drop(['Latitude', 'Longitude', 'AT ID', 'Stop'])

  df = df.with_columns(
    pl.col("Destination Address")
    .str.split_exact(",", 3)
    .struct.rename_fields(['Address line 1', 'Address line 2', 'Complemento'])
    .alias("Destination Address")
  ).unnest("Destination Address")

  df = df.with_columns(
    pl.struct('Address line 1','Zipcode/Postal code').map_elements(function=get_street_name, return_dtype=str).alias('database_streets')
  )

  df = df.with_columns(
      pl.when(pl.col('database_streets') == '')
      .then(pl.struct('Address line 1', 'City').map_elements(fuzzy_find_street_name, return_dtype=str))
      .otherwise(pl.col('database_streets'))
      .alias('database_streets')
  )

  df = df.with_columns(
      pl.when(pl.col('database_streets') == '')
      .then(pl.col('Address line 1').map_elements(correct_street_name, return_dtype=str))
      .otherwise(pl.col('database_streets'))
      .alias('Address line 1')
  )


  grouped_df = df.group_by(['Address line 1', 'Address line 2'], maintain_order=True).agg([pl.first('Complemento'), 
                                                                                           pl.first('Bairro'),
                                                                                           pl.first('City'), 
                                                                                           pl.first('Zipcode/Postal code'),
                                                                                           pl.col('Sequence')])
  grouped_df = grouped_df.with_columns(
    pl.col('Sequence').map_elements(aggregate_sequences, str).alias('Sequence')
  )
  grouped_df = grouped_df.with_columns(
    pl.col('Sequence').map_elements(package_count, int).alias('Qtd. Pacotes')
  )

  grouped_df = grouped_df.rename({"Sequence": "N° dos Pacotes", "Bairro": "Neighborhood"})

  cols = ['Qtd. Pacotes', 'N° dos Pacotes', 'Address line 1', 'Address line 2', 'Complemento',
          'Neighborhood', 'City', 'Zipcode/Postal code']
  grouped_df = grouped_df[cols]
  return grouped_df


def open_ceps_csv(first_cep_number):
  with open("app/ceps_db/ceps_" + first_cep_number + ".csv", 'r', encoding='utf-8') as file:
    return pl.read_csv(file, encoding='utf-8', separator=';')

def get_street_name(address):
  internal_storage = open_ceps_csv(address['Zipcode/Postal code'][0])

  try: street_name = internal_storage.row(by_predicate=(pl.col("cep") == address['Zipcode/Postal code']))[1]
  except pl.exceptions.NoRowsReturnedError: street_name = None

  if street_name is None or street_name == '':  return ''

  return validate_street_name(address['Address line 1'], street_name)

def validate_street_name(original_street_name, cep_street_name):
  score = fuzz.ratio(original_street_name, cep_street_name)

  if score > 75:
    return cep_street_name

  return ''


def fuzzy_find_street_name(address):
  city = unidecode.unidecode(address['City'].replace(' ', '-').lower())
  if 'ceps_db/cidades_cep_unico/' + city +'.csv' not in glob.glob('ceps_db/cidades_cep_unico/*.csv'):
    try:
      ruas_crawler.get_city_data(city)
    except:
      pass

  try:
    with open('ceps_db/cidades_cep_unico/' + city +'.csv') as file:
      cep_unico = pl.read_csv(file, has_header=False)
  except:
    return ""

  street = remove_street_preefix(address['Address line 1'])

  sorted = process.extractOne(street, cep_unico['column_1'], scorer=fuzz.partial_token_sort_ratio, score_cutoff=76)
  ratio = process.extractOne(street, cep_unico['column_1'], scorer=fuzz.partial_ratio, score_cutoff=76)

  if sorted == None and ratio == None: return ""
  if sorted == None: return ratio[0]
  if ratio == None: return sorted[0]

  if ratio[1] > sorted[1]:
    return ratio[0]
  else:
    return sorted[0]

def remove_name_abbreviations(street_name):
    words = street_name.split()
    filtered_words = [word for word in words if len(word) > 1]

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

def aggregate_sequences(sequences):
  return ', '.join(map(str, sequences))

def remove_street_preefix(street_name):
  filter_out = ['Rua', 'rua', 'Avenida', 'avenida']

  for n in filter_out:
    if street_name.startswith(n + ' '):
      return street_name[len(n):]

  return street_name.strip()

