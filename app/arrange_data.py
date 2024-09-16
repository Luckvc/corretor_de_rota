import polars as pl

def open_ceps_csv(first_cep_number):
  with open("app/ceps_db/ceps_" + first_cep_number + ".csv", 'r', encoding='utf-8') as file:
    return pl.read_csv(file, encoding='utf-8', separator=';')

def get_street_name(cep):
  internal_storage = open_ceps_csv(cep[0])

  try: street_name = internal_storage.row(by_predicate=(pl.col("cep") == cep))[1]
  except pl.exceptions.NoRowsReturnedError: street_name = ''

  if street_name is None: street_name = ''

  return street_name

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
   

def process_data(file_path):
  df = pl.read_excel(file_path)

  df = df.drop(['Latitude', 'Longitude', 'AT ID', 'Stop', 'SPX TN'])

  df = df.with_columns(
    pl.col("Destination Address")
    .str.split_exact(",", 3)
    .struct.rename_fields(['Address line 1', 'Address line 2', 'Complemento'])
    .alias("Destination Address")
  ).unnest("Destination Address")

  df = df.with_columns(
    pl.col('Zipcode/Postal code').map_elements(function=get_street_name, return_dtype=str).alias('database_streets')
  )

  df = df.with_columns(
      pl.when(pl.col('database_streets') == '')
      .then(pl.col('Address line 1').map_elements(correct_street_name, return_dtype=str))
      .otherwise(pl.col('database_streets'))
      .alias('Address line 1')
  )

  grouped_df = df.group_by(['Address line 1', 'Address line 2']).agg([pl.first('Complemento'), pl.first('Bairro'),
                                                                      pl.first('City'), pl.first('Zipcode/Postal code'),
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
