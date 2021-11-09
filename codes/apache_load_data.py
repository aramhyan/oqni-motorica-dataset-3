import apache_beam as beam
file_dir = '../data/digits_opto_preprocessed.csv'


with beam.Pipeline() as pipeline:
    (
      pipeline
      | 'Read files' >> beam.io.ReadFromText(file_dir)
      | 'Print contents' >> beam.Map(print)
    )