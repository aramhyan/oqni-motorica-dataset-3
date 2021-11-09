import apache_beam as beam
import numpy as np
from sklearn.preprocessing import normalize
file_dir = './data/digits_opto_preprocessed.txt'

# import pandas as pd
# df = pd.read_csv('./data/digits_opto_preprocessed.csv')
# df.to_csv(file_dir, header=None, index=None, sep='\t')

'''
question 1: how do we access all the data, rather than each element
question 2: is there a way to read the data as an array, rather than a string
'''
class ProcessData(beam.DoFn):
  def process(self, element, *args, **kwargs):
    element = element.split('.0\t')[1:-1] 
    # element = [int(i) for i in element]
    element = np.array([int(i) for i in element]).reshape(-1,1)
    element = normalize(element, axis=0)
    print(element)

    return [element]


with beam.Pipeline() as pipeline:
    (
      pipeline
      | 'Read files' >> beam.io.ReadFromText(file_dir)
      # | 'Print contents 1' >> beam.FlatMap(lambda element:print(element, '\n'))
      | 'Scale Data' >> beam.ParDo(ProcessData())
      # | 'Print contents' >> beam.FlatMap(lambda element:print(element, '\n'))
    )