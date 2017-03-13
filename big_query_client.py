# -*- coding: utf-8 -*-

from __future__ import unicode_literals, print_function

import six
import os
from dotenv import load_dotenv

import googleapiclient.discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
import pandas as pd

class BigQueryClient():
    def __init__(self):
        self._envset()
        credentials = GoogleCredentials.get_application_default()
        bigquery_service = googleapiclient.discovery.build('bigquery', 'v2', credentials=credentials)
        self.request = bigquery_service.jobs()

    def _envset(self):
        load_dotenv(os.path.join(os.curdir, '.env'))

    def run_query(self, query):
        try:
            response = self.request.query(
                projectId=os.environ.get('PROJECT_ID'),
                body={'query': query}
            ).execute()
        except HttpError as err:
            print('Error: {}'.format(err.content))
            raise err

        return response

    def output_response(self, response):
        print('Query Results:')
        for row in response['rows']:
            print('\t'.join(field['v'] for field in row['f']))

    def output_df(self, response):
        columns = [field['name'] for field in response['schema']['fields']]
        rows = [[field['v'] for field in row['f']] for row in response['rows']]
        df = pd.DataFrame(rows, columns=columns)
        return df

def main():
    bq = BigQueryClient()
    query = '''
        select top(corpus, 10) as title,
        count(*) as unique_words
        from [publicdata:samples.shakespeare];
    '''[1:-1]
    response = bq.run_query(query)
    bq.output_response(response)

if __name__ == '__main__':
    main()
