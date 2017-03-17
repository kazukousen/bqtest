# -*- coding: utf-8 -*-

from __future__ import unicode_literals, print_function

import six
import os
import codecs
from dotenv import load_dotenv

import googleapiclient.discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
import pandas as pd

class BigQueryClient():
    def __init__(self):
        # 環境変数(.envファイル)のロード
        self._envset()
        credentials = GoogleCredentials.get_application_default()
        bigquery_service = googleapiclient.discovery.build('bigquery', 'v2', credentials=credentials)
        self.request = bigquery_service.jobs()

    def _envset(self):
        load_dotenv(os.path.join(os.curdir, '.env'))

    def run_query(self, query):
        '''クエリを実行し, 結果を返す'''
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
        '''標準出力で結果を表示'''
        print('Query Results:')
        for row in response['rows']:
            print('\t'.join(field['v'] for field in row['f']))

    def convert_res_to_df(self, response):
        '''結果をpandas.DataFrameで成形'''
        columns = [field['name'] for field in response['schema']['fields']]
        rows = [[field['v'] for field in row['f']] for row in response['rows']]
        df = pd.DataFrame(rows, columns=columns)
        return df

    def save_df_to_csv(self, df, csv_path='test.csv', index=False):
        '''pandas.DataFrame型のデータをCSVに出力'''
        with codecs.open(os.path.join(os.curdir, '{}'.format(csv_path)), 'w', encoding='sjis') as fd:
            fd.write(df.to_csv(index=index))
        return csv_path

    def example_query(self):
        '''例のクエリを返す'''
        query = '''
        select name, year, max(prcp) as max_prcp, avg(prcp) as avg_prcp from(
          select usaf, wban, name from [bigquery-public-data:noaa_gsod.stations] where name is not null and name<>""
        ) a join (
          select stn, wban, year, prcp from table_query([bigquery-public-data:noaa_gsod], 'table_id contains "gsod"')
        ) b on a.usaf=b.stn and a.wban=b.wban group by name, year
        '''[1:-1]
        return query

def main():
    bq = BigQueryClient()
    query = bq.example_query()
    response = bq.run_query(query)
    bq.output_response(response)

if __name__ == '__main__':
    main()
