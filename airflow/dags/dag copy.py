from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime
import logging
import os


with DAG(dag_id="bigdata_tp_2",
         start_date=datetime(2023,5,5),
         schedule="@daily",
         description="Filter data with active ids and calculate TopCTR & TopProduct",
         catchup=False,
         tags=['Workflow_copy']) as dag:
    
    @task
    def FiltrarDatos():
        """

        Funcion para filtrar los datos del del dia con solo advertisers activos

        Parameters
        ---------------------------
        date: datetime.datetime()
            Fecha para filtrar los datos 
        
        """
        logging.info('Levantando ids activos...')
        ads_ids = pd.read_csv('/home/alanmatys/airflow/data/raw/advertiser_ids.csv', low_memory=False)

        # Filtrar Datos del dia
        ads_views = pd.read_csv('/home/alanmatys/airflow/data/raw/ads_views.csv',low_memory=False)

        ads_views_filter = ads_views[ads_views['advertiser_id'].isin(ads_ids['advertiser_id'])]

        # Filtrar Datos del dia
        prod_views = pd.read_csv('/home/alanmatys/airflow/data/raw/product_views.csv',low_memory=False)

        prod_views_filter = prod_views[prod_views['advertiser_id'].isin(ads_ids['advertiser_id'])]

        logging.info('Devolviendo DFs....')

        ads_views_filter.to_csv('/home/alanmatys/airflow/data/processed_data/ads_views_filter.csv',index=False)

        prod_views_filter.to_csv('/home/alanmatys/airflow/data/processed_data/prod_views_filter.csv',index=False)


    @task
    def TopCTR():
        """
        
        """

        df = pd.read_csv('/home/alanmatys/airflow/data/processed_data/ads_views_filter.csv',low_memory=False)

        # Group the rows by 'advertiser_id' and 'product_id', and calculate the total number of conversions and impressions for each product and advertiser
        grouped = df.groupby(['advertiser_id', 'product_id', 'type']).agg({'type': 'count'}).unstack('type', fill_value=0)
        grouped.columns = ['_'.join(col) for col in grouped.columns]
        grouped = grouped.reset_index()

        # Calculate the conversion rate for each product and advertiser
        grouped['conversion_rate'] = grouped['type_click'] / grouped['type_impression']

        # Sort the products by conversion rate in descending order for each advertiser_id and select the top 20 products
        top_20 = grouped.groupby('advertiser_id').apply(lambda x: x.sort_values(by='conversion_rate', ascending=False).head(20)).reset_index(drop=True).loc[:,['advertiser_id','product_id','conversion_rate']]


        top_20.to_csv('/home/alanmatys/airflow/data/results/topctr.csv',index=False)

    @task
    def TopProduct():
        """
        
        """

        df = pd.read_csv('/home/alanmatys/airflow/data/processed_data/prod_views_filter.csv',low_memory=False)

        grouped = df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='counts')

        top_20 = grouped.groupby('advertiser_id').apply(lambda x: x.sort_values(by='counts', ascending=False).head(20))


        top_20.to_csv('/home/alanmatys/airflow/data/results/topprod.csv',index=False)
    
    FiltrarDatos() >> [TopCTR(),TopProduct()]