import os
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

def get_adx_db_schema(database):
    """
    Retrieves schema (tables, columns, and column types) for target ADX database and returns as a JSON dictionary

    :param database: str, the name of the ADX database.

    """
    with connect_to_adx() as client:
        # Get all tables in the database
        tables = client.execute_mgmt(database=database, query=".show tables | project DatabaseName, TableName")

        table_details = []

        # For each table, get all columns and their types
        for table in tables.primary_results[0]:
            curr_table = {'table': f'{table["TableName"]}', 'columns': []}
            columns = client.execute_mgmt(database=database, query=f".show table {table["TableName"]} | project AttributeName, AttributeType")
            for column in columns.primary_results[0]:
                curr_table['columns'].append({'name': column["AttributeName"], 'type': column["AttributeType"]})
            table_details.append(curr_table)
        
    return table_details

def query_adx_db(database, query):
    """
    Executes a query against an ADX database and returns results as a pandas dataframe

    :param query: str, query to be executed.
    """
    with connect_to_adx() as client:
        result = client.execute_query(database=database, query=query)
        dataframe = dataframe_from_result_table(result.primary_results[0])
        return dataframe
    
def connect_to_adx():
    """
    Connects to the target ADX database and returns a connection object

    """
    TENANT_ID = os.getenv("TENANT_ID")
    KUSTO_CLUSTER = os.getenv("KUSTO_CLUSTER")
    DOMAIN_HINT = os.getenv("DOMAIN_HINT")
    #KUSTO_MANAGED_IDENTITY_APP_ID = os.getenv("KUSTO_MANAGED_IDENTITY_APP_ID")
    #KUSTO_MANAGED_IDENTITY_SECRET = os.getenv("KUSTO_MANAGED_IDENTITY_SECRET")

    cluster = KUSTO_CLUSTER
#   kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, KUSTO_MANAGED_IDENTITY_APP_ID, KUSTO_MANAGED_IDENTITY_SECRET,  TENANT_ID)
    kcsb = KustoConnectionStringBuilder.with_interactive_login(connection_string=cluster)
    kcsb.authority_id = TENANT_ID
    client = KustoClient(kcsb)

    return client