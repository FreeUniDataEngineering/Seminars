# pip install --upgrade google-cloud-bigquery
# https://googleapis.dev/python/bigquery/latest/reference.html

from google.cloud import bigquery
from google.cloud.bigquery import ExternalSourceFormat
from google.cloud.bigquery.external_config import HivePartitioningOptions
from google.cloud.exceptions import NotFound

# Construct a BigQuery client object.
client = bigquery.Client()

print(f'Default Project: \n {client.project} \n ------------------------ \n')

dataset_location = "europe-west6"


def create_dataset(dataset_id):
    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project, dataset_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = dataset_location

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

# create_dataset('dataset_name')


def list_datasets():
    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project

    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:
            print(f'\t{dataset.dataset_id}')
    else:
        print("{} project does not contain any datasets.".format(project))


# list_datasets()


def describe_dataset(dataset_id):
    dataset = client.get_dataset(dataset_id)  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    friendly_name = dataset.friendly_name
    print(
        "Got dataset '{}' with friendly_name '{}'.".format(
            full_dataset_id, friendly_name
        )
    )

    # View dataset properties.
    print("Description: {}".format(dataset.description))
    print("Labels:")
    labels = dataset.labels

    if labels:
        for label, value in labels.items():
            print("\t{}: {}".format(label, value))
    else:
        print("\tDataset has no labels defined.")

    # View tables in dataset.
    print("Tables:")
    tables = list(client.list_tables(dataset))  # Make an API request(s).
    if tables:
        for table in tables:
            print("\t{}".format(table.table_id))
    else:
        print("\tThis dataset does not contain any tables.")

# describe_dataset('dataset_name')


def update_dataset_description(dataset_id, new_description):
    dataset = client.get_dataset(dataset_id)  # Make an API request.
    dataset.description = new_description
    dataset = client.update_dataset(dataset, ["description"])  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print(
        "Updated dataset '{}' with description '{}'.".format(
            full_dataset_id, dataset.description
        )
    )

# update_dataset_description('dataset_name', 'description goes here')


def dataset_exists(dataset_id):
    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))

# dataset_exists('found')


def delete_dataset(dataset_id, cascade=False):
    # Use the delete_contents parameter to delete a dataset and its contents.
    # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
    client.delete_dataset(
        dataset_id, delete_contents=cascade, not_found_ok=True
    )  # Make an API request.

    print("Deleted dataset '{}'.".format(dataset_id))


def create_native_table(dataset_id, table_id, schema=None):
    full_table_id = '.'.join([client.project, dataset_id, table_id])
    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, dataset_id, table.table_id)
    )


# create_native_table('dataset_name',
#                     'test_table',
#                     [bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
#                      bigquery.SchemaField("age", "INTEGER", mode="REQUIRED")])

# create_native_table('dataset_name',
#                     'test_table_complex',
#                     # struct
#                     [bigquery.SchemaField("id", "integer", mode="REQUIRED"),
#                      bigquery.SchemaField(
#                          "struct",
#                          "RECORD",
#                          mode="NULLABLE",
#                          fields=[
#                              bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
#                              bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
#                              bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
#                              bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
#                              bigquery.SchemaField("zip", "STRING", mode="NULLABLE"),
#                              bigquery.SchemaField("numberOfYears", "STRING", mode="NULLABLE"),
#                          ]),
#                      #array
#                      bigquery.SchemaField(
#                          "array",
#                          "string",
#                          mode="REPEATED")
#                      ])

# create_native_table('dataset_name',
#                     'test_table_empty')


def create_native_table_from_query(dataset_id, table_id, query):
    table_id = '.'.join([client.project, dataset_id, table_id])

    job_config = bigquery.QueryJobConfig(destination=table_id)

    # Start the query, passing in the extra configuration.
    query_job = client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))


# create_native_table_from_query('dataset_name',
#                                'from_query',
#                                """
#                                    SELECT 1 as id
#                                """)

def create_external_table(dataset_id,
                          table_id,
                          schema=None,
                          source_paths=[],
                          source_format=ExternalSourceFormat.PARQUET):
    full_table_id = '.'.join([client.project, dataset_id, table_id])

    external_config = bigquery.ExternalConfig(source_format=source_format)
    external_config.source_uris = source_paths
    external_config.schema = schema

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    client.create_table(table)


schema = [bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
          bigquery.SchemaField("first_name", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("last_name", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("birth_date", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("country", "STRING", mode="REQUIRED")]

# create_external_table('dataset_name',
#                       'users',
#                       schema,
#                       ['gs://yet_another_bucket/ml-latest-small/users.csv'],
#                       ExternalSourceFormat.CSV)


# create_external_table('uni',
#                       'twitter',
#                       source_paths=['gs://yet_another_bucket/twitter.avro'],
#                       source_format=ExternalSourceFormat.AVRO)

# create_external_table('dataset_name',
#                       'users_multiple_files',
#                       schema,
#                       ['gs://yet_another_bucket/ml-latest-small/users.csv',
#                       'gs://yet_another_bucket/ml-latest-small/users2.csv'],
#                       ExternalSourceFormat.CSV)

def drop_table(dataset_id, table_id):
    full_table_id = '.'.join([client.project, dataset_id, table_id])

    client.delete_table(full_table_id)

    print('Deleted table {}'.format(full_table_id))


def query(sql):
    data = client.query(sql)
    print(list(data.result()))


def show_create_table(dataset_id, table_id):
    """ Show create table DDL """
    full_dataset_id = '.'.join([client.project, dataset_id])

    result = client.query(f'''select
                                table_name, ddl
                             from
                                {full_dataset_id}.INFORMATION_SCHEMA.TABLES
                             where table_name = "{table_id}"''')

    for row in result:
        # Row values can be accessed by field name or index.
        print("table_name={}, ddl={}".format(row[0], row["ddl"]))


# show_create_table('dtst', 'test')


''' Update records '''
# sql = """ update `complete-verve-325920.dtst.test`
#           set age = 26
#           WHERE TRUE
#       """
#
# client.query(sql).result()


''' TIME TRAVEL '''
# SELECT *
# FROM `complete-verve-325920.dtst.test`
#   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE);


def create_hive_external_table_from_query(dataset_id,
                                          table_id,
                                          source_uri_prefix,
                                          source_paths=[],
                                          source_format=ExternalSourceFormat.PARQUET,
                                          schema=schema):

    full_table_id = '.'.join([client.project, dataset_id, table_id])

    external_config = bigquery.ExternalConfig(source_format=source_format)
    external_config.source_uris = source_paths

    if source_format not in [ExternalSourceFormat.PARQUET, ExternalSourceFormat.ORC, ExternalSourceFormat.AVRO]:
        external_config.schema = schema

    hive_config = HivePartitioningOptions()
    hive_config.mode = 'CUSTOM'
    hive_config.source_uri_prefix = source_uri_prefix
    # hive_config.fields = fields

    external_config.hive_partitioning = hive_config

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    client.create_table(table)

    print(f'Created external table {full_table_id}')


# create_hive_external_table_from_query('dtst',
#                                       'users_hive',
#                                       source_uri_prefix='gs://yet_another_bucket/ml-latest-small/users_data/{country:STRING}',
#                                       source_paths=['gs://yet_another_bucket/ml-latest-small/users_data/*'],
#                                       source_format=ExternalSourceFormat.PARQUET)
