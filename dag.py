import airflow
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime


project     = 'project_iron'
dsIngestion = 'bv_ingestion'
dsDW        = 'bv_dw'

default_args = {
    'owner': 'bv',
    'start_date': datetime(2020,9,16),
    'depends_on_past': False
}

dag = DAG(
    dag_id='bv_dag',
    default_args=default_args,
    schedule_interval=("0 9 * * *")
)

# Definição dos Schema 

"""
 Para o caso da tabela exitir no BQ poderia definr uma Função para definir o Schema
 
 table_ref = dataset_ref.table(table_id)
 table = client.get_table(table_ref)  

  lista = list()
  for schema in table.schema:
      if schema.name != 'METADATA':
         lista.append({
             'campo':schema.name,
             'tipo':schema.field_type
         })

    return lista
"""
task_1 = GoogleCloudStorageToBigQueryOperator(
    task_id='load_price_quote_to_bigquery',
    bucket='bv-data',
    source_objects=['data/price_quote.csv'],
    destination_project_dataset_table='{}.{}.price_quotes'.format(project,dsIngestion),
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    schema_fields=[
    {
        "name": "tube_assembly_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "supplier",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quote_date",
        "type": "DATE",
        "mode": "NULLABLE"
    },
    {
        "name": "annual_usage",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "min_order_quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "bracket_pricing",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "cost",
        "type": "FLOAT",
        "mode": "NULLABLE"
    }],
    dag=dag)

task_2 = GoogleCloudStorageToBigQueryOperator(
    task_id='load_comp_boss_to_bigquery',
    bucket='bv-data',
    source_objects=['data/comp_boss.csv'],
    destination_project_dataset_table='{}.{}.comp_boss'.format(project,dsIngestion),
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    schema_fields=[
    {
        "name": "component_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_type_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "connection_type_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "outside_shape",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "base_type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "height_over_tube",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "bolt_pattern_long",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "bolt_pattern_wide",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "groove",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "base_diameter",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "shoulder_diameter",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "unique_feature",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },
    {
        "name": "orientation",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },
    {
        "name": "weight",
        "type": "STRING",
        "mode": "NULLABLE"
    }],
    dag=dag)

task_3 = GoogleCloudStorageToBigQueryOperator(
    task_id='load_bill_of_materials_to_bigquery',
    bucket='bv-data',
    source_objects=['data/bill_of_materials.csv'],
    destination_project_dataset_table='{}.{}.bill_of_materials'.format(project,dsIngestion),
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    schema_fields=[
    {
        "name": "tube_assembly_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_1",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_1",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_2",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_2",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_3",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_3",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_4",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_4",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_5",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_5",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_6",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_6",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_7",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_7",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "component_id_8",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "quantity_8",
        "type": "STRING",
        "mode": "NULLABLE"
    }],
    dag=dag)

task_4 = BigQueryOperator(
    task_id='dim_tube_assembly',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_tube_assembly,
                        tube_assembly_id
        FROM `{project}.{dsIngestion}.price_quotes`
        GROUP BY tube_assembly_id;
        """,
    destination_dataset_table='{}.{}.dim_tube_assembly'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_5 = BigQueryOperator(
    task_id='dim_supplier',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_supplier,
                        supplier
        FROM `{project}.{dsIngestion}.price_quotes`
        GROUP BY
            supplier;
        """,
    destination_dataset_table='{}.{}.dim_supplier'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_6 = BigQueryOperator(
    task_id='fat_quotes',
    sql="""
        SELECT T2.sk_supplier,
               T3.sk_tube_assembly, 
               CAST(T1.quote_date AS DATE) AS quote_date,
               T1.annual_usage, 
               T1.min_order_quantity, 
               T1.bracket_pricing, 
               T1.quantity, 
               T1.cost 
        FROM `{project}.{dsIngestion}.price_quotes`     AS T1
        INNER JOIN `{project}.{dsDW}.dim_supplier`      AS T2 ON T1.supplier = T2.supplier
        INNER JOIN `{project}.{dsDW}.dim_tube_assembly` AS T3 ON T1.tube_assembly_id = T3.tube_assembly_id;
        """,
    
    destination_dataset_table='{}.{}.fat_quotes'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_7 = BigQueryOperator(
    task_id='dim_outside_shape',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_outside_shape,
                        outside_shape
        FROM `{project}.{dsIngestion}.comp_boss`
        GROUP BY outside_shape;
        """,

    destination_dataset_table='{}.{}.dim_outside_shape'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_8 = BigQueryOperator(
    task_id='dim_type',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_type,
                        type
        FROM `{project}.{dsIngestion}.comp_boss`
        GROUP BY type;
        """,
    destination_dataset_table='{}.{}.dim_type'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_9 = BigQueryOperator(
    task_id='dim_component_type',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_component_type,
                        component_type_id
        FROM `{project}.{dsIngestion}.comp_boss`
        GROUP BY component_type_id;
        """,
    destination_dataset_table='{}.{}.dim_component_type'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_10 = BigQueryOperator(
    task_id='dim_base_type',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_base_type,
                        base_type 
        FROM `{project}.{dsIngestion}.comp_boss`
        GROUP BY base_type;
        """,
    destination_dataset_table='{}.{}.dim_base_type'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_11 = BigQueryOperator(
    task_id='dim_connection_type',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_connection_type,
                        connection_type_id
        FROM `{project}.{dsIngestion}.comp_boss`
        GROUP BY connection_type_id;
        """,
    destination_dataset_table='{}.{}.dim_connection_type'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_12 = BigQueryOperator(
    task_id='dim_component',
    sql="""
        SELECT DISTINCT ROW_NUMBER() OVER() AS sk_component,
                        component_id
        FROM `{project}.{dsIngestion}.comp_boss`
        GROUP BY component_id;
        """,
    destination_dataset_table='{}.{}.dim_component'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

task_13 = BigQueryOperator(
    task_id='fat_component',
    sql="""
        SELECT T2.sk_component, 
               T3.sk_component_type, 
               T4.sk_type, 
               T5.sk_connection_type, 
               T6.sk_outside_shape, 
               T7.sk_base_type,
               T1.height_over_tube, 
               T1.bolt_pattern_long,
               T1.bolt_pattern_wide,
               T1.groove, 
               T1.base_diameter, 
               T1.shoulder_diameter, 
               T1.unique_feature, 
               T1.orientation, 
               T1.weight 
        FROM `project_iron.bv_ingestion.comp_boss`          AS T1
        INNER JOIN `project_iron.bv_dw.dim_component`       AS T2 ON T1.component_id = T2.component_id 
        INNER JOIN `project_iron.bv_dw.dim_component_type`  AS T3 ON T1.component_type_id = T3.component_type_id
        INNER JOIN `project_iron.bv_dw.dim_type`            AS T4 ON T1.type = T4.type
        INNER JOIN `project_iron.bv_dw.dim_connection_type` AS T5 ON T1.connection_type_id = T5.connection_type_id 
        INNER JOIN `project_iron.bv_dw.dim_outside_shape`   AS T6 ON T1.outside_shape = T6.outside_shape 
        INNER JOIN `project_iron.bv_dw.dim_base_type`       AS T7 ON T1.base_type = T7.base_type;
        """,
    destination_dataset_table='{}.{}.fat_component'.format(project,dsDW),
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag)

[task_1, task_2] >> task_3 >> [task_4, task_5] >> task_6 >> [task_7, task_8, task_9, task_10, task_11, task_12] >> task_13