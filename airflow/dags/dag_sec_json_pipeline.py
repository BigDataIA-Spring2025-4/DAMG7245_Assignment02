import io
from airflow.utils.task_group import TaskGroup
from io import BytesIO
import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from datetime import date, datetime
from marshmallow import Schema, fields
import numpy as np
import pandas as pd
import serpy

# Import custom functions
from sec_scraper.scrape import scrape_sec_data
from sec_scraper.convert_to_parquet import get_ticker_file, parquet_transformer

# Fetch variables from Airflow Variables
bucket_name = Variable.get("s3_bucket_name")
AWS_KEY_ID = Variable.get("AWS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
s3_path = f"s3://{bucket_name}"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='sec_json_data_to_snowflake',
    default_args=default_args,
    description='Load parquet files into S3 (if needed) and print their head rows',
    schedule_interval=None,
    tags=['sec_datapipelines'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    class FinancialElementImportDto:
        def __init__(self, label="", concept="", info="", unit="", value=0.0):
            self.label = label
            self.concept = concept
            self.info = info
            self.unit = unit
            self.value = value


    class FinancialsDataDto:
        def __init__(self):
            self.bs = []
            self.cf = []
            self.ic = []


    class SymbolFinancialsDto:
        def __init__(self):
            self.startDate = date.today()
            self.endDate = date.today()
            self.year = 0
            self.quarter = ""
            self.symbol = ""
            self.name = ""
            self.country = ""
            self.city = ""
            self.data = FinancialsDataDto()


    # Serpy serializers (for fast serialization)
    class FinancialElementImportSchema(serpy.Serializer):
        label = serpy.StrField()
        concept = serpy.StrField()
        info = serpy.StrField()
        unit = serpy.StrField()
        value = serpy.FloatField()


    class FinancialsDataSchema(serpy.Serializer):
        bs = serpy.MethodField()
        cf = serpy.MethodField()
        ic = serpy.MethodField()

        def get_bs(self, obj):
            return FinancialElementImportSchema(obj.bs, many=True).data

        def get_cf(self, obj):
            return FinancialElementImportSchema(obj.cf, many=True).data

        def get_ic(self, obj):
            return FinancialElementImportSchema(obj.ic, many=True).data


    class SymbolFinancialsSchema(serpy.Serializer):
        startDate = serpy.MethodField()
        endDate = serpy.MethodField()
        year = serpy.IntField()
        quarter = serpy.StrField()
        symbol = serpy.StrField()
        name = serpy.StrField()
        country = serpy.StrField()
        city = serpy.StrField()
        data = serpy.MethodField()

        def get_startDate(self, obj):
            return obj.startDate.strftime("%Y-%m-%d")

        def get_endDate(self, obj):
            return obj.endDate.strftime("%Y-%m-%d")

        def get_data(self, obj):
            return FinancialsDataSchema(obj.data).data
        
    def get_df_length(**kwargs):
        """Reads the 'sub.parquet' file and pushes its length to XCom."""
        ti = kwargs['ti']
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        # Try pulling the parquet_files dictionary from XCom (from load_data or skip_load_data)
        parquet_files = ti.xcom_pull(task_ids='load_data', key='parquet_files')
        if parquet_files is not None:
            print(f"Using data from 'load_data': {parquet_files}")
        else:
            parquet_files = ti.xcom_pull(task_ids='skip_load_data', key='parquet_files')
            print(f"Using data from 'skip_load_data': {parquet_files}")
        
        # Read sub.parquet and determine the DataFrame length
        if 'sub.parquet' in parquet_files:
            file_obj = s3_hook.get_key(parquet_files['sub.parquet'], bucket_name)
            buffer = BytesIO(file_obj.get()["Body"].read())
            dfSub = pd.read_parquet(buffer)
            df_length = len(dfSub)
        else:
            df_length = 0
            
        print(f"Total rows in dfSub: {df_length}")
        ti.xcom_push(key='dfSub_length', value=df_length)


    def compute_range(task_index, num_chunks, **kwargs):
        """Computes start and end indices for each processing task and pushes them to XCom."""
        ti = kwargs['ti']
        df_length = ti.xcom_pull(task_ids='get_df_length', key='dfSub_length')
        if df_length is None:
            raise ValueError("dfSub_length XCom value is missing!")
        
        # Calculate the chunk size (using integer division)
        chunk_size = df_length // num_chunks
        start_idx = chunk_size * task_index
        # For the last chunk, ensure it covers the remaining rows
        end_idx = chunk_size * (task_index + 1) if task_index < num_chunks - 1 else df_length
        
        print(f"Computed range for task {task_index}: {start_idx} to {end_idx}")
        ti.xcom_push(key=f'process_range_{task_index}', value=(start_idx, end_idx))

    def check_s3_files(**kwargs):
        """Check if all required parquet files exist in S3 for the requested year and quarter"""
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split('Q')[1]
        bucket_name = Variable.get("s3_bucket_name")
        required_files = ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet"]
        s3_hook = S3Hook(aws_conn_id='aws_default')
        file_exists_list = []
        for file_name in required_files:
            key = f"data/{year}/{quarter}/parquet/{file_name}"
            exists = s3_hook.check_for_key(key=key, bucket_name=bucket_name)
            print(f"Checking {key} in bucket {bucket_name}: {exists}")
            file_exists_list.append(exists)
        all_files_exist = all(file_exists_list)
        print(f"All required files exist: {all_files_exist}")
        kwargs['ti'].xcom_push(key='files_exist', value=all_files_exist)
        return all_files_exist
    
    def decide_on_loading(**kwargs):
        """Decide whether to load the parquet files (if missing) or skip loading."""
        files_exist = kwargs['ti'].xcom_pull(task_ids='check_s3_files', key='files_exist')
        return 'load_data' if not files_exist else 'skip_load_data'

    def load_data(**kwargs):
        """Scrape and load Parquet file paths from S3."""
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split('Q')[1]
        parquet_base_path = f"data/{year}/{quarter}/parquet"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        
        # Scrape and convert to parquet files
        extracted_files = scrape_sec_data(year, quarter)
        parquet_transformed_files = parquet_transformer(extracted_files, year, quarter)
        ticker_files = get_ticker_file()
        all_parquet_files = parquet_transformed_files + ticker_files
        
        parquet_files = {}
        for file_name, file_bytes in all_parquet_files:
            s3_key = f"{parquet_base_path}/{file_name}"
            if isinstance(file_bytes, BytesIO):
                file_obj = file_bytes
            else:
                file_obj = BytesIO(file_bytes)
            s3_hook.load_file_obj(file_obj=file_obj, bucket_name=bucket_name, key=s3_key, replace=True)
            parquet_files[file_name] = s3_key
            print(f"Uploaded {file_name} to S3 at {s3_key}")
        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)

    def skip_load_data(**kwargs):
        """Generate the S3 paths for existing parquet files."""
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split('Q')[1]
        parquet_base_path = f"data/{year}/{quarter}/parquet/"
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        parquet_files = {}
        for file_name in ["sub.parquet", "num.parquet", "pre.parquet", "tag.parquet", "ticker.parquet"]:
            file_key = f"{parquet_base_path}{file_name}"
            if s3_hook.check_for_key(file_key, bucket_name):
                parquet_files[file_name] = file_key
            else: 
                raise Exception(f"Not all required parquet files for {year} Q{quarter} have been loaded!")
        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)

    def parquet_to_json(task_index , num_chunks, **kwargs):
        """Read parquet files from S3 and print their head rows."""
        ti = kwargs['ti']
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split('Q')[1]
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = Variable.get("s3_bucket_name")
        parquet_files = ti.xcom_pull(task_ids='load_data', key='parquet_files')
        if not parquet_files:
            parquet_files = ti.xcom_pull(task_ids='skip_load_data', key='parquet_files')
        
        if not parquet_files:
            raise ValueError("Parquet files information not found in XCom.")
        
        def read_parquet(file_name):
            if file_name not in parquet_files:
                raise Exception(f"Missing parquet file: {file_name}")
            s3_key = parquet_files[file_name]
            file_obj = s3_hook.get_key(s3_key, bucket_name)
            buffer = BytesIO(file_obj.get()["Body"].read())
            return pd.read_parquet(buffer)
        
        def npInt_to_str(var):
            return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

        def formatDateNpNum(var):
            dateStr = npInt_to_str(var)
            return dateStr[0:4]+"-"+dateStr[4:6]+"-"+dateStr[6:8]
        
        dfNum = read_parquet("num.parquet")
        dfPre = read_parquet("pre.parquet")
        dfSub = read_parquet("sub.parquet")
        dfTag = read_parquet("tag.parquet")
        dfSym = read_parquet("ticker.parquet")

            # Partition dfSub based on the task_index and num_chunks:
        total_rows = len(dfSub)
        chunk_size = (total_rows + num_chunks - 1) // num_chunks  # ceiling division
        start = task_index * chunk_size
        end = min(start + chunk_size, total_rows)
        dfSub_chunk = dfSub.iloc[start:end]

        # Pull the computed range for the current task from XCom
        process_range = ti.xcom_pull(key=f'process_range_{task_index}', task_ids=f'compute_range_{task_index}')
        if process_range is None:
            raise ValueError(f"No range computed for task {task_index}")
        start_idx, end_idx = process_range

        for subId in range(len(dfSub_chunk)):
            submitted = dfSub_chunk.iloc[subId]
            print(f"Processing adsh: {submitted['adsh']} for task {task_index}")
            sfDto = SymbolFinancialsDto()
            sfDto.data = FinancialsDataDto()
            sfDto.data.bs = []
            sfDto.data.cf = []
            sfDto.data.ic = []
            try:
                periodStartDate = date.fromisoformat(formatDateNpNum(submitted["period"]))
            except ValueError:
                print(submitted["adsh"]+".json has Period: "+str(submitted["period"]))
                continue   
            sfDto.startDate = periodStartDate
            sfDto.endDate = date.today()
            if pd.isna(submitted["fy"]):
                sfDto.year = 0
            else: 
                sfDto.year = submitted["fy"].astype(int)
            sfDto.quarter = str(submitted["fp"]).strip().upper()

            if sfDto.quarter == "FY" or sfDto.quarter == "CY":
                sfDto.endDate = periodStartDate + relativedelta(months=+12, days=-1)
            elif sfDto.quarter == "H1" or sfDto.quarter == "H2":
                sfDto.endDate = periodStartDate + relativedelta(months=+6, days=-1)
            elif sfDto.quarter == "T1" or sfDto.quarter == "T2" or sfDto.quarter == "T3":
                sfDto.endDate = periodStartDate + relativedelta(months=+4, days=-1)
            elif sfDto.quarter == "Q1" or sfDto.quarter == "Q2" or sfDto.quarter == "Q3" or sfDto.quarter == "Q4":
                sfDto.endDate = periodStartDate + relativedelta(months=+3, days=-1)
            else:
                continue

            val = dfSym[dfSym["cik"]==submitted["cik"]]
            if len(val) > 0:
                sfDto.symbol = val["symbol"].to_string(index = False).split("\n")[0].split("\\n")[0].strip().split(" ")[0].strip().upper()
                if len(sfDto.symbol) > 19 or len(sfDto.symbol) < 1:
                    print(submitted["adsh"]+".json has Symbol "+sfDto.symbol)
            else:
                print(submitted["adsh"]+".json has Symbol "+val["symbol"].to_string(index = False))
                continue    
            sfDto.name = submitted["name"]
            sfDto.country = submitted["countryma"]
            sfDto.city = submitted["cityma"]
            sdDto = FinancialsDataDto()
            dfNum['value'] = pd.to_numeric(dfNum['value'], errors='coerce')
            dfNum = dfNum.dropna(subset=['value'])
            dfNum['value'] = dfNum['value'].astype(int)
            filteredDfNum = dfNum[dfNum['adsh'] == submitted['adsh']]
            filteredDfNum = filteredDfNum.reset_index(drop=True)
            for myId in range(len(filteredDfNum)):
                submitted["adsh"] = str(submitted["adsh"])
                myNum = filteredDfNum.iloc[myId]
                myDto = FinancialElementImportDto()
                
                # Match tag
                myTag = dfTag[dfTag["tag"] == myNum['tag']]
                myDto.label = myTag["doc"].to_string(index=False)
                myDto.concept = myNum["tag"]
                
                # Match presentation data
                myPre = dfPre[(dfPre['adsh'] == submitted["adsh"]) & (dfPre['tag'] == myNum['tag'])]
                if myPre.empty:
                    print(f"No matching presentation data found for TAG: {myNum['tag']} in ADSH: {submitted['adsh']}")
                    continue
                
                # Extract info
                myDto.info = myPre["plabel"].to_string(index=False).replace('"', "'")
                myDto.unit = myNum["uom"]
                myDto.value = myNum["value"]
                
                # Debugging stmt value
                stmt_value = myPre['stmt'].iloc[0]  # Directly access the first value
                
                # Append to appropriate list based on stmt value
                if stmt_value == 'BS':
                    sfDto.data.bs.append(myDto)
                elif stmt_value == 'CF':
                    sfDto.data.cf.append(myDto)
                elif stmt_value == 'IS':
                    sfDto.data.ic.append(myDto)
            #result = SymbolFinancialsSchema().dump(sfDto)
            result = SymbolFinancialsSchema(sfDto).data
            result = str(json.dumps(result))
            result = result.replace('\\r', '').replace('\\n', ' ')
            #print(result)
            file_obj = io.BytesIO(result.encode("utf-8"))
            filename = submitted["adsh"]+".json"
            s3_key = f"data/{year}/{quarter}/json/{filename}"
            s3_hook.load_file_obj(file_obj=file_obj, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"Data pushed {s3_key}")

     # Define tasks
    start_task = DummyOperator(task_id="start")
    
    check_files_task = PythonOperator(
        task_id='check_s3_files',
        python_callable=check_s3_files,
        provide_context=True,
    )
    
    load_decider_task = BranchPythonOperator(
        task_id='decide_on_loading',
        python_callable=decide_on_loading,
        provide_context=True,
    )
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )
    
    skip_load_data_task = PythonOperator(
        task_id='skip_load_data',
        python_callable=skip_load_data,
        provide_context=True,
    )
    
    downstream_tasks = DummyOperator(
        task_id='join',
        trigger_rule='none_failed'
    )
    
    length_task = PythonOperator(
        task_id='get_df_length',
        python_callable=get_df_length,
        provide_context=True
    )

    num_chunks = 2
    compute_tasks = []

    # Compute ranges separately (outside the TaskGroup)
    for i in range(num_chunks):
        compute_task = PythonOperator(
            task_id=f'compute_range_{i}',
            python_callable=compute_range,
            op_kwargs={'task_index': i, 'num_chunks': num_chunks},
            provide_context=True,
        )
        compute_tasks.append(compute_task)

    # Process rows inside TaskGroup
    with TaskGroup("parallel_processing") as processing_group:
        process_tasks = []
        for i in range(num_chunks):
            process_task = PythonOperator(
                task_id=f'process_rows_{i}',
                python_callable=parquet_to_json,
                op_kwargs={'task_index': i, 'num_chunks': num_chunks},
                provide_context=True,
            )
            process_tasks.append(process_task)

    
    end_task = DummyOperator(task_id="end")

    # Snowflake-Task 1: Create External Stage in Snowflake
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_v2',
        sql=f"""
            USE ROLE dbt_role;
            CREATE OR REPLACE STAGE sec_json_stage
            URL='{s3_path}'
            CREDENTIALS = (
                AWS_KEY_ID = '{AWS_KEY_ID}',
                AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
            );
        """
    )

    # Snowflake-Task 2: Create File Format for JSON Files
    create_fileformat_json = SnowflakeOperator(
        task_id='create_fileformat_json',
        snowflake_conn_id='snowflake_v2',
        sql="""
            CREATE OR REPLACE FILE FORMAT sec_json_format
            TYPE = 'JSON';
        """
    )

    # Snowflake-Task 3: Create Schema for "json" tables
    def schema_def_json(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split('Q')[1]
        return f"""
            CREATE OR REPLACE TABLE SEC_JSON_{year}_Q{quarter} (
            json_data VARIANT
            );
        """
    generate_ddl_json = PythonOperator(
        task_id='generate_ddl_json',
        python_callable=schema_def_json,
        provide_context=True,
    )
    ddl_json = SnowflakeOperator(
        task_id='ddl_json',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_ddl_json') }}"
    )

    # Snowflake-Task 4: Copy Data from S3 to Snowflake for year and quarter
    def copyinto_json(**kwargs):
        year = kwargs['dag_run'].conf.get('year')
        quarter = kwargs['dag_run'].conf.get('quarter').split('Q')[1]
        return f"""
            COPY INTO SEC_JSON_{year}_Q{quarter}
            FROM @sec_json_stage/data/{year}/{quarter}/json/
            FILE_FORMAT = (TYPE = 'JSON');
            CREATE OR REPLACE VIEW JSON_BS_{year}_Q{quarter} AS
            SELECT 
                t.json_data:year::NUMBER AS year,
                t.json_data:quarter::STRING AS quarter,
                t.json_data:country::STRING AS country,
                t.json_data:city::STRING AS city,
                t.json_data:name::STRING AS company_name,
                t.json_data:symbol::STRING AS symbol,
                
                bs.value:concept::STRING AS concept,
                bs.value:info::STRING AS info,
                bs.value:label::STRING AS label,
                bs.value:unit::STRING AS unit,
                bs.value:value::NUMBER AS value
            FROM SEC_JSON_{year}_Q{quarter} t, 
                LATERAL FLATTEN(input => t.json_data:data.bs) bs;
            
            CREATE OR REPLACE VIEW JSON_CF_{year}_Q{quarter} AS
            SELECT 
                t.json_data:year::NUMBER AS year,
                t.json_data:quarter::STRING AS quarter,
                t.json_data:country::STRING AS country,
                t.json_data:city::STRING AS city,
                t.json_data:name::STRING AS company_name,
                t.json_data:symbol::STRING AS symbol,

                cf.value:concept::STRING AS concept,
                cf.value:info::STRING AS info,
                cf.value:label::STRING AS label,
                cf.value:unit::STRING AS unit,
                cf.value:value::NUMBER AS value
            FROM SEC_JSON_{year}_Q{quarter} t, 
                LATERAL FLATTEN(input => t.json_data:data.cf) cf;
            
            CREATE OR REPLACE VIEW JSON_IS_{year}_Q{quarter} AS
            SELECT 
                t.json_data:year::NUMBER AS year,
                t.json_data:quarter::STRING AS quarter,
                t.json_data:country::STRING AS country,
                t.json_data:city::STRING AS city,
                t.json_data:name::STRING AS company_name,
                t.json_data:symbol::STRING AS symbol,

                ic.value:concept::STRING AS concept,
                ic.value:info::STRING AS info,
                ic.value:label::STRING AS label,
                ic.value:unit::STRING AS unit,
                ic.value:value::NUMBER AS value
            FROM SEC_JSON_{year}_Q{quarter} t, 
                LATERAL FLATTEN(input => t.json_data:data.ic) ic;

            CREATE OR REPLACE VIEW JSON_FV_{year}_Q{quarter} AS
                SELECT *, 'Balance Sheet' AS section_type FROM JSON_BS_{year}_Q{quarter}
                UNION ALL
                SELECT *, 'Cash Flow' AS section_type FROM JSON_CF_{year}_Q{quarter}
                UNION ALL
                SELECT *, 'Income Statement' AS section_type FROM JSON_IS_{year}_Q{quarter};


        """
    generate_copy_sql_json = PythonOperator(
        task_id='generate_copy_sql_json',
        python_callable=copyinto_json,
        provide_context=True,
    )

    copy_from_s3_json = SnowflakeOperator(
        task_id='copy_from_s3_json',
        snowflake_conn_id='snowflake_v2',
        sql="{{ ti.xcom_pull(task_ids='generate_copy_sql_json') }}"
    )

    # Set task dependencies
    start_task >> check_files_task >> load_decider_task
    load_decider_task >> [load_data_task, skip_load_data_task] >> downstream_tasks
    downstream_tasks >> length_task >> compute_tasks >> processing_group >> create_stage >> create_fileformat_json >> generate_ddl_json >> ddl_json >> generate_copy_sql_json >> copy_from_s3_json >> end_task