import os, requests
import streamlit as st
from streamlit_option_menu import option_menu
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

# Airflow API endpoint
AIRFLOW_API_URL = "http://35.209.49.151:8080"
QUERY_API_URL = "https://fastapi-service-7ss2sa6dka-uc.a.run.app"

def populate_schema(source, year, quarter):
    SCHEMAS = {}
    if source == "FACT":
        SCHEMAS = {
            "Balance_Sheet": [
                "COMPANY_NAME", "COMPANY_ID", "FILING_DATE", "PERIOD",
                "FISCAL_YEAR", "FISCAL_PERIOD", "UNIT", "PREFERRED_LABEL",
                "TOTAL_REPORTED_AMOUNT", "TAG", "DATATYPE", "DOCUMENTATION"
            ],
            "Cash_Flow": [
                "COMPANY_NAME", "COMPANY_ID", "FILING_DATE", "PERIOD",
                "FISCAL_YEAR", "FISCAL_PERIOD", "UNIT", "PREFERRED_LABEL",
                "TOTAL_REPORTED_AMOUNT", "TAG", "DATATYPE", "DOCUMENTATION"
            ],
            "Income_Statement": [
                "COMPANY_NAME", "COMPANY_ID", "FILING_DATE", "PERIOD",
                "FISCAL_YEAR", "FISCAL_PERIOD", "UNIT", "PREFERRED_LABEL",
                "TOTAL_REPORTED_AMOUNT", "TAG", "DATATYPE", "DOCUMENTATION"
            ]
            }
    elif source == "RAW":
        SCHEMAS = {
                    f"STG_NUM_{year}_{quarter}": [
                        "SUBMISSION_ID", "TAG", "VERSION", "PERIOD_END_DATE",
                        "NUM_QUATERS_COVERED", "UNIT", "SEGMENTS", "COREG",
                        "REPORTED_AMOUNT", "FOOTNOTE"
                    ],
                    f"STG_PRE_{year}_{quarter}": [
                        "SUBMISSION_ID", "REPORT", "LINE", "STATEMENT_TYPE",
                        "DIRECTLY_REPORTED", "RFILE", "TAG", "VERSION",
                        "PREFERRED_LABEL", "NEGATING"
                    ],
                    f"STG_SUB_{year}_{quarter}": [
                        "SUBMISSION_ID", "COMPANY_ID", "COMPANY_NAME", "SIC_CODE",
                        "BUSINESS_COUNTRY", "BUSINESS_STATE", "BUSINESS_CITY",
                        "BUSINESS_ZIP", "BUSINESS_ADD_1", "BUSINESS_ADD_2",
                        "BUSINESS_PH_NO", "MAILING_COUNTRY", "MAILING_STATE",
                        "MAILING_CITY", "MAILING_ZIP", "REGISTRANT_MAIL_ADD_1",
                        "REGISTRANT_MAIL_ADD_2", "COUNTRYINC", "STATE_PROV_INC",
                        "EMPLOYER_ID", "FORMER", "CHANGED", "AFS", "WKSI", "FYE",
                        "FORM", "PERIOD", "FILING_DATE", "FISCAL_YEAR", "FISCAL_PERIOD",
                        "ACCEPTED", "PREVRPT", "DETAIL", "INSTANCE", "NCIKS", "ACIKS"
                    ],
                    f"STG_TAG_{year}_{quarter}": [
                        "TAG", "VERSION", "CUSTOM", "ABSTRACT", "DATATYPE",
                        "ITEM_ORDER", "BALANCE_TYPE", "TAG_LABEL", "DOCUMENTATION"
                    ]
                }
    else:
        SCHEMAS = {
                    f"SEC_JSON_{year}_{quarter}": ["JSON_DATA"]
                }
    selected_schema = st.selectbox(
            "Choose Table:",
            options=list(SCHEMAS.keys())
            )
    schema_df = pd.DataFrame({
    f"{selected_schema}": SCHEMAS[selected_schema]
    })
    st.write(f"**{selected_schema} Schema:**")
    st.dataframe(schema_df, use_container_width=True, hide_index=True)
    schema_df = pd.DataFrame({
    f"{selected_schema}": SCHEMAS[selected_schema]
    })
    
def populate_airflow_page():
    # Display the airflow page
    st.subheader("Trigger Airflow DAGs")
    # Input fields for source, year and quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT"])
    with col2:
        year = st.selectbox("Select Year", range(2024, 2009,-1))
    with col3:
        quarter = st.selectbox("Select Quarter", ("Q1","Q2","Q3","Q4"))
    trigger = st.button("Trigger Airflow DAG", use_container_width=True)
    if trigger:
        # Payload for triggering the DAG
        source = source.lower()
        payload = {
            "conf": {
                "source": source,
                "year": year,
                "quarter": quarter
            }
        }
        # Trigger the DAG via Airflow REST API
        response = requests.post(
            f"{AIRFLOW_API_URL}/api/v1/dags/sec_{source}_data_to_snowflake/dagRuns",
            json=payload,
            auth=(f"{os.getenv('AIRFLOW_USER')}", f"{os.getenv('AIRFLOW_PASSCODE')}")
        )
        if response.status_code == 200:
            st.success("DAG triggered successfully!")
        else:
            st.error(f"Failed to trigger DAG: {response.text}")
    
def populate_query_page():
    # Display the query page
    st.subheader("Query Snowflake")

    if "flag" not in st.session_state:
        st.session_state.flag = False
    if "query" not in st.session_state:
        st.session_state.query = None
    if "table_avail" not in st.session_state:
        st.session_state.query = None
    if "prev_inputs" not in st.session_state:
        st.session_state.prev_inputs = {"source": None, "year": None, "quarter": None}
        
    # Input fields for source, year and quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT"])
    with col2:
        year = st.selectbox("Select Year", range(2024, 2009,-1))
    with col3:
        quarter = st.selectbox("Select Quarter", ("Q1","Q2","Q3","Q4"))
        
    # Check if any of the inputs have changed
    current_inputs = {"source": source, "year": year, "quarter": quarter}
    if current_inputs != st.session_state.prev_inputs:
        # Reset session state flag and update previous inputs
        st.session_state.flag = False
        st.session_state.prev_inputs = current_inputs
    
    # Button to trigger the query
    avail = st.button("Check Availability", use_container_width=True)
    # Handle data availability logic
    if avail:
        # st.session_state.flag = check_data_availability(source, year, quarter)
        # Execute the query
        st.session_state.table_avail = generate_check_query(source, year, quarter)
        print(st.session_state.table_avail)
        query_executed = check_data_availability(st.session_state.table_avail)
        if query_executed[0]["record_count"] > 0:
            st.success(f"Data available. Kindly find the schema for **{source}** below.")
            st.session_state.flag = True
        else:
            st.info(f"No data available for **{source}**, Year: **{year}**, Quarter: **{quarter}**. Trigger the Airflow DAG to fetch data.")

    # Show query input only if data is available
    if st.session_state.flag:
        
        # Insert Schema Function here
        
        populate_schema(source, year, quarter)
        
        # Text area for query input (persistent using session state)
        st.session_state.query = st.text_area(
            "Enter your query here:", 
            value=st.session_state.query, 
            height=200,
            key="query_input"
        )
        # Button to run the query
        run_query = st.button("Run Query", use_container_width=True)
        if run_query:
            if st.session_state.query is not None:  # Ensure the query is not empty
                # Execute the query
                query_executed = execute_query(st.session_state.query)
                st.write("Query Results:")
                st.dataframe(query_executed)
                st.success(f"Query executed successfully.")
            else:
                st.error("Please enter a valid query before running.")
                # Additional user guidance when no action is taken yet
        
def generate_check_query(source, year, quarter):
    query_gen = ""
    # Sample SQL Queries to Check Data Availability in Snowflake
    # SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='DBT_SCHEMA' AND TABLE_NAME IN ('NUM_2024_Q1', 'PRE_2024_Q1', 'SUB_2024_Q1', 'TAG_2024_Q1');
    # SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='DBT_SCHEMA' AND TABLE_NAME = 'SEC_JSON_2024_Q1';
    # SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='DBT_SCHEMA' AND TABLE_NAME IN ('FACT_BS_2024_Q1', 'FACT_CF_2024_Q1', 'FACT_IS_2024_Q1');
    if source == "RAW":
        query_gen = f"SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}' AND TABLE_NAME IN ('STG_NUM_{year}_{quarter}', 'STG_PRE_{year}_{quarter}', 'STG_SUB_{year}_{quarter}', 'STG_TAG_{year}_{quarter}');"
    elif source == "JSON":
        query_gen = f"SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}' AND TABLE_NAME = 'SEC_JSON_{year}_{quarter}';"
    elif source == "FACT":
        query_gen = f"SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}' AND TABLE_NAME IN ('BALANCE_SHEET', 'CASH_FLOW', 'INCOME_STATEMENT');"
    return(query_gen)

def check_data_availability(query):
    # Placeholder function to check data availability
    if query.strip():  # Ensure the query is not empty
        try:
            # Send the query to the FastAPI backend
            response = requests.get(f"{QUERY_API_URL}/check-availability", params={"query": query})            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json().get("data", [])
                # Display the data in a table format if results are available
                if data:
                    return(data)
                else:
                    st.write("No data returned for the given query.")
            else:
                # Handle errors from the API
                st.write("No data returned for the given query. Please check the query.")
        except Exception as e:
            # Handle connection or other exceptions
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid SQL query.")
    
def execute_query(query):
    if query.strip():  # Ensure the query is not empty
        try:
            # Send the query to the FastAPI backend
            response = requests.get(f"{QUERY_API_URL}/query-data", params={"query": query})            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json().get("data", [])
                # Display the data in a table format if results are available
                if data:
                    return(data)
                else:
                    st.write("No data returned for the given query.")
            else:
                # Handle errors from the API
                st.error(f"Error: {response.status_code} - {response.text}")
        except Exception as e:
            # Handle connection or other exceptions
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid SQL query.")

def main():
    # Set the title of the app
    st.title("US Securities and Exchange Commission")
    st.header("Financial Data Pipeline")
    
    # Create a sidebar menu
    with st.sidebar:
        selected = option_menu(
            menu_title="Main Menu",  # Title of the menu
            options=["Airflow", "Query Snowflake" ],  # Menu options
            icons=["rocket", "database"],  # Bootstrap icons (optional)
            default_index=0,  # Default selected option
        )
    # Display content based on selection
    if selected == "Airflow":
        populate_airflow_page()
    elif selected == "Query Snowflake":
        populate_query_page()
            
if __name__ == "__main__":
# Set page configuration
    st.set_page_config(
        page_title="US SEC - Data Bridge",  # Name of the app
        layout="wide",              # Layout: "centered" or "wide"
        initial_sidebar_state="expanded"  # Sidebar: "expanded" or "collapsed"
    )    
    main()
