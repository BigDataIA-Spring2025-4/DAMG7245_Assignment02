import os, requests
import streamlit as st
from streamlit_option_menu import option_menu
from dotenv import load_dotenv
import requests
load_dotenv()

# Airflow API endpoint
# AIRFLOW_API_URL = "https://ebaeb7d6-905a-429f-8719-9ff6a3c16313.c67.us-east-1.airflow.amazonaws.com"
AIRFLOW_API_URL = "http://localhost:8080"

QUERY_API_URL = "https://fastapi-service-7ss2sa6dka-uc.a.run.app"

def populate_airflow_page():
    # Display the airflow page
    st.subheader("Trigger Airflow DAGs")
    # Input fields for source, year and quarter
    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT"])
    with col2:
        year = st.selectbox("Select Year", range(2009, 2025))
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
        source = st.selectbox("Choose Source", ["RAW", "JSON", "FACT Tables"])
    with col2:
        year = st.selectbox("Select Year", range(2009, 2025))
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
            st.success(f"Data is available for **{source}**, Year: **{year}**, Quarter: **{quarter}**.")
            st.session_state.flag = True
        else:
            st.info(f"No data available for **{source}**, Year: **{year}**, Quarter: **{quarter}**. Trigger the Airflow DAG to fetch data.")
        # st.write("Query Results:")
        # st.dataframe(query_executed)
        # st.success(f"Query executed successfully.")
    # Show query input only if data is available
    if st.session_state.flag:
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
        query_gen = f"SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}' AND TABLE_NAME IN ('NUM_{year}_{quarter}', 'PRE_{year}_{quarter}', 'SUB_{year}_{quarter}', 'TAG_{year}_{quarter}');"
    elif source == "JSON":
        query_gen = f"SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}' AND TABLE_NAME = 'SEC_JSON_{year}_{quarter}';"
    elif source == "FACT Tables":
        query_gen = f"SELECT COUNT(*) AS RECORD_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{os.getenv('SNOWFLAKE_SCHEMA')}' AND TABLE_NAME IN ('FACT_BS_{year}_{quarter}', 'FACT_CF_{year}_{quarter}', 'FACT_IS_{year}_{quarter}');"
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
                st.error(f"Error: {response.status_code} - {response.text}")
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