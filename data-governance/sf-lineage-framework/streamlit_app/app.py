import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import graphviz
from datetime import datetime

# --- Snowflake Connection ---
@st.cache_resource
def get_snowpark_session():
    """Gets the active Snowpark session."""
    return get_active_session()

# --- Query Functions ---
def run_query(query):
    """Executes a query and returns the result as a pandas DataFrame."""
    session = get_snowpark_session()
    if session:
        return session.sql(query).to_pandas()
    return pd.DataFrame()

def get_lineage_query(lineage_type, object_name, direction, time_travel_ts):
    """Builds the recursive SQL query for fetching lineage."""
    # Use the current database and schema from the session
    session = get_snowpark_session()
    current_db = session.get_current_database().replace('"', '')
    current_schema = session.get_current_schema().replace('"', '')
    
    table = f"{current_db}.{current_schema}.{lineage_type}"

    target_col = '"TARGET_OBJECT_NAME"'
    source_col = '"SOURCE_OBJECT_NAME"'

    if lineage_type == "LINEAGE_COLUMN":
        target_col = '"TARGET_COLUMN_NAME"'
        source_col = '"SOURCE_COLUMN_NAME"'

    if direction == 'Backward (Upstream)':
        recursive_join_col = "bl.source"
        main_join_col = f"l.{target_col}"
        start_node_col = target_col
        end_node_col = source_col
    else: # Forward (Downstream)
        recursive_join_col = "fl.target"
        main_join_col = f"l.{source_col}"
        start_node_col = source_col
        end_node_col = target_col

    query = f"""
    WITH RECURSIVE lineage_graph (target, source, level) AS (
        SELECT {target_col} as target, {source_col} as source, 1
        FROM {table}
        WHERE {start_node_col} = '{object_name.upper()}'
          AND "QUERY_START_TIME" <= '{time_travel_ts}'
    UNION ALL
        SELECT l.{target_col}, l.{source_col}, lg.level + 1
        FROM {table} l
        JOIN lineage_graph lg ON l.{main_join_col} = lg.{recursive_join_col}
        WHERE l."QUERY_START_TIME" <= '{time_travel_ts}'
    )
    SELECT DISTINCT source, target FROM lineage_graph;
    """
    return query

# --- Visualization ---
def draw_lineage_graph(df, direction):
    """Draws a directed graph using graphviz."""
    if df.empty:
        st.warning("No lineage data found for the given criteria.")
        return

    dot = graphviz.Digraph(comment='Data Lineage')
    dot.attr('graph', rankdir='LR') # Left to Right layout
    dot.attr('node', shape='box', style='rounded')

    edges = []
    # Snowpark returns column names in uppercase
    source_df_col = 'SOURCE'
    target_df_col = 'TARGET'

    if direction == 'Backward (Upstream)':
        source_col, target_col = source_df_col, target_df_col
    else:
        source_col, target_col = target_df_col, source_df_col

    for index, row in df.iterrows():
        source = row[source_col]
        target = row[target_col]
        if (source, target) not in edges:
            dot.edge(source, target)
            edges.append((source, target))

    st.graphviz_chart(dot)

# --- Streamlit App ---
def main():
    st.set_page_config(page_title="Snowflake Data Lineage Visualizer", layout="wide")
    st.title("❄️ Snowflake Data Lineage Visualizer")

    st.sidebar.header("Lineage Options")
    lineage_level = st.sidebar.radio("Lineage Level", ["Object", "Column"])
    lineage_type = "LINEAGE_OBJECT" if lineage_level == "Object" else "LINEAGE_COLUMN"
    
    direction = st.sidebar.radio("Direction", ['Backward (Upstream)', 'Forward (Downstream)'])
    object_name = st.sidebar.text_input("Object/Column Name (e.g., DB.SCHEMA.TABLE or COLUMN)").strip()
    
    st.sidebar.subheader("Time Travel")
    tt_date = st.sidebar.date_input("Date", datetime.now())
    tt_time = st.sidebar.time_input("Time", datetime.now().time())
    time_travel_ts = f"{tt_date} {tt_time}"

    if st.sidebar.button("Show Lineage"):
        if not object_name:
            st.sidebar.warning("Please enter an object or column name.")
        else:
            with st.spinner("Fetching lineage from Snowflake..."):
                query = get_lineage_query(lineage_type, object_name, direction, time_travel_ts)
                st.write("Generated SQL Query:")
                st.code(query, language='sql')
                lineage_df = run_query(query)
                draw_lineage_graph(lineage_df, direction)

if __name__ == "__main__":
    main()