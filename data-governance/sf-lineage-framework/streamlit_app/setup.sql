-- This script sets up the Streamlit in Snowflake application for data lineage visualization.

-- Step 1: Create a stage for the Streamlit app files
-- This stage is where you will upload your Streamlit application file (app.py) and environment.yml.
CREATE OR REPLACE STAGE lineage_app_stage;

-- Step 2: Create the Streamlit application object
-- The MAIN_FILE is your main Streamlit python file.
-- The QUERY_WAREHOUSE is the warehouse that will be used to run the queries from the Streamlit app.
CREATE OR REPLACE STREAMLIT lineage_app
    ROOT_LOCATION = '@lineage_app_stage'
    MAIN_FILE = '/app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH'; -- Replace with your warehouse

-- After running these SQL commands, you need to upload the app.py and environment.yml files
-- to the 'lineage_app_stage' stage. You can do this using the Snowflake UI or SnowSQL.

-- Example using SnowSQL:
-- snowsql -q "PUT file://path/to/your/app.py @lineage_app_stage;"
-- snowsql -q "PUT file://path/to/your/environment.yml @lineage_app_stage;"

-- Once the files are uploaded, you can access the Streamlit app from the Snowflake UI.
