-- This script creates and manages the tasks for the data lineage framework.
-- A root task orchestrates the object and column lineage tasks.

-- Note: Replace 'COMPUTE_WH' with the name of a warehouse suitable for this workload.

-- Root task to orchestrate the lineage population.
CREATE OR REPLACE TASK LINEAGE.LINEAGE_ROOT_TASK
    WAREHOUSE = 'COMPUTE_WH' -- Replace with your warehouse
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Root task to trigger object and column lineage processing.'
AS
SELECT 1; -- Dummy statement, its purpose is to trigger downstream tasks.

-- Task to populate object lineage. It runs after the root task.
CREATE OR REPLACE TASK LINEAGE.LINEAGE_OBJECT_TASK
    WAREHOUSE = 'COMPUTE_WH' -- Replace with your warehouse
    AFTER LINEAGE.LINEAGE_ROOT_TASK
    COMMENT = 'Populates the object lineage table.'
AS
CALL LINEAGE.POPULATE_LINEAGE_OBJECT(
    -- Use task history to get the last successful run time, avoiding gaps or overlaps.
    -- On first run, it will look back 24 hours.
    NVL(
        (SELECT completed_time 
         FROM TABLE(snowflake.information_schema.task_history(task_name => 'LINEAGE_OBJECT_TASK')) 
         WHERE state = 'SUCCEEDED' 
         ORDER BY completed_time DESC 
         LIMIT 1),
        DATEADD('hour', -24, CURRENT_TIMESTAMP())
    ),
    CURRENT_TIMESTAMP()
);

-- Task to populate column lineage. It runs after the root task.
CREATE OR REPLACE TASK LINEAGE.LINEAGE_COLUMN_TASK
    WAREHOUSE = 'COMPUTE_WH' -- Replace with your warehouse
    AFTER LINEAGE.LINEAGE_ROOT_TASK
    COMMENT = 'Populates the column lineage table.'
AS
CALL LINEAGE.POPULATE_LINEAGE_COLUMN(
    -- Use task history to get the last successful run time.
    NVL(
        (SELECT completed_time 
         FROM TABLE(snowflake.information_schema.task_history(task_name => 'LINEAGE_COLUMN_TASK')) 
         WHERE state = 'SUCCEEDED' 
         ORDER BY completed_time DESC 
         LIMIT 1),
        DATEADD('hour', -24, CURRENT_TIMESTAMP())
    ),
    CURRENT_TIMESTAMP()
);

-- Initially, the tasks are suspended. Resume them to start the lineage processing.
-- Only the root task needs to be resumed manually.
-- ALTER TASK LINEAGE.LINEAGE_OBJECT_TASK RESUME;
-- ALTER TASK LINEAGE.LINEAGE_COLUMN_TASK RESUME;
-- ALTER TASK LINEAGE.LINEAGE_ROOT_TASK RESUME;

SELECT 'Tasks created successfully. Remember to replace COMPUTE_WH and resume the root task.' AS status;