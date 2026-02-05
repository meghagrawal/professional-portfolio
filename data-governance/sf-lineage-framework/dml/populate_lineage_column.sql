
CREATE OR REPLACE PROCEDURE LINEAGE.POPULATE_LINEAGE_COLUMN("START_TIME" TIMESTAMP_LTZ, "END_TIME" TIMESTAMP_LTZ)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO LINEAGE.LINEAGE_COLUMN (
        SOURCE_OBJECT_NAME,
        SOURCE_OBJECT_DOMAIN,
        SOURCE_COLUMN_NAME,
        TARGET_OBJECT_NAME,
        TARGET_OBJECT_DOMAIN,
        TARGET_COLUMN_NAME,
        QUERY_ID,
        QUERY_TYPE,
        QUERY_TEXT,
        QUERY_USER,
        QUERY_START_TIME,
        INSERTION_TIME
    )
    WITH lineage_cte AS (
        SELECT
            q.query_id,
            q.query_type,
            q.query_text,
            q.user_name AS query_user,
            q.start_time AS query_start_time,
            CURRENT_TIMESTAMP() AS insertion_time,
            target_col.value:columnName::string AS target_column_name,
            modified_obj.value:objectName::string AS target_object_name,
            modified_obj.value:objectDomain::string AS target_object_domain,
            source_col.value:columnName::string AS source_column_name,
            direct_source_obj.value:objectName::string AS source_object_name,
            direct_source_obj.value:objectDomain::string AS source_object_domain
        FROM
            snowflake.account_usage.query_history q
        JOIN
            snowflake.account_usage.access_history a ON q.query_id = a.query_id,
        LATERAL FLATTEN(input => a.objects_modified) modified_obj,
        LATERAL FLATTEN(input => modified_obj.value:columns) target_col,
        LATERAL FLATTEN(input => target_col.value:directSources) source_col_id,
        LATERAL FLATTEN(input => a.direct_objects_accessed) direct_source_obj,
        LATERAL FLATTEN(input => direct_source_obj.value:columns) source_col
        WHERE
            q.start_time BETWEEN :START_TIME AND :END_TIME
            AND source_col_id.value:objectId = direct_source_obj.value:objectId
            AND source_col_id.value:columnId = source_col.value:columnId
    )
    SELECT DISTINCT
        SOURCE_OBJECT_NAME,
        SOURCE_OBJECT_DOMAIN,
        SOURCE_COLUMN_NAME,
        TARGET_OBJECT_NAME,
        TARGET_OBJECT_DOMAIN,
        TARGET_COLUMN_NAME,
        QUERY_ID,
        QUERY_TYPE,
        QUERY_TEXT,
        QUERY_USER,
        QUERY_START_TIME,
        INSERTION_TIME
    FROM lineage_cte
    WHERE SOURCE_OBJECT_NAME IS NOT NULL
      AND SOURCE_COLUMN_NAME IS NOT NULL
      AND TARGET_OBJECT_NAME IS NOT NULL
      AND TARGET_COLUMN_NAME IS NOT NULL;

    RETURN 'Column lineage populated successfully.';
END;
$$;
