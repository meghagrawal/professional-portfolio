CREATE OR REPLACE PROCEDURE LINEAGE.POPULATE_LINEAGE_OBJECT("START_TIME" TIMESTAMP_LTZ, "END_TIME" TIMESTAMP_LTZ)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO LINEAGE.LINEAGE_OBJECT (
        SOURCE_OBJECT_NAME,
        SOURCE_OBJECT_DOMAIN,
        TARGET_OBJECT_NAME,
        TARGET_OBJECT_DOMAIN,
        QUERY_ID,
        QUERY_TYPE,
        QUERY_TEXT,
        QUERY_USER,
        QUERY_START_TIME,
        INSERTION_TIME
    )
    SELECT DISTINCT
        acc.value:objectName::string AS SOURCE_OBJECT_NAME,
        acc.value:objectDomain::string AS SOURCE_OBJECT_DOMAIN,
        mod.value:objectName::string AS TARGET_OBJECT_NAME,
        mod.value:objectDomain::string AS TARGET_OBJECT_DOMAIN,
        q.query_id,
        q.query_type,
        q.query_text,
        q.user_name AS query_user,
        q.start_time AS query_start_time,
        CURRENT_TIMESTAMP() AS INSERTION_TIME
    FROM snowflake.account_usage.query_history q
    JOIN snowflake.account_usage.access_history a ON q.query_id = a.query_id
    , LATERAL FLATTEN(input => a.base_objects_accessed) acc
    , LATERAL FLATTEN(input => a.objects_modified) mod
    WHERE q.start_time BETWEEN :START_TIME AND :END_TIME
      AND mod.value:objectId IS NOT NULL
      AND acc.value:objectId IS NOT NULL
      AND SOURCE_OBJECT_NAME IS NOT NULL
      AND TARGET_OBJECT_NAME IS NOT NULL
      AND SOURCE_OBJECT_NAME != TARGET_OBJECT_NAME;

    RETURN 'Object lineage populated successfully.';
END;
$$;