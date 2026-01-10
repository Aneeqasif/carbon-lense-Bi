SELECT * FROM duckdb_tables();


SELECT *
FROM "raw-mp".carbonlens_stationarycombustions ;


-- run this to get the structure that duckdb infers
select json_structure(document) from rawjson.users;

--paste that here in json_transform for example.
select unnest(json_transform(document, '{"_id":"VARCHAR","name":"VARCHAR","email":"VARCHAR","address":"VARCHAR","joined_at":"DATE","active":"BOOLEAN","updated_at":"VARCHAR"}')
) from rawjson.users;

select count(*) from rawjson.users;
describe rawjson.users;

WITH latest_docs AS (
  SELECT 
    object_id,
    document,
    operation_type,
    cluster_time,
    ROW_NUMBER() OVER (
      PARTITION BY object_id 
      ORDER BY cluster_time DESC
    ) as rn
  FROM rawjson.users
  WHERE _sdc_deleted_at IS NULL  -- Not deleted
)
SELECT 
  object_id,
  document,
  operation_type as last_operation,
  cluster_time as last_updated
FROM latest_docs
WHERE rn = 1;

create or replace view currentdocs as
SELECT *
FROM rawjson.users
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY object_id 
  ORDER BY cluster_time DESC
) = 1
AND _sdc_deleted_at IS NULL;


SELECT *
FROM rawjson.users
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY object_id
  ORDER BY
    cluster_time DESC,
    _sdc_extracted_at DESC
) = 1
AND _sdc_deleted_at IS NULL;


select * from "rawjson"."users";
