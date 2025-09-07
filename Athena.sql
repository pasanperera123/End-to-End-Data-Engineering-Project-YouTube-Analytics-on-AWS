#Athena SQL querry commands


SELECT a.title,a.category_id,b.snippet_title FROM "AwsDataCatalog"."de01-youtubedataanalysis-raw-useast1-db1"."raw_statistics" a 
INNER JOIN "AwsDataCatalog"."de01-youtubedataanalysis-cleaned-useast1-db1"."cleaned_statistics_reference_data" b ON a.category_id=cast(b.id as int);


SELECT a.title,a.category_id,b.snippet_title FROM "AwsDataCatalog"."de01-youtubedataanalysis-raw-useast1-db1"."raw_statistics" a 
INNER JOIN "AwsDataCatalog"."de01-youtubedataanalysis-cleaned-useast1-db1"."cleaned_statistics_reference_data" b ON a.category_id=cast(b.id as int)
where a.region='ca';



