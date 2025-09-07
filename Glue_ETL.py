import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog 1
AWSGlueDataCatalog1_node1756995843600 = glueContext.create_dynamic_frame.from_catalog(database="de01-youtubedataanalysis-cleaned-useast1-db1", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog1_node1756995843600")

# Script generated for node AWS Glue Data Catalog 2
AWSGlueDataCatalog2_node1756995992209 = glueContext.create_dynamic_frame.from_catalog(database="de01-youtubedataanalysis-cleaned-useast1-db1", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog2_node1756995992209")

# Script generated for node Join
Join_node1756996046743 = Join.apply(frame1=AWSGlueDataCatalog1_node1756995843600, frame2=AWSGlueDataCatalog2_node1756995992209, keys1=["id"], keys2=["category_id"], transformation_ctx="Join_node1756996046743")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1756996046743, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1756995622863", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1756996811950 = glueContext.getSink(path="s3://de01-youtubedataanalysis-analytics-useast1-dev", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1756996811950")
AmazonS3_node1756996811950.setCatalogInfo(catalogDatabase="de01-youtubedataanalysis-analytics-useast1-db1",catalogTableName="final_analytics")
AmazonS3_node1756996811950.setFormat("glueparquet", compression="snappy")
AmazonS3_node1756996811950.writeFrame(Join_node1756996046743)
job.commit()