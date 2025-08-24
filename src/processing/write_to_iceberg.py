from pyiceberg.catalog import load_catalog
catalog = load_catalog('hive_catalog', uri='thrift://minio:9083', warehouse='s3a://iceberg-data/')
table = catalog.create_table(
  identifier='lakehouse.orders',
  schema=...
)