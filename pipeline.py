import time
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Base URL 
  # endpoints : - /customers
  #             - /orders
  #             - /products
url = "https://jaffle-shop.scalevector.ai/api/v1"

# Set up client
client = RESTClient(
    base_url=url,
    paginator=PageNumberPaginator(
        base_page=1,
        total_path=None,
        stop_after_empty_page=True
    )
)

# Define the pipeline
pipeline = dlt.pipeline(
    pipeline_name="get_jaffleshop_data",
    destination="duckdb",
    dataset_name="jaffleshop",
)

import os
# Set buffer max_item to speed up data passing between extract and normalize
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '1000'
# Increase max_workers for extract
os.environ["EXTRACT__WORKERS"] = "5"
# Set file max items to 20K enabling extract file rotation
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "20000"
# Increase max_workers for normalize
os.environ['NORMALIZE__WORKERS'] = '5'
# Disable file compression
os.environ['NORMALIZE_DATA_WRITER__DISABLE_COMPRESSION'] = 'true'
# Tune normalization buffer size
os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'
# Set file max items to 20K enabling normalize file rotation
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = '20000'
# Increase max_workers for load
os.environ["LOAD__WORKERS"] = "5"


# Set resource for each endpoint with parallelized enabled 
@dlt.resource(name="customers", write_disposition="replace", parallelized=True)
def get_customers():
    for page in client.paginate("/customers"):
      yield page

@dlt.resource(name="orders", write_disposition="replace", parallelized=True)
def get_orders():
    for page in client.paginate("/orders"):
      yield page

@dlt.resource(name="products", write_disposition="replace", parallelized=True)
def get_products():
    for page in client.paginate("/products"):
      yield page

# Group resources into a single source
@dlt.source(name="jaffle_shop_data")
def get_data():
        return get_customers, get_orders, get_products

# Run pipeline
pipeline_info = pipeline.run(get_data())

print(pipeline_info)

