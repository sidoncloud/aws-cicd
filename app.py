import redshift_connector
import boto3
import logging

# Configure logging to send messages to CloudWatch
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bucket_name = "nl-aws-de-labs"

try:
    conn = redshift_connector.connect(
        host='dwh-wg.127489365181.us-east-1.redshift-serverless.amazonaws.com',
        database='prod_db',
        port=5439,
        user='admin',
        password='Etl123$$'
    )
    cursor = conn.cursor()

    # Execute COPY commands to ingest data into temporary tables
    copy_orders_tmp_sql = """
        COPY prod_schema.orders_tmp 
        FROM 's3://nl-aws-de-labs/orders/'
        IAM_ROLE 'arn:aws:iam::127489365181:role/redshift-custom-role' 
        CSV
        IGNOREHEADER 1;
    """
    copy_order_items_tmp_sql = """
        COPY prod_schema.order_items_tmp
        FROM 's3://nl-aws-de-labs/order_items/' 
        IAM_ROLE 'arn:aws:iam::127489365181:role/redshift-custom-role'
        CSV
        IGNOREHEADER 1;
    """

    # Execute the COPY operations
    cursor.execute(copy_orders_tmp_sql)
    cursor.execute(copy_order_items_tmp_sql)
    logger.info("Data ingestion into temporary Redshift tables completed successfully.")

    # MERGE from temporary to main tables
    merge_orders_sql = """
        MERGE INTO prod_schema.orders
        USING prod_schema.orders_tmp AS source
        ON orders.order_id = source.order_id
        WHEN MATCHED THEN
            UPDATE SET
                user_id = source.user_id,
                status = source.status,
                gender = source.gender,
                created_at = source.created_at,
                returned_at = source.returned_at,
                delivered_at = source.delivered_at,
                num_of_item = source.num_of_item
        WHEN NOT MATCHED THEN
            INSERT (order_id, user_id, status, gender, created_at, returned_at, delivered_at, num_of_item)
            VALUES (source.order_id, source.user_id, source.status, source.gender, source.created_at, source.returned_at, source.delivered_at, source.num_of_item);
    """

    truncate_orders_tmp_sql = """truncate table prod_schema.orders_tmp"""

    merge_order_items_sql = """
        MERGE INTO prod_schema.order_items
        USING prod_schema.order_items_tmp AS source
        ON order_items.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                user_id = source.user_id,
                product_id = source.product_id,
                inventory_item_id = source.inventory_item_id,
                status = source.status,
                created_at = source.created_at,
                shipped_at = source.shipped_at,
                delivered_at = source.delivered_at,
                sale_price = source.sale_price
        WHEN NOT MATCHED THEN
            INSERT (id, order_id, user_id, product_id, inventory_item_id, status, created_at, shipped_at, delivered_at, sale_price)
            VALUES (source.id, source.order_id, source.user_id, source.product_id, source.inventory_item_id, source.status, source.created_at, source.shipped_at, source.delivered_at, source.sale_price);
    """

    truncate_order_items_tmp_sql = """truncate table prod_schema.order_items_tmp"""

    # Execute MERGE operations
    cursor.execute(merge_orders_sql)
    cursor.execute(truncate_orders_tmp_sql)
    cursor.execute(merge_order_items_sql)
    cursor.execute(truncate_order_items_tmp_sql)
    conn.commit()
    logger.info("Merge operations completed successfully.")

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Function to move and delete files
    def move_and_delete_files(source_prefix, destination_prefix):
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)

        for page in pages:
            for obj in page['Contents']:
                # Copy file to new location
                copy_source = f"{bucket_name}/{obj['Key']}"
                new_key = obj['Key'].replace(source_prefix, destination_prefix)
                s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key)
                # Delete original file
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

    # Move and delete files in 'orders' and 'order_items' folders
    move_and_delete_files('orders/', 'processed/orders/')
    move_and_delete_files('order_items/', 'processed/order_items/')
    logger.info("Files moved to 'processed' folder and deleted from the original folders.")

except Exception as e:
    logger.error("Error during Redshift operations:", exc_info=True)
    conn.rollback()
finally:
    conn.close()