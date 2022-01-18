import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node olist_order_dataset
olist_order_dataset_node1642363253757 = glueContext.create_dynamic_frame.from_catalog(
    database="data_demo_database",
    table_name="olist_orders_dataset_csv",
    transformation_ctx="olist_order_dataset_node1642363253757",
)

# Script generated for node olist_costumer_dataset
olist_costumer_dataset_node1642363360729 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="data_demo_database",
        table_name="olist_customers_dataset_csv",
        transformation_ctx="olist_costumer_dataset_node1642363360729",
    )
)

# Script generated for node olist_order_reviews_dataset
olist_order_reviews_dataset_node1642363794256 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="data_demo_database",
        table_name="olist_order_reviews_dataset_csv",
        transformation_ctx="olist_order_reviews_dataset_node1642363794256",
    )
)

# Script generated for node olist_order_paymets_dataset
olist_order_paymets_dataset_node1642363929841 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="data_demo_database",
        table_name="olist_order_payments_dataset_csv",
        transformation_ctx="olist_order_paymets_dataset_node1642363929841",
    )
)

# Script generated for node olist_order_items_dataset
olist_order_items_dataset_node1642364088858 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="data_demo_database",
        table_name="olist_order_items_dataset_csv",
        transformation_ctx="olist_order_items_dataset_node1642364088858",
    )
)

# Script generated for node olist_products_dataset
olist_products_dataset_node1642364194693 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="data_demo_database",
        table_name="olist_products_dataset_csv",
        transformation_ctx="olist_products_dataset_node1642364194693",
    )
)

# Script generated for node olist_sellers_dataset
olist_sellers_dataset_node1642364335808 = glueContext.create_dynamic_frame.from_catalog(
    database="data_demo_database",
    table_name="olist_sellers_dataset_csv",
    transformation_ctx="olist_sellers_dataset_node1642364335808",
)

# Script generated for node olist_geolocation_dataset
olist_geolocation_dataset_node1642364604751 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="data_demo_database",
        table_name="olist_geolocation_dataset_csv",
        transformation_ctx="olist_geolocation_dataset_node1642364604751",
    )
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642363524533 = ApplyMapping.apply(
    frame=olist_order_dataset_node1642363253757,
    mappings=[
        ("col0", "string", "order_id", "bigint"),
        ("col1", "string", "customer_id", "bigint"),
        ("col2", "string", "order_status", "bigint"),
        ("col3", "string", "order_purchase_timestamp", "string"),
        ("col4", "string", "order_approved_at", "string"),
        ("col5", "string", "order_delivered_carrier_date", "date"),
        ("col6", "string", "order_delivered_customer_date", "date"),
        ("col7", "string", "order_estimated_delivery_date", "date"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642363524533",
)

# Script generated for node olist_customer_dataset_apply
olist_customer_dataset_apply_node1642364753060 = ApplyMapping.apply(
    frame=olist_costumer_dataset_node1642363360729,
    mappings=[
        ("customer_id", "long", "customer_id", "long"),
        ("customer_unique_id", "long", "customer_unique_id", "long"),
        ("customer_zip_code_prefix", "long", "customer_zip_code_prefix", "long"),
        ("customer_city", "string", "customer_city", "string"),
        ("customer_state", "string", "customer_state", "string"),
    ],
    transformation_ctx="olist_customer_dataset_apply_node1642364753060",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642452101105 = ApplyMapping.apply(
    frame=olist_order_items_dataset_node1642364088858,
    mappings=[
        ("order_id", "string", "(right) order_id", "string"),
        ("order_item_id", "long", "(right) order_item_id", "long"),
        ("product_id", "string", "(right) product_id", "string"),
        ("seller_id", "string", "(right) seller_id", "string"),
        ("shipping_limit_date", "string", "(right) shipping_limit_date", "string"),
        ("price", "double", "(right) price", "double"),
        ("freight_value", "double", "(right) freight_value", "double"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642452101105",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642452153762 = ApplyMapping.apply(
    frame=olist_order_items_dataset_node1642364088858,
    mappings=[
        ("order_id", "string", "(right) order_id", "string"),
        ("order_item_id", "long", "(right) order_item_id", "long"),
        ("product_id", "string", "(right) product_id", "string"),
        ("seller_id", "string", "(right) seller_id", "string"),
        ("shipping_limit_date", "string", "(right) shipping_limit_date", "string"),
        ("price", "double", "(right) price", "double"),
        ("freight_value", "double", "(right) freight_value", "double"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642452153762",
)

# Script generated for node Apply Mapping
ApplyMapping_node1642364251595 = ApplyMapping.apply(
    frame=olist_products_dataset_node1642364194693,
    mappings=[
        ("product_id", "string", "product_id", "string"),
        ("product_category_name", "string", "product_category_name", "string"),
        ("product_name_lenght", "long", "product_name_lenght", "long"),
        ("product_description_lenght", "long", "product_description_lenght", "long"),
        ("product_photos_qty", "long", "product_photos_qty", "long"),
        ("product_weight_g", "long", "product_weight_g", "long"),
        ("product_length_cm", "long", "product_length_cm", "long"),
        ("product_height_cm", "long", "product_height_cm", "long"),
        ("product_width_cm", "long", "product_width_cm", "long"),
    ],
    transformation_ctx="ApplyMapping_node1642364251595",
)

# Script generated for node Apply Mapping
ApplyMapping_node1642364445647 = ApplyMapping.apply(
    frame=olist_sellers_dataset_node1642364335808,
    mappings=[
        ("seller_id", "string", "seller_id", "string"),
        ("seller_zip_code_prefix", "long", "seller_zip_code_prefix", "long"),
        ("seller_city", "string", "seller_city", "string"),
        ("seller_state", "string", "seller_state", "string"),
    ],
    transformation_ctx="ApplyMapping_node1642364445647",
)

# Script generated for node Join
Join_node1642364640478 = Join.apply(
    frame1=olist_geolocation_dataset_node1642364604751,
    frame2=olist_customer_dataset_apply_node1642364753060,
    keys1=["geolocation_zip_code_prefix"],
    keys2=["customer_zip_code_prefix"],
    transformation_ctx="Join_node1642364640478",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642445404933 = ApplyMapping.apply(
    frame=RenamedkeysforJoin_node1642363524533,
    mappings=[
        ("order_id", "bigint", "(right) order_id", "string"),
        ("customer_id", "bigint", "(right) customer_id", "string"),
        ("order_status", "bigint", "(right) order_status", "bigint"),
        (
            "order_purchase_timestamp",
            "string",
            "(right) order_purchase_timestamp",
            "string",
        ),
        ("order_approved_at", "string", "(right) order_approved_at", "string"),
        (
            "order_delivered_carrier_date",
            "date",
            "(right) order_delivered_carrier_date",
            "date",
        ),
        (
            "order_delivered_customer_date",
            "date",
            "(right) order_delivered_customer_date",
            "date",
        ),
        (
            "order_estimated_delivery_date",
            "date",
            "(right) order_estimated_delivery_date",
            "date",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642445404933",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642445418695 = ApplyMapping.apply(
    frame=RenamedkeysforJoin_node1642363524533,
    mappings=[
        ("order_id", "bigint", "(right) order_id", "string"),
        ("customer_id", "bigint", "(right) customer_id", "string"),
        ("order_status", "bigint", "(right) order_status", "bigint"),
        (
            "order_purchase_timestamp",
            "string",
            "(right) order_purchase_timestamp",
            "string",
        ),
        ("order_approved_at", "string", "(right) order_approved_at", "string"),
        (
            "order_delivered_carrier_date",
            "date",
            "(right) order_delivered_carrier_date",
            "date",
        ),
        (
            "order_delivered_customer_date",
            "date",
            "(right) order_delivered_customer_date",
            "date",
        ),
        (
            "order_estimated_delivery_date",
            "date",
            "(right) order_estimated_delivery_date",
            "date",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642445418695",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642452030080 = ApplyMapping.apply(
    frame=RenamedkeysforJoin_node1642363524533,
    mappings=[
        ("order_id", "bigint", "(right) order_id", "string"),
        ("customer_id", "bigint", "(right) customer_id", "string"),
        ("order_status", "bigint", "(right) order_status", "bigint"),
        (
            "order_purchase_timestamp",
            "string",
            "(right) order_purchase_timestamp",
            "string",
        ),
        ("order_approved_at", "string", "(right) order_approved_at", "string"),
        (
            "order_delivered_carrier_date",
            "date",
            "(right) order_delivered_carrier_date",
            "date",
        ),
        (
            "order_delivered_customer_date",
            "date",
            "(right) order_delivered_customer_date",
            "date",
        ),
        (
            "order_estimated_delivery_date",
            "date",
            "(right) order_estimated_delivery_date",
            "date",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642452030080",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1642445424808 = ApplyMapping.apply(
    frame=olist_customer_dataset_apply_node1642364753060,
    mappings=[
        ("customer_id", "long", "(right) customer_id", "string"),
        ("customer_unique_id", "long", "(right) customer_unique_id", "string"),
        (
            "customer_zip_code_prefix",
            "long",
            "(right) customer_zip_code_prefix",
            "long",
        ),
        ("customer_city", "string", "(right) customer_city", "string"),
        ("customer_state", "string", "(right) customer_state", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1642445424808",
)

# Script generated for node Join
Join_node1642364265666 = Join.apply(
    frame1=ApplyMapping_node1642364251595,
    frame2=RenamedkeysforJoin_node1642452101105,
    keys1=["product_id"],
    keys2=["(right) product_id"],
    transformation_ctx="Join_node1642364265666",
)

# Script generated for node Join
Join_node1642364455473 = Join.apply(
    frame1=ApplyMapping_node1642364445647,
    frame2=RenamedkeysforJoin_node1642452153762,
    keys1=["seller_id"],
    keys2=["(right) seller_id"],
    transformation_ctx="Join_node1642364455473",
)

# Script generated for node Join
Join_node1642363970543 = Join.apply(
    frame1=olist_order_paymets_dataset_node1642363929841,
    frame2=RenamedkeysforJoin_node1642445404933,
    keys1=["order_id"],
    keys2=["(right) order_id"],
    transformation_ctx="Join_node1642363970543",
)

# Script generated for node Join
Join_node1642363854456 = Join.apply(
    frame1=olist_order_reviews_dataset_node1642363794256,
    frame2=RenamedkeysforJoin_node1642445418695,
    keys1=["order_id"],
    keys2=["(right) order_id"],
    transformation_ctx="Join_node1642363854456",
)

# Script generated for node Join
Join_node1642364134617 = Join.apply(
    frame1=olist_order_items_dataset_node1642364088858,
    frame2=RenamedkeysforJoin_node1642452030080,
    keys1=["order_id"],
    keys2=["(right) order_id"],
    transformation_ctx="Join_node1642364134617",
)

# Script generated for node Join
RenamedkeysforJoin_node1642363524533DF = RenamedkeysforJoin_node1642363524533.toDF()
RenamedkeysforJoin_node1642445424808DF = RenamedkeysforJoin_node1642445424808.toDF()
Join_node1642363417871 = DynamicFrame.fromDF(
    RenamedkeysforJoin_node1642363524533DF.join(
        RenamedkeysforJoin_node1642445424808DF,
        (
            RenamedkeysforJoin_node1642363524533DF["customer_id"]
            == RenamedkeysforJoin_node1642445424808DF["(right) customer_id"]
        ),
        "right",
    ),
    glueContext,
    "Join_node1642363417871",
)

# Script generated for node Drop Fields
DropFields_node1642364534038 = DropFields.apply(
    frame=Join_node1642364265666,
    paths=["(right) product_id"],
    transformation_ctx="DropFields_node1642364534038",
)

# Script generated for node Drop Fields
DropFields_node1642364543333 = DropFields.apply(
    frame=Join_node1642364455473,
    paths=["(right) seller_id"],
    transformation_ctx="DropFields_node1642364543333",
)

# Script generated for node Drop Fields
DropFields_node1642364081469 = DropFields.apply(
    frame=Join_node1642363970543,
    paths=["(right) order_id"],
    transformation_ctx="DropFields_node1642364081469",
)

# Script generated for node Drop Fields
DropFields_node1642364075698 = DropFields.apply(
    frame=Join_node1642363854456,
    paths=["(right) order_id"],
    transformation_ctx="DropFields_node1642364075698",
)

# Script generated for node Drop Fields
DropFields_node1642364186111 = DropFields.apply(
    frame=Join_node1642364134617,
    paths=["(right) order_id"],
    transformation_ctx="DropFields_node1642364186111",
)

# Script generated for node Drop Fields
DropFields_node1642363566852 = DropFields.apply(
    frame=Join_node1642363417871,
    paths=["(right) customer_id"],
    transformation_ctx="DropFields_node1642363566852",
)

job.commit()
