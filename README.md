# Retail Data Processing with PySpark Streaming

This project demonstrates batch and streaming data processing using PySpark. It involves generating input data in JSON format, performing batch processing, and then converting the batch processing code to streaming processing to handle real-time data.

## Table of Contents

- [Generating Input Data (JSON)](#generating-input-data-json)
- [Batch Processing Use Case](#batch-processing-use-case)
- [Streaming Use Case: Complete Output Mode](#streaming-use-case-complete-output-mode)
- [Streaming Use Case: Update Mode](#streaming-use-case-update-mode)
- [Prerequisites](#prerequisites)
- [Running the Project](#running-the-project)
- [Project Structure](#project-structure)
- [License](#license)

## Generating Input Data (JSON)

1. **Create Spark Session.**

2. **Define the Schema and Create DataFrames:**
   - Orders, Order_item, and Customers datasets.

3. **Join the DataFrames:**
   - Save the results in `joined_df`.

4. **Perform GroupBy on `joined_df`:**
   - Use `collect_list()` and `struct()` functions.

5. **Save DataFrame as JSON:**
   ```python
   result_df \
       .repartition(1) \
       .write \
       .format("json") \
       .mode("overwrite") \
       .option("path", "/user/.../data_json_orders") \
       .save()

## Batch Processing Use Case

1. **Create Spark Session.**

2. **Define Schema:**
   - Use array-struct for line items.

3. **Read Data:**
   ```python
   orders_df = spark.read \
    .format("json") \
    .schema(orders_schema) \
    .option("path", "/user/...") \
    .load()

4. **Create Temporary Views:**
   ```python
   orders_df.createOrReplaceTempView("orders")

5. **Explode and Flatten Data:**
   ```python
   exploded_orders = spark.sql("""
    SELECT order_id, customer_id, city, state, pincode, explode(line_items) AS lines
    FROM orders
    """)

    exploded_orders.createOrReplaceTempView("exploded_orders")
  
    flattened_orders = spark.sql("""
    SELECT order_id, customer_id, city, state, pincode, lines.order_item_id AS item_id,
           lines.order_item_product_id AS product_id, lines.order_item_quantity AS quantity,
           lines.order_item_product_price AS price, lines.order_item_subtotal AS subtotal
    FROM exploded_orders
    """)

   flattened_orders.createOrReplaceTempView("orders_flattened")

6. **Aggregate Data:**
   ```python
   aggregated_orders = spark.sql("""
    SELECT customer_id, COUNT(DISTINCT order_id) AS orders_placed, COUNT(item_id) AS products_purchased,
           SUM(subtotal) AS amount_spent
    FROM orders_flattened
    GROUP BY customer_id
    """)

   aggregated_orders.createOrReplaceTempView("orders_aggregated")

7. **Save Results:**
   ```python
   aggregated_orders.repartition(1) \
    .write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", True) \
    .option("path", "/user/.../result") \
    .save()

## Streaming Use Case: Complete Output Mode

1. **Start Databricks Cluster.**

2. **Create New Notebook:**

3. **Define Schema:**
   - Use array-struct for line items.

4. **Read Streaming Data:**
   ```python
   orders_df = spark.readStream \
    .format("json") \
    .schema(orders_schema) \
    .option("path", "/user/...") \
    .load()

5. **Create Temporary Views:**
   ```python
   orders_df.createOrReplaceTempView("orders")

6. **Explode and Flatten Data:**
   ```python
   exploded_orders = spark.sql("""
    SELECT order_id, customer_id, city, state, pincode, explode(line_items) AS lines
    FROM orders
    """)
  
    exploded_orders.createOrReplaceTempView("exploded_orders")

    flattened_orders = spark.sql("""
      SELECT order_id, customer_id, city, state, pincode, lines.order_item_id AS item_id,
           lines.order_item_product_id AS product_id, lines.order_item_quantity AS quantity,
           lines.order_item_product_price AS price, lines.order_item_subtotal AS subtotal
      FROM exploded_orders
      """)

   flattened_orders.createOrReplaceTempView("orders_flattened")

7. **Aggregate Data:**
   ```python
   aggregated_orders = spark.sql("""
    SELECT customer_id, approx_count_distinct(order_id) AS orders_placed, COUNT(item_id) AS products_purchased,
       SUM(subtotal) AS amount_spent
    FROM orders_flattened
    GROUP BY customer_id
    """)

   aggregated_orders.createOrReplaceTempView("orders_aggregated")

8. **Write Results with Complete Mode:**
   ```python
   streamingQuery = aggregated_orders \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("header", True) \
    .option("checkpointLocation", "checkpointdir101") \
    .toTable("orders_results101")

9. **View Final Results:**
    ```python
    spark.sql("SELECT * FROM orders_results101").show()

## Streaming Use Case: Update Mode

1. **Logic for Update Mode:**
   - Implement upsert operation using foreachBatch.

2. **Merge Logic:**
   ```python
   def myFunction(orders_result, batch_id):
    orders_result.createOrReplaceTempView("orders_result")
    merge_statement = """
        MERGE INTO orders_final_result t
        USING orders_result s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN
           UPDATE SET t.products_purchased = s.products_purchased, t.orders_placed = s.orders_placed, t.amount_spent = s.amount_spent
        WHEN NOT MATCHED THEN
           INSERT *
        """

   orders_result._jdf.sparkSession().sql(merge_statement)

   streamingQuery = aggregated_orders \
      .writeStream \
      .format("delta") \
      .outputMode("update") \
      .option("header", True) \
      .option("checkpointLocation", "checkpointdir101") \
      .foreachBatch(myFunction) \
      .start()

3. **Create Final Result Table:**
   ```python
   spark.sql("CREATE TABLE orders_final_result (customer_id LONG, orders_placed LONG, products_purchased LONG, amount_spent FLOAT)")

4. **View Final Results:**
   ```python
   spark.sql("SELECT * FROM orders_final_result").show()

## Prerequisites

- Apache Spark
- PySpark
- Databricks (Community Edition or higher)
- Storage system (HDFS, ADLS Gen2, etc.)

## Running the Project

1. Clone the repository.
2. Set up the environment.
3. Follow the steps in the provided notebooks for each use case.

## Project Structure

- **input_data_generation.ipynb:** Notebook for generating input data.
- **batch_processing.ipynb:** Notebook for batch processing.
- **streaming_processing_complete_mode.ipynb:** Notebook for streaming processing in complete output mode.
- **streaming_processing_update_mode.ipynb:** Notebook for streaming processing in update mode.

## License

This project is licensed under the MIT License. You are free to use, modify, and distribute this software in any way you see fit, provided that this license notice appears in all copies of the software.
