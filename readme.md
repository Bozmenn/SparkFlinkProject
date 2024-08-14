# Data Engineering Example Project

Purpose of this project is developing data engineering skills showed below.
- Batch Processing
- Stream Processing

![Project Overview](images/architecture.svg)

The diagram above shows an end-to-end Data Pipeline. Here are Data Producer, Kafka, Flink and Spark applications.

Requirements:

- Write batch Spark application(s) with data in file system.
- Write a Flink application using Kafka topics which is the DataProducer application writes data.
- Develop the project using Scala.

## Problem Details

### **Data**

Data produces from`orders` and `products`. Here are their schemas:

```
orders
 |-- customer_id: string
 |-- location:    string
 |-- seller_id:   string
 |-- order_date:  string
 |-- order_id:    string
 |-- price:       double
 |-- product_id:  string
 |-- status:      string

products
 |-- brandname:    string
 |-- categoryname: string
 |-- productid:    string
 |-- productname:  string
```

`join keys` are showed below:
```
orders.product_id = products.productid
```