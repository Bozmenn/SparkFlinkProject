package com.berkozmen.Model

case class Order(customer_id: String,
                 location: String,
                 seller_id: String,
                 order_date: String,
                 order_id: String,
                 price: Double,
                 product_id: String,
                 status: String)
