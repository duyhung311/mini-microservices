-- One database per service. The app role is created by POSTGRES_USER=app.
CREATE DATABASE order_db OWNER app;
CREATE DATABASE restaurant_db OWNER app;
CREATE DATABASE delivery_db OWNER app;
