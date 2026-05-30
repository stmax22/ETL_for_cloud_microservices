/* Создаем схему cdm. */
DROP SCHEMA IF EXISTS cdm;
CREATE SCHEMA IF NOT EXISTS cdm;


/* Создаем витрину user_product_counters. */
DROP TABLE IF EXISTS cdm.user_product_counters;

CREATE TABLE IF NOT EXISTS cdm.user_product_counters(
	id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	user_id UUID NOT NULL,
	product_id UUID NOT NULL,
	product_name VARCHAR NOT NULL,
	order_cnt INTEGER NOT NULL CHECK(order_cnt >= 0),
	CONSTRAINT unique_user_product_id UNIQUE(user_id, product_id)
);
CREATE INDEX user_product_id ON cdm.user_product_counters(user_id, product_id);


/* Создаем витрину user_category_counters. */
DROP TABLE IF EXISTS cdm.user_category_counters;

CREATE TABLE IF NOT EXISTS cdm.user_category_counters(
	id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	user_id UUID NOT NULL,
	category_id UUID NOT NULL,
	category_name VARCHAR NOT NULL,
	order_cnt INTEGER NOT NULL CHECK(order_cnt >= 0),
	CONSTRAINT unique_user_category_id UNIQUE(user_id, category_id)
);
CREATE INDEX user_category_id ON cdm.user_category_counters(user_id, category_id);


/* Создаем схему stg. */
DROP SCHEMA IF EXISTS stg;
CREATE SCHEMA IF NOT EXISTS stg;


/* Создаем таблицу сырых данных order_events. */
DROP TABLE IF EXISTS stg.order_events;

CREATE TABLE IF NOT EXISTS stg.order_events(
	id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	object_id INTEGER NOT NULL UNIQUE,
	object_type VARCHAR NOT NULL,
	sent_dttm TIMESTAMP NOT NULL,
	payload JSON NOT NULL
);


/* Создаем схему dds. */
DROP SCHEMA IF EXISTS dds;
CREATE SCHEMA IF NOT EXISTS dds;


/* Создаем хаб-таблицу h_user. */
DROP TABLE IF EXISTS dds.h_user;

CREATE TABLE IF NOT EXISTS dds.h_user(
	h_user_pk UUID PRIMARY KEY,
	user_id VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL
);


/* Создаем хаб-таблицу h_product. */
DROP TABLE IF EXISTS dds.h_product;

CREATE TABLE IF NOT EXISTS dds.h_product(
	h_product_pk UUID PRIMARY KEY,
	product_id VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL
);


/* Создаем хаб-таблицу h_category. */
DROP TABLE IF EXISTS dds.h_category;

CREATE TABLE IF NOT EXISTS dds.h_category(
	h_category_pk UUID PRIMARY KEY,
	category_name VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL
);


/* Создаем хаб-таблицу h_restaurant. */
DROP TABLE IF EXISTS dds.h_restaurant;

CREATE TABLE IF NOT EXISTS dds.h_restaurant(
	h_restaurant_pk UUID PRIMARY KEY,
	restaurant_id VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL
);


/* Создаем хаб-таблицу h_order. */
DROP TABLE IF EXISTS dds.h_order;

CREATE TABLE IF NOT EXISTS dds.h_order(
	h_order_pk UUID PRIMARY KEY,
	order_id INTEGER NOT NULL,
	order_dt TIMESTAMP NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL
);


/* Создаем линк-таблицу l_order_product. */
DROP TABLE IF EXISTS dds.l_order_product;

CREATE TABLE IF NOT EXISTS dds.l_order_product(
	hk_order_product_pk UUID PRIMARY KEY,
	h_order_pk UUID NOT NULL,
	h_product_pk UUID NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	CONSTRAINT h_order_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk),
	CONSTRAINT h_product_fk FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk)
);


/* Создаем линк-таблицу l_product_restaurant. */
DROP TABLE IF EXISTS dds.l_product_restaurant;

CREATE TABLE IF NOT EXISTS dds.l_product_restaurant(
	hk_product_restaurant_pk UUID PRIMARY KEY,
	h_product_pk UUID NOT NULL,
	h_restaurant_pk UUID NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	CONSTRAINT h_product_fk FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk),
	CONSTRAINT h_restaurant_fk FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk)
);


/* Создаем линк-таблицу l_product_category. */
DROP TABLE IF EXISTS dds.l_product_category;

CREATE TABLE IF NOT EXISTS dds.l_product_category(
	hk_product_category_pk UUID PRIMARY KEY,
	h_product_pk UUID NOT NULL,
	h_category_pk UUID NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	CONSTRAINT h_product_fk FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk),
	CONSTRAINT h_category_fk FOREIGN KEY (h_category_pk) REFERENCES dds.h_category(h_category_pk)
);


/* Создаем линк-таблицу l_order_user. */
DROP TABLE IF EXISTS dds.l_order_user;

CREATE TABLE IF NOT EXISTS dds.l_order_user(
	hk_order_user_pk UUID PRIMARY KEY,
	h_order_pk UUID NOT NULL,
	h_user_pk UUID NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	CONSTRAINT h_order_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk),
	CONSTRAINT h_user_fk FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk)
);


/* Создаем сателлит-таблицу s_user_names. */
DROP TABLE IF EXISTS dds.s_user_names;

CREATE TABLE IF NOT EXISTS dds.s_user_names(
	h_user_pk UUID NOT NULL,
	username VARCHAR NOT NULL,
	userlogin VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_user_names_hashdiff UUID NOT NULL,
	CONSTRAINT s_user_names_pk PRIMARY KEY (h_user_pk, load_dt),
	CONSTRAINT h_user_fk FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk)
);


/* Создаем сателлит-таблицу s_product_names. */
DROP TABLE IF EXISTS dds.s_product_names;

CREATE TABLE IF NOT EXISTS dds.s_product_names(
	h_product_pk UUID NOT NULL,
	name VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_product_names_hashdiff UUID NOT NULL,
	CONSTRAINT s_product_names_pk PRIMARY KEY (h_product_pk, load_dt),
	CONSTRAINT h_product_fk FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk)
);


/* Создаем сателлит-таблицу s_restaurant_names. */
DROP TABLE IF EXISTS dds.s_restaurant_names;

CREATE TABLE IF NOT EXISTS dds.s_restaurant_names(
	h_restaurant_pk UUID NOT NULL,
	name VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_restaurant_names_hashdiff UUID NOT NULL,
	CONSTRAINT s_restaurant_names_pk PRIMARY KEY (h_restaurant_pk, load_dt),
	CONSTRAINT h_restaurant_fk FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk)
);


/* Создаем сателлит-таблицу s_order_cost. */
DROP TABLE IF EXISTS dds.s_order_cost;

CREATE TABLE IF NOT EXISTS dds.s_order_cost(
	h_order_pk UUID NOT NULL,
	cost DECIMAL(19, 5) NOT NULL,
	payment DECIMAL(19, 5) NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_order_cost_hashdiff UUID NOT NULL,
	CONSTRAINT s_order_cost_pk PRIMARY KEY (h_order_pk, load_dt),
	CONSTRAINT h_order_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk)
);


/* Создаем сателлит-таблицу s_order_status. */
DROP TABLE IF EXISTS dds.s_order_status;

CREATE TABLE IF NOT EXISTS dds.s_order_status(
	h_order_pk UUID NOT NULL,
	status VARCHAR NOT NULL,
	load_dt TIMESTAMP NOT NULL,
	load_src VARCHAR NOT NULL,
	hk_order_status_hashdiff UUID NOT NULL,
	CONSTRAINT s_order_status_pk PRIMARY KEY (h_order_pk, load_dt),
	CONSTRAINT h_order_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk)
);
