-- drop table cdm.user_product_counters;
create table cdm.user_product_counters (
	id serial,
	user_id uuid not null,
	product_id uuid not null,
	product_name varchar not null,
	order_cnt int not null,
	CHECK (order_cnt >=0),
	PRIMARY KEY (id)

);
CREATE UNIQUE INDEX user_id_product_id_index ON cdm.user_product_counters (user_id, product_id);


-- drop table cdm.user_category_counters;
create table cdm.user_category_counters (
	id serial,
	user_id uuid not null,
	category_id uuid not null,
	category_name varchar not null,
	order_cnt int not null,
	CHECK (order_cnt >=0),
	PRIMARY KEY (id)

);
CREATE UNIQUE INDEX user_id_category_id_index ON cdm.user_category_counters (user_id, category_id);


--DROP TABLE dds.h_category;

CREATE TABLE dds.h_category (
	h_category_pk uuid NOT NULL,
	category_name varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_category_pkey PRIMARY KEY (h_category_pk)
);

--DROP TABLE dds.h_order;
CREATE TABLE dds.h_order (
	h_order_pk uuid NOT NULL,
	order_id int4 NOT NULL,
	order_dt timestamp NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_order_pkey PRIMARY KEY (h_order_pk)
);

--DROP TABLE dds.h_product;

CREATE TABLE dds.h_product (
	h_product_pk uuid NOT NULL,
	product_id varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_product_pkey PRIMARY KEY (h_product_pk)
);

--DROP TABLE dds.h_restaurant;

CREATE TABLE dds.h_restaurant (
	h_restaurant_pk uuid NOT NULL,
	restaurant_id varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_restaurant_pkey PRIMARY KEY (h_restaurant_pk)
);


--DROP TABLE dds.h_user;

CREATE TABLE dds.h_user (
	h_user_pk uuid NOT NULL,
	user_id varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_user_pkey PRIMARY KEY (h_user_pk)
);

--DROP TABLE dds.l_order_product;

CREATE TABLE dds.l_order_product (
	hk_order_product_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_order_product_pkey PRIMARY KEY (hk_order_product_pk)
);

ALTER TABLE dds.l_order_product ADD CONSTRAINT fk_l_order_product_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);
ALTER TABLE dds.l_order_product ADD CONSTRAINT fk_l_order_product_product_id FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk);

--DROP TABLE dds.l_order_user;

CREATE TABLE dds.l_order_user (
	hk_order_user_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_user_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_order_user_pkey PRIMARY KEY (hk_order_user_pk)
);

ALTER TABLE dds.l_order_user ADD CONSTRAINT fk_l_order_user_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);
ALTER TABLE dds.l_order_user ADD CONSTRAINT fk_l_order_user_user_id FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk);

--DROP TABLE dds.l_product_category;

CREATE TABLE dds.l_product_category (
	hk_product_category_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	h_category_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_product_category_pkey PRIMARY KEY (hk_product_category_pk)
);

ALTER TABLE dds.l_product_category ADD CONSTRAINT fk_l_product_category_category_id FOREIGN KEY (h_category_pk) REFERENCES dds.h_category(h_category_pk);
ALTER TABLE dds.l_product_category ADD CONSTRAINT fk_l_product_category_product_id FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk);

--DROP TABLE dds.l_product_restaurant;

CREATE TABLE dds.l_product_restaurant (
	hk_product_restaurant_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	h_restaurant_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_product_restaurant_pkey PRIMARY KEY (hk_product_restaurant_pk)
);

ALTER TABLE dds.l_product_restaurant ADD CONSTRAINT fk_l_product_restaurant__restaurant_id FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk);
ALTER TABLE dds.l_product_restaurant ADD CONSTRAINT fk_l_product_restaurant_product_id FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk);

--DROP TABLE dds.s_order_cost;

CREATE TABLE dds.s_order_cost (
	h_order_pk uuid NOT NULL,
	"cost" numeric(19, 5) NOT NULL,
	payment numeric(19, 5) NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_order_cost_hashdiff uuid NOT NULL,
	CONSTRAINT s_order_cost_pkey PRIMARY KEY (h_order_pk, load_dt)
);

ALTER TABLE dds.s_order_cost ADD CONSTRAINT fk_s_order_names_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);

--DROP TABLE dds.s_order_status;

CREATE TABLE dds.s_order_status (
	h_order_pk uuid NOT NULL,
	status varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_order_status_hashdiff uuid NOT NULL,
	CONSTRAINT s_order_status_pkey PRIMARY KEY (h_order_pk, load_dt)
);

ALTER TABLE dds.s_order_status ADD CONSTRAINT fk_s_order_status_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);


-- DROP TABLE dds.s_product_names;

CREATE TABLE dds.s_product_names (
	h_product_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_product_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_product_names_pkey PRIMARY KEY (h_product_pk, load_dt)
);

ALTER TABLE dds.s_product_names ADD CONSTRAINT fk_s_product_names_product_id FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk);

-- DROP TABLE dds.s_restaurant_names;

CREATE TABLE dds.s_restaurant_names (
	h_restaurant_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_restaurant_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_restaurant_names_pkey PRIMARY KEY (h_restaurant_pk, load_dt)
);

ALTER TABLE dds.s_restaurant_names ADD CONSTRAINT fk_s_restaurant_names_restaurant_id FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk);

-- DROP TABLE dds.s_user_names;

CREATE TABLE dds.s_user_names (
	h_user_pk uuid NOT NULL,
	username varchar NOT NULL,
	userlogin varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_user_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_user_names_pkey PRIMARY KEY (h_user_pk, load_dt)
);

ALTER TABLE dds.s_user_names ADD CONSTRAINT fk_s_user_names_user_id FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk);





