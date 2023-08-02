drop table cdm.user_product_counters;
create table cdm.user_product_counters (
	id serial,
	user_id uuid not null,
	product_id uuid not null,
	product_name varchar unique not null,
	order_cnt int not null,
	CHECK (order_cnt >=0),
	PRIMARY KEY (id, user_id)

);
CREATE INDEX user_id_product_id_index ON cdm.user_product_counters (user_id, product_id);


drop table cdm.user_category_counters;
create table cdm.user_category_counters (
	id serial,
	user_id uuid not null,
	category_id uuid not null,
	category_name varchar unique not null,
	order_cnt int not null,
	CHECK (order_cnt >=0),
	PRIMARY KEY (id, user_id)

);
CREATE INDEX user_id_category_id_index ON cdm.user_category_counters (user_id, category_id);



drop table stg.order_events;
create table stg.order_events (
	id serial not null,
	object_id int not null UNIQUE,
	object_type varchar not null,
	sent_dttm timestamp not null,
	payload json not null,
	PRIMARY KEY (id)

);

_________________________________________________________________________

drop table dds.h_user;
create table dds.h_user
(
    h_user_pk uuid primary key not null,
    user_id varchar not null,

    load_dt timestamp not null,
    load_src varchar  not null
);

INSERT INTO dds.h_user(h_user_pk, user_id,load_dt, load_src)
select
       md5(id) as  h_user_pk,
       user_id,
       	
       now() as load_dt,
       'stg-order-service' as load_src
       from stg.order_events
where hash(id) not in (select h_user_pk from dds.h_user);


                    """
                    INSERT INTO dds.h_user(h_user_pk, user_id, load_dt, load_src )
                    VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_id,
                        'load_dt': now(),
                        'load_src': 'stg-order-service'
                    }

_____________________________________________________________________________
drop table dds.h_product;
create table dds.h_product
(
    h_product_pk uuid primary key not null,
    product_id varchar not null,

    load_dt timestamp not null,
    load_src varchar  not null
);


INSERT INTO dds.h_product(h_product_pk, product_id,load_dt, load_src)
select
       md5(id) as  h_product_pk,
       product_id,
       	
       now() as load_dt,
       'stg-order-service' as load_src
       from stg.order_events
where hash(id) not in (select h_product_pk from dds.h_product);
_____________________________________________________________________________

drop table dds.h_category;
create table dds.h_category
(
	 
    h_category_pk uuid primary key not null,
    category_name varchar not null,

    load_dt timestamp not null,
    load_src varchar  not null
);

INSERT INTO dds.h_category(h_category_pk, category_name,load_dt,load_src)
select
       md5(id) as  h_product_pk,
       category_name,
       	
       now() as load_dt,
       'stg-order-service' as load_src
       from stg.order_events
where hash(id) not in (select h_category_pk from dds.h_category);
_____________________________________________________________________________

drop table dds.h_restaurant;
create table dds.h_restaurant (
	h_restaurant_pk uuid primary key not null,
    restaurant_id varchar not null,
    load_dt timestamp not null,
    load_src varchar  not null
);

drop table dds.h_order;


INSERT INTO dds.h_restaurant(h_restaurant_pk, restaurant_id, load_dt,load_src)
select
       md5(id) as  h_restaurant_pk,
       restaurant_id,
       	
       now() as load_dt,
       'stg-order-service' as load_src
       from stg.order_events
where hash(id) not in (select h_restaurant_pk from dds.h_restaurant);
____________________________________________________________________________
create table dds.h_order (
	h_order_pk uuid primary key not null,
    order_id int not null,
    order_dt timestamp not null,
    load_dt timestamp not null,
    load_src varchar  not null
);

INSERT INTO dds.h_order(h_restaurant_pk, restaurant_id, order_dt, load_dt,load_src)
select
       md5(id) as  h_restaurant_pk,
       order_id,
       order_dt,	
       now() as load_dt,
       'stg-order-service' as load_src
       from stg.order_events
where hash(id) not in (select h_restaurant_pk from dds.h_order);
___________________________________________________________________________

drop table dds.l_order_product;
create table dds.l_order_product(
	hk_order_product_pk uuid primary key,
	h_order_pk uuid not null CONSTRAINT fk_l_order_product_order_id REFERENCES dds.h_order (h_order_pk),
	h_product_pk uuid not null CONSTRAINT fk_l_rder_product_product_id  REFERENCES dds.h_product (h_product_pk),
	load_dt timestamp not null,
	load_src varchar not null
);

drop table dds.l_product_restaurant;
create table dds.l_product_restaurant(
	hk_product_restaurant_pk uuid primary key,
	h_product_pk uuid not null CONSTRAINT fk_l_product_restaurant_product_id  REFERENCES dds.h_product (h_product_pk),
	h_restaurant_pk uuid not null CONSTRAINT fk_l_product_restaurant__restaurant_id REFERENCES dds.h_restaurant (h_restaurant_pk),
	load_dt timestamp not null,
	load_src varchar not null
);

_________________________________________________________
CREATE TABLE dds.l_order_user (
	hk_order_user_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_user_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_order_user_pkey PRIMARY KEY (hk_order_user_pk)
);


-- dds.l_order_user foreign keys

ALTER TABLE dds.l_order_user ADD CONSTRAINT fk_l_order_user_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);
ALTER TABLE dds.l_order_user ADD CONSTRAINT fk_l_order_user_user_id FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk);


_________________________________________________________
CREATE TABLE dds.l_product_restaurant (
	hk_product_restaurant_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	h_restaurant_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT l_product_restaurant_pkey PRIMARY KEY (hk_product_restaurant_pk)
);


-- dds.l_product_restaurant foreign keys

ALTER TABLE dds.l_product_restaurant ADD CONSTRAINT fk_l_product_restaurant__restaurant_id FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk);
ALTER TABLE dds.l_product_restaurant ADD CONSTRAINT fk_l_product_restaurant_product_id FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk);
_________________________________________________________

CREATE TABLE dds.s_order_cost (
	h_order_pk uuid NOT NULL,
	"cost" numeric(19, 5) NOT NULL,
	payment numeric(19, 5) NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_order_cost_hashdiff uuid NOT NULL,
	CONSTRAINT s_order_cost_pkey PRIMARY KEY (h_order_pk, load_dt)
);


-- dds.s_order_cost foreign keys

ALTER TABLE dds.s_order_cost ADD CONSTRAINT fk_s_order_names_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);
________________________________
CREATE TABLE dds.s_order_status (
	h_order_pk uuid NOT NULL,
	status varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_order_status_hashdiff uuid NOT NULL,
	CONSTRAINT s_order_status_pkey PRIMARY KEY (h_order_pk, load_dt)
);


-- dds.s_order_status foreign keys

ALTER TABLE dds.s_order_status ADD CONSTRAINT fk_s_order_status_order_id FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk);
________________________________
CREATE TABLE dds.s_product_names (
	h_product_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_product_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_product_names_pkey PRIMARY KEY (h_product_pk, load_dt)
);


-- dds.s_product_names foreign keys

ALTER TABLE dds.s_product_names ADD CONSTRAINT fk_s_product_names_product_id FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk);

________________________________

CREATE TABLE dds.s_restaurant_names (
	h_restaurant_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_restaurant_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_restaurant_names_pkey PRIMARY KEY (h_restaurant_pk, load_dt)
);


-- dds.s_restaurant_names foreign keys

ALTER TABLE dds.s_restaurant_names ADD CONSTRAINT fk_s_restaurant_names_restaurant_id FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk);

________________________________________

CREATE TABLE dds.s_user_names (
	h_user_pk uuid NOT NULL,
	username varchar NOT NULL,
	userlogin varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_user_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_user_names_pkey PRIMARY KEY (h_user_pk, load_dt)
);


-- dds.s_user_names foreign keys

ALTER TABLE dds.s_user_names ADD CONSTRAINT fk_s_user_names_user_id FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk);

