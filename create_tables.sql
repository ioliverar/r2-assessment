-- 0
CREATE DATABASE coredbr2;

-- 1
DROP TABLE IF EXISTS stores;
CREATE TABLE stores (
	id varchar(36) NOT NULL,
	store_token varchar(36) NOT NULL,
	store_group char(16) NOT NULL,
	store_name varchar(128) NOT NULL,
	created_datetime datetime NOT NULL,
	PRIMARY KEY (id),
	UNIQUE KEY(store_token)
);

-- 2
DROP TABLE IF EXISTS transaction_sales;
CREATE TABLE transaction_sales (
	id varchar(36) NOT NULL,
	store_token varchar(40) NOT NULL,
	transaction_id varchar(40) NOT NULL,
	receipt_token char(8) NOT NULL,
	created_datetime datetime NOT NULL,
	transaction_datetime datetime NOT NULL,
	amount decimal(16,4) NOT NULL,
	currency char(3) NOT NULL,
	source_id varchar(36) DEFAULT NULL,
	user_role char(32) DEFAULT NULL,
	batch_date date NOT NULL,
	PRIMARY KEY (id)
);