CREATE TABLE IF NOT EXISTS {{params.schema_stg}}.transactions(
	operation_id VARCHAR(120) NOT NULL,
	account_number_from INT NOT NULL,
	account_number_to INT,
	currency_code INT,
	country VARCHAR(60) NOT NULL,
	status VARCHAR(60) NOT NULL,
	transaction_type VARCHAR(60) NOT NULL,
	amount INT NOT NULL,
	transaction_dt TIMESTAMP(3) NOT NULL,
	load_dt TIMESTAMP(0) DEFAULT NOW(),
	load_src VARCHAR(40) DEFAULT('postgre') NOT NULL
)
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);


CREATE TABLE IF NOT EXISTS {{params.schema_stg}}.currencies(
	date_update TIMESTAMP(3),
	currency_code INTEGER NOT NULL,
	currency_code_with INTEGER NOT NULL,
	currency_with_div NUMERIC(5, 3) NOT NULL,
	load_dt TIMESTAMP(0) DEFAULT NOW(),
	load_src VARCHAR(40) DEFAULT('postgre') NOT NULL
)
ORDER BY date_update
SEGMENTED BY HASH(currency_code, currency_code_with) ALL NODES;