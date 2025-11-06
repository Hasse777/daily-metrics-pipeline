-- Создаем хаб транзакций
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.h_transactions(
	hk_operation_id INT PRIMARY KEY,
	operation_id VARCHAR(120) NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT(NOW()),
	load_src VARCHAR(40) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_operation_id) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Создаем сателлит транзакций
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.s_transactions(
	hk_operation_id INT NOT NULL,
	status VARCHAR(60) NOT NULL,
	transaction_type VARCHAR(60) NOT NULL,
	amount INT NOT NULL,
	transaction_dt TIMESTAMP(3) NOT NULL CHECK (transaction_dt::DATE > '2000-01-01'),
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_s_transactions_to_h_transactions FOREIGN KEY(hk_operation_id) REFERENCES {{ params.schema_dwh }}.h_transactions(hk_operation_id)
)
ORDER BY load_dt, transaction_dt
SEGMENTED BY HASH(hk_operation_id) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Создаем хаб аккаунтов
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.h_accounts(
	hk_account_number INT PRIMARY KEY,
	account_number INT NOT NULL CHECK(account_number >= 0),
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_account_number) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- Создаем связь между транзакциями и аккаунтом
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.l_accounts_transactions(
	hk_l_account_transaction INT PRIMARY KEY,
	hk_operation_id INT NOT NULL,
	hk_account_number_from INT NOT NULL,
	hk_account_number_to INT NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_l_accounts_to_transactions FOREIGN KEY(hk_operation_id) REFERENCES {{ params.schema_dwh }}.h_transactions(hk_operation_id),
	CONSTRAINT fk_l_account_to_account_from FOREIGN KEY(hk_account_number_from) REFERENCES {{ params.schema_dwh }}.h_accounts(hk_account_number),
	CONSTRAINT fk_l_account_to_account_to FOREIGN KEY(hk_account_number_to) REFERENCES {{ params.schema_dwh }}.h_accounts(hk_account_number),
	CONSTRAINT uq_l_accounts_transactions UNIQUE(hk_operation_id, hk_account_number_from, hk_account_number_to)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_l_account_transaction) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Создаем хаб валют
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.h_currencies(
	hk_currency_code INT PRIMARY KEY,
	currency_code INT NOT NULL CHECK(currency_code >= 0),
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_currency_code) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Cоздаем линк отношений валют и их курсу
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.l_currency_ratio(
	hk_l_currency_ratio INT PRIMARY KEY,
	hk_currency_from INT NOT NULL, -- отношение валюты от которой отталкиваемся, например: рубль к доллару
	hk_currency_to INT NOT NULL, -- к какой валюте применяется отношение
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_currency_to_currency_from FOREIGN KEY(hk_currency_from) REFERENCES {{ params.schema_dwh }}.h_currencies(hk_currency_code),
	CONSTRAINT fk_currency_to_currency_to FOREIGN KEY(hk_currency_to) REFERENCES {{ params.schema_dwh }}.h_currencies(hk_currency_code),
	CONSTRAINT uq_l_currency_ratio UNIQUE(hk_currency_from, hk_currency_to)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_l_currency_ratio) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Создаем саттелит отношений валют
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.s_l_currency_ratio(
	hk_l_currency_ratio INT NOT NULL,
	currency_with_div NUMERIC(5, 3) NOT NULL CHECK(currency_with_div >= 0),
	date_update TIMESTAMP(3) NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_s_l_currency_ratio_to_l_currency_ratio FOREIGN KEY(hk_l_currency_ratio) REFERENCES {{ params.schema_dwh }}.l_currency_ratio(hk_l_currency_ratio)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_l_currency_ratio) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- Соединяем валюты с транзакциями
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.l_currency_transaction(
	hk_l_currency_transaction INT PRIMARY KEY,
	hk_currency_code INT NOT NULL,
	hk_operation_id INT NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_l_currency_transaction_to_transaction FOREIGN KEY(hk_operation_id) REFERENCES {{ params.schema_dwh }}.h_transactions(hk_operation_id),
	CONSTRAINT fk_l_currency_transaction_to_currency FOREIGN KEY(hk_currency_code) REFERENCES {{ params.schema_dwh }}.h_currencies(hk_currency_code),
	CONSTRAINT uq_l_currency_transaction UNIQUE(hk_currency_code, hk_operation_id)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_l_currency_transaction) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Cоздаем хабы стран
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.h_countries(
	hk_country_name INT PRIMARY KEY,
	country_name VARCHAR(60) NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_country_name) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- Создаем линк с валютой
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.l_countries_currencies(
	hk_l_countries_currencies INT PRIMARY KEY,
	hk_country_name INT NOT NULL,
	hk_currency_code INT NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_l_countries_h_currencies_to_countries FOREIGN KEY(hk_country_name) REFERENCES {{ params.schema_dwh }}.h_countries(hk_country_name),
	CONSTRAINT fk_l_countries_h_currencies_to_currencies FOREIGN KEY(hk_currency_code) REFERENCES {{ params.schema_dwh }}.h_currencies(hk_currency_code),
	CONSTRAINT uq_l_countries_currencies UNIQUE(hk_country_name, hk_currency_code)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_l_countries_currencies) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- Создаем линк стран с транзакциями
CREATE TABLE IF NOT EXISTS {{ params.schema_dwh }}.l_countries_transactions(
	hk_countries_transactions INT PRIMARY KEY,
	hk_country_name INT NOT NULL,
	hk_operation_id INT NOT NULL,
	load_dt TIMESTAMP(0) NOT NULL DEFAULT NOW(),
	load_src VARCHAR(40) NOT NULL,
	CONSTRAINT fk_l_countries_transactions_to_countries FOREIGN KEY(hk_country_name) REFERENCES {{ params.schema_dwh }}.h_countries(hk_country_name),
	CONSTRAINT fk_l_countries_transactions_to_transactions FOREIGN KEY(hk_operation_id) REFERENCES {{ params.schema_dwh }}.h_transactions(hk_operation_id),
	CONSTRAINT uq_l_countries_transactions UNIQUE(hk_country_name, hk_operation_id)
)
ORDER BY load_dt
SEGMENTED BY HASH(hk_countries_transactions) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);