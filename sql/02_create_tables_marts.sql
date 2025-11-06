CREATE TABLE IF NOT EXISTS {{params.schema_dwh}}.global_metrics(
	date_update DATE NOT NULL CHECK(date_update >= '2020-01-01'::DATE),
	currency_from INTEGER CHECK(currency_from >= 0),
	amount_total NUMERIC(18, 3) CHECK(amount_total >= 0),
	cnt_transactions NUMERIC(18, 3) CHECK(cnt_transactions >= 0),
	avg_transactions_per_account NUMERIC(18, 3) CHECK(avg_transactions_per_account >= 0),
	cnt_accounts_make_transactions INT CHECK(cnt_accounts_make_transactions >= 0),
	CONSTRAINT pk_global_metrics PRIMARY KEY(date_update, currency_from)
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2);