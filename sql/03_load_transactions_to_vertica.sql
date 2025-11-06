-- Перед вставкой очищаем таблицу
-- TRUNCATE TABLE {{ params.schema_stg }}.transactions;

COPY {{params.schema_stg}}.transactions(operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
FROM LOCAL '{{params.path}}/transactions_{{ds}}.csv'
DELIMITER ','
SKIP 1
REJECTED DATA AS TABLE transactions_broken_REJECTED_str;