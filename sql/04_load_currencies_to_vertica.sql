-- Перед вставкой очищаем таблицу
-- TRUNCATE TABLE {{ params.schema_stg }}.currencies;

COPY {{params.schema_stg}}.currencies(date_update, currency_code, currency_code_with, currency_with_div)
FROM LOCAL '{{params.path}}/currencies_{{ds}}.csv'
DELIMITER ','
SKIP 1
REJECTED DATA AS TABLE currencies_broken_REJECTED_str;