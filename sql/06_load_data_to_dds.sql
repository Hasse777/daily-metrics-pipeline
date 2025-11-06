-- Начинаем вставку в хабы 
-- Вставляем значения в хаб транзакций
INSERT INTO {{ params.schema_dwh }}.h_transactions(hk_operation_id, operation_id, load_src)
SELECT DISTINCT
    HASH(t.operation_id) AS hk_operation_id,
    t.operation_id AS operation_id,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE 
    AND t.account_number_from >= 0 
    AND t.account_number_to >= 1
    AND NOT EXISTS (
        SELECT 1
        FROM {{params.schema_dwh}}.h_transactions h
        WHERE h.hk_operation_id = HASH(t.operation_id)
    );


-- Заполняем хаб стран
INSERT INTO {{ params.schema_dwh }}.h_countries(hk_country_name, country_name, load_src)
SELECT DISTINCT
    HASH(LOWER(t.country)) AS hk_country_name,
    LOWER(t.country) AS country_name,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND t.country IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM {{params.schema_dwh}}.h_countries h
        WHERE h.hk_country_name = HASH(LOWER(t.country))
    );

-- Заполняем хаб валют
INSERT INTO {{ params.schema_dwh }}.h_currencies(hk_currency_code, currency_code, load_src)
SELECT DISTINCT
    HASH(t.currency_code) AS hk_currency_code,
    t.currency_code AS currency_code,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND t.currency_code >= 0
    AND NOT EXISTS (
        SELECT 1
        FROM {{ params.schema_dwh }}.h_currencies AS h
        WHERE h.hk_currency_code = HASH(t.currency_code)
    );


-- Заполняем хаб аккаунтов
INSERT INTO {{ params.schema_dwh }}.h_accounts(hk_account_number, account_number, load_src)
WITH all_accounts AS (
    SELECT
        HASH(t.account_number_from) AS hk_account_number,
        account_number_from AS account_number,
        t.load_src AS load_src
    FROM {{ params.schema_stg }}.transactions AS t
    WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
        AND account_number_from >= 0

    UNION

    SELECT
        HASH(t.account_number_to) AS hk_account_number,
        account_number_to AS account_number,
        t.load_src AS load_src
    FROM {{ params.schema_stg }}.transactions AS t
    WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
        AND account_number_to >= 0
)
SELECT
    aa.hk_account_number AS hk_account_number,
    aa.account_number AS account_number,
    aa.load_src AS load_src
FROM all_accounts AS aa
WHERE NOT EXISTS(
    SELECT 1
    FROM {{ params.schema_dwh }}.h_accounts AS h
    WHERE h.hk_account_number = aa.hk_account_number
);


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Заполняем линки
-- Линк аккаунтов с транзакциями 
INSERT INTO {{ params.schema_dwh }}.l_accounts_transactions(hk_l_account_transaction, hk_operation_id, hk_account_number_from, hk_account_number_to, load_src)
SELECT DISTINCT
    HASH(ht.operation_id, t.account_number_from, t.account_number_to) AS hk_l_account_transaction,
    ht.hk_operation_id AS hk_operation_id,
    ha_from.hk_account_number AS hk_account_number_from,
    ha_to.hk_account_number AS hk_account_number_to,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
INNER JOIN {{ params.schema_dwh }}.h_transactions AS ht USING(operation_id)
INNER JOIN {{ params.schema_dwh }}.h_accounts AS ha_from ON HASH(t.account_number_from) = ha_from.hk_account_number
INNER JOIN {{ params.schema_dwh }}.h_accounts AS ha_to ON HASH(t.account_number_to) = ha_to.hk_account_number
WHERE t.account_number_from >= 0
    AND t.account_number_to >= 0
    AND '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND NOT EXISTS(
        SELECT 1
        FROM {{ params.schema_dwh }}.l_accounts_transactions AS lat
        WHERE lat.hk_l_account_transaction = HASH(ht.operation_id, t.account_number_from, t.account_number_to)
    );


-- Линк транзакций и стран
INSERT INTO {{ params.schema_dwh }}.l_countries_transactions(hk_countries_transactions, hk_country_name, hk_operation_id, load_src)
SELECT DISTINCT
    HASH(t.operation_id, LOWER(t.country)) AS hk_countries_transactions,
    hc.hk_country_name AS hk_country_name,
    ht.hk_operation_id AS hk_operation_id,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
INNER JOIN {{ params.schema_dwh }}.h_transactions AS ht USING(operation_id)
INNER JOIN {{ params.schema_dwh }}.h_countries AS hc ON hc.country_name = LOWER(t.country)
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND t.country IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM {{ params.schema_dwh }}.l_countries_transactions AS lct
        WHERE lct.hk_countries_transactions = HASH(t.operation_id, LOWER(t.country))
    );


-- Линк валют и транзакций
INSERT INTO {{ params.schema_dwh }}.l_currency_transaction(hk_l_currency_transaction, hk_currency_code, hk_operation_id, load_src)
SELECT DISTINCT
    HASH(t.currency_code, t.operation_id) AS hk_l_currency_transaction,
    hc.hk_currency_code AS hk_currency_code,
    ht.hk_operation_id AS hk_operation_id,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
INNER JOIN {{ params.schema_dwh }}.h_transactions AS ht USING(operation_id)
INNER JOIN {{ params.schema_dwh }}.h_currencies AS hc USING(currency_code)
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND NOT EXISTS(
        SELECT 1
        FROM {{ params.schema_dwh }}.l_currency_transaction AS lct
        WHERE lct.hk_l_currency_transaction = HASH(t.currency_code, t.operation_id)
    );


-- Линк стран и валюты страны
INSERT INTO {{ params.schema_dwh }}.l_countries_currencies(hk_l_countries_currencies, hk_country_name, hk_currency_code, load_src)
SELECT DISTINCT
    HASH(LOWER(t.country), t.currency_code) AS hk_l_countries_currencies,
    hcou.hk_country_name AS hk_country_name,
    hc.hk_currency_code AS hk_currency_code,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
INNER JOIN {{ params.schema_dwh }}.h_transactions AS ht USING(operation_id)
INNER JOIN {{ params.schema_dwh }}.h_countries AS hcou ON LOWER(t.country) = hcou.country_name
INNER JOIN {{ params.schema_dwh }}.h_currencies AS hc USING(currency_code)
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND NOT EXISTS(
        SELECT 1
        FROM {{ params.schema_dwh }}.l_countries_currencies AS lcc
        WHERE lcc.hk_l_countries_currencies = HASH(LOWER(t.country), t.currency_code)
    );


-- Линк отношений валют
INSERT INTO {{ params.schema_dwh }}.l_currency_ratio(hk_l_currency_ratio, hk_currency_from, hk_currency_to, load_src)
SELECT DISTINCT
    HASH(c.currency_code, currency_code_with) AS hk_l_currency_ratio,
    hc_from.hk_currency_code AS hk_currency_from,
    hc_to.hk_currency_code AS hk_currency_to,
    c.load_src AS load_src
FROM {{ params.schema_stg }}.currencies AS c
INNER JOIN {{ params.schema_dwh }}.h_currencies AS hc_from USING(currency_code)
INNER JOIN {{ params.schema_dwh }}.h_currencies AS hc_to ON hc_to.currency_code = c.currency_code_with
WHERE '{{ ds }}'::DATE = c.date_update::DATE
    AND NOT EXISTS (
        SELECT 1
        FROM {{ params.schema_dwh }}.l_currency_ratio AS lcr
        WHERE lcr.hk_l_currency_ratio = HASH(c.currency_code, currency_code_with)
    );


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Заполняеми сателиты

-- Сателит транзакций
-- Здесь нужно быть аккуратным так как этот запрос не идемпотентый и при каждом перезапуске у нас будут дубли
-- Сделал это осознанно, так как сателиты хранят историю и на больших таблицах запрос будет выполнятся дольше
INSERT INTO {{ params.schema_dwh }}.s_transactions(hk_operation_id, status, transaction_type, amount, transaction_dt, load_src)
SELECT DISTINCT
    ht.hk_operation_id AS hk_operation_id,
    t.status AS status,
    t.transaction_type AS transaction_type,
    t.amount AS amount,
    t.transaction_dt AS transaction_dt,
    t.load_src AS load_src
FROM {{ params.schema_stg }}.transactions AS t
INNER JOIN {{ params.schema_dwh }}.h_transactions AS ht USING(operation_id)
WHERE '{{ ds }}'::DATE = t.transaction_dt::DATE
    AND t.status IS NOT NULL
    AND t.transaction_type IS NOT NULL
    AND t.amount IS NOT NULL;


--Сателит отношений валют
INSERT INTO {{ params.schema_dwh }}.s_l_currency_ratio(hk_l_currency_ratio, currency_with_div, date_update, load_src)
SELECT DISTINCT
    lcr.hk_l_currency_ratio AS hk_l_currency_ratio,
    c.currency_with_div AS currency_with_div,
    c.date_update AS date_update,
    c.load_src AS load_src
FROM {{ params.schema_stg }}.currencies AS c
INNER JOIN {{ params.schema_dwh }}.l_currency_ratio AS lcr ON lcr.hk_l_currency_ratio = HASH(c.currency_code, c.currency_code_with)
WHERE '{{ ds }}'::DATE = c.date_update::DATE
    AND c.currency_with_div IS NOT NULL
    AND c.currency_with_div >= 0;
