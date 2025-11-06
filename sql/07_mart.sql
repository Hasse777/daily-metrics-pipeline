MERGE INTO {{ params.schema_dwh }}.global_metrics tgt
USING(
	WITH chargeback_tr AS(
		-- Исключаем проблемные транзакции (chargeback, blocked, authorization)
		SELECT DISTINCT
			hk_operation_id AS hk_operation_id,
			"status" AS "status",
			transaction_type AS transaction_type
			FROM {{ params.schema_dwh }}.s_transactions AS st
		WHERE ("status" IN ('blocked', 'chargeback')
			OR transaction_type = 'authorization')
			AND st.transaction_dt::DATE = '{{ ds }}'::DATE
		),
	clean_tr AS(
			-- Оставляем только корректные транзакции
		SELECT DISTINCT
			st.hk_operation_id AS hk_operation_id,
			ABS(st.amount) / 100 AS amount, -- Переводим валюты в полноценную единицу, например: из центов в доллар из копеек в рубль
			-- Возможно стоило бы сделать это при добавлении в сателит.
			-- Так же стоило бы уточнить: А какие точности нам использовать в числах до запятой?
			st.transaction_dt AS transaction_dt
		FROM {{ params.schema_dwh }}.s_transactions AS st
		LEFT JOIN chargeback_tr AS bad USING(hk_operation_id)
		WHERE st.status = 'done'
			AND bad.hk_operation_id IS NULL
 			AND st.transaction_dt::DATE = '{{ ds }}'::DATE
		),
	dollar AS(
		-- Узнаем под каким хэш ключем доллар
		SELECT
			currency_code AS currency_code_dollar,
			hc_cur.hk_currency_code AS hk_currency_code_dollar
		FROM {{ params.schema_dwh }}.h_countries AS hc
		INNER JOIN {{ params.schema_dwh }}.l_countries_currencies AS lcc USING(hk_country_name)
		INNER JOIN {{ params.schema_dwh }}.h_currencies AS hc_cur USING(hk_currency_code)
		WHERE hc.country_name = 'usa'
		),
	last_rate AS (
		-- Функция поможет узнать последний обновленный курс на момент транзакции
		SELECT
			slc.hk_l_currency_ratio AS hk_l_currency_ratio,
			slc.currency_with_div AS currency_with_div,
			slc.date_update AS date_update
		FROM {{ params.schema_dwh }}.s_l_currency_ratio AS slc
		WHERE slc.date_update::DATE <= '{{ ds }}'::DATE
		),
	join_result AS(
		-- Джоиним все нужные таблицы для результата
		SELECT DISTINCT
			ct.hk_operation_id AS hk_operation_id,
			ct.amount AS amount, -- Сумма транзакции в местной валюте,
			ct.amount * COALESCE(
			FIRST_VALUE(lr.currency_with_div) 
			OVER(PARTITION BY hc.currency_code ORDER BY lr.date_update DESC), 1) -- Ищем самый близкий курс валют к дате транзакции
			-- Если текущая валюта уже доллар, то курс нужно выставить к 1
			AS amount_in_dollar, -- Сумма транзакции в долларах
			ct.transaction_dt AS transaction_dt,
			hc.currency_code AS currency_code,
			lac.hk_account_number_from AS account,
			d.currency_code_dollar AS currency_code_dollar
		FROM clean_tr AS ct
		INNER JOIN {{ params.schema_dwh }}.l_currency_transaction AS lct USING(hk_operation_id) -- Добираемся до кода валюты транзакций
		INNER JOIN {{ params.schema_dwh }}.h_currencies AS hc USING(hk_currency_code)
		INNER JOIN {{ params.schema_dwh }}.l_accounts_transactions AS lac USING(hk_operation_id) -- ДОбираемся до пользователей
		CROSS JOIN dollar AS d -- Наконец узнаем код доллара, на этом моменте я начал жалеть, что строил хранилище по DV 2.0
		LEFT JOIN {{ params.schema_dwh }}.l_currency_ratio AS lcr ON lcr.hk_currency_from = hc.hk_currency_code 
			AND lcr.hk_currency_to = d.hk_currency_code_dollar -- Используем левый джоин
		LEFT JOIN last_rate AS lr ON lr.hk_l_currency_ratio = lcr.hk_l_currency_ratio
			AND lr.date_update <= ct.transaction_dt
	)
	SELECT
		jr.transaction_dt::DATE AS date_update, -- Дата расчета
		jr.currency_code AS currency_from, -- местной валюты
		ROUND(SUM(amount_in_dollar), 3) AS amount_total, -- Сумма валюты по коду в долларах,
		ROUND(SUM(amount), 3) AS cnt_transactions, -- Общий объём транзакций по валюте,
		ROUND(SUM(amount) / COUNT(DISTINCT account), 3) AS avg_transactions_per_account, -- Средний объём транзакций с аккаунта,
		COUNT(DISTINCT account) AS cnt_accounts_make_transactions -- количество уникальных аккаунтов с совершёнными транзакциями по валюте
	FROM join_result AS jr
	GROUP BY date_update, currency_from
	) src
ON(tgt.date_update = src.date_update AND tgt.currency_from = src.currency_from)
WHEN MATCHED THEN
	UPDATE SET
		amount_total = src.amount_total,
		cnt_transactions = src.cnt_transactions,
		avg_transactions_per_account = src.avg_transactions_per_account,
		cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN
	INSERT (
		date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
	)
	VALUES (
        src.date_update,
        src.currency_from,
        src.amount_total,
        src.cnt_transactions,
        src.avg_transactions_per_account,
        src.cnt_accounts_make_transactions
    );