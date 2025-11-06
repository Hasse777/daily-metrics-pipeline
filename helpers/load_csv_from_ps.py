from pathlib import Path



def load_data_csv_from_ps(*, pg_conn, ds, output_dir, table_name, date_column):
    """
    Выгружаем данные PostgresSQL в CSV за указанную дату ds.

    Если файл уже существует, то удаляем его

    Args:
        pg_conn: Обьект подключения к postgr
        ds: Дата из контекста аирфлов в формате YYYY-MM-DD
        output_dir: Путь к директории где будет хранится файл csv
        table_name: Имя таблицы, которую выгружаем
        date_column: Название колонки с датой в выгружаемой таблицы.
        Нужна для того чтобы выполнять инкрементальную загрузку
    """
    try:
        sql = f"""
        COPY
            (SELECT *
            FROM {table_name}
            WHERE {date_column}::DATE = '{ds}'
            )
        TO STDOUT WITH CSV HEADER;
        """
        output_path = Path(output_dir) /f'{table_name}_{ds}.csv'

        # Создаем родительскую папку файла если её еще нет
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Если файл данными за эту дату уже есть, пересоздаем
        if output_path.exists():
            output_path.unlink()

        # Выгружаем данные
        with pg_conn.connection() as conn:
            with conn.cursor() as cur, open(output_path, "w", encoding="utf-8") as f:
                cur.copy_expert(sql, f)

    except Exception as e:
        print(f'При загрузки данных {table_name} из postg in csv за дату {ds} произошла ошибка: {e}')
        raise
