import re
import sqlite3
import csv


def connect_sqlite(database_path):
    try:
        # Attempt to connect to the SQLite database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        if conn is not None and cursor is not None:
            print("Connection successful")
            return conn, cursor
        else:
            print("Connection or cursor creation failed")
            return None, None

    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None, None


def import_csv_to_sqlite(conn, cursor, csv_path, table_name):
    try:
        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)

            # Create the table based on CSV headers
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(headers)});"
            cursor.execute(create_table_query)

            # Import data from CSV
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(headers))});"
            cursor.executemany(insert_query, csv_reader)

            conn.commit()
            print(f"Data from {csv_path} imported to {table_name} table.")

    except sqlite3.Error as e:
        print(f"Error: {e}")


def validate_email_addresses(conn, cursor, table_name, column_name):
    try:
        query = f"SELECT {column_name} FROM {table_name};"
        cursor.execute(query)
        email_addresses = [row[0] for row in cursor.fetchall()]
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        invalid_emails = [email for email in email_addresses if not email_pattern.match(email)]
        if invalid_emails:
            print(f"Invalid email addresses in table '{table_name}' in column '{column_name}")
            for email in invalid_emails:
                print(email)
            else:
                print(f"Rest all values in column '{column_name}' are valid email addresses.")
    except sqlite3.Error as e:
        print(f"Error: {e}")


def perform_boundry_check(database_path, table_name, column_name, min_value, max_value):
    try:
        query = f"SELECT * FROM {table_name} WHERE {column_name} < ? OR {column_name} > ?"
        cursor.execute(query, (min_value, max_value))
        violating_rows = cursor.fetchall()
        if violating_rows:
            print(f"Rows in table '{table_name}' with {column_name} outside the specified bounds:")
            for row in violating_rows:
                print(row)
            else:
                print(f"All rows in table '{table_name}' having are '{column_name}' within specified bounds.")
    except sqlite3.Error as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    database_path = "Users.db"
    csv_file_path = "C:\\Users\\Asus\\Desktop\\ML-intern\\users.csv"
    table_name = "Users"
    column_name = "email"
    min_value = 20
    max_value = 40

    conn, cursor = connect_sqlite(database_path)
    import_csv_to_sqlite(conn, cursor, csv_file_path, table_name)
    perform_boundry_check(database_path, table_name, column_name, min_value, max_value)
    validate_email_addresses(conn, cursor, table_name, column_name)
    conn.close()
