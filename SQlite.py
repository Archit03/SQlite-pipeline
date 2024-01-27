import re
import sqlite3
import csv
from collections import Counter


def connect_sqlite(database_path):
    """
    Connect to an SQLite database and return the connection and cursor.

    Parameters:
    - database_path (str): Path to the SQLite database.

    Returns:
    - conn (sqlite3.Connection): SQLite database connection.
    - cursor (sqlite3.Cursor): SQLite database cursor.
    """
    try:
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
    """
    Import data from a CSV file into an SQLite database.

    Parameters:
    - conn (sqlite3.Connection): SQLite database connection.
    - cursor (sqlite3.Cursor): SQLite database cursor.
    - csv_path (str): Path to the CSV file.
    - table_name (str): Name of the table to create.

    Returns:
    None
    """
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
    """
    Validate email addresses in a specified column of an SQLite table.

    Parameters:
    - conn (sqlite3.Connection): SQLite database connection.
    - cursor (sqlite3.Cursor): SQLite database cursor.
    - table_name (str): Name of the table to validate.
    - column_name (str): Name of the column containing email addresses.

    Returns:
    None
    """
    try:
        query = f"SELECT {column_name} FROM {table_name};"
        cursor.execute(query)
        email_addresses = [row[0] for row in cursor.fetchall()]
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        invalid_emails = [email for email in email_addresses if not email_pattern.match(email)]
        if invalid_emails:
            print(f"Invalid email addresses in table '{table_name}' in column '{column_name}'")
            for email in invalid_emails:
                print(email)
            else:
                print(f"Rest all values in column '{column_name}' are valid email addresses.")
    except sqlite3.Error as e:
        print(f"Error: {e}")


def log_query(cursor, query):
    """
    Log a query to an 'query_logs' table in the SQLite database.

    Parameters:
    - cursor (sqlite3.Cursor): SQLite database cursor.
    - query (str): SQL query to log.

    Returns:
    None
    """
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS query_logs (query_text TEXT);")
        # Log the query to a table named 'query_logs'
        cursor.execute("INSERT INTO query_logs (query_text) VALUES (?)", (query,))
    except sqlite3.Error as e:
        print(f"Error logging query: {e}")


def predict_most_repeatable_queries(conn, cursor, top_n=5):
    """
    Predict the most repeatable queries based on query logs.

    Parameters:
    - conn (sqlite3.Connection): SQLite database connection.
    - cursor (sqlite3.Cursor): SQLite database cursor.
    - top_n (int): Number of top queries to display. Default is 5.

    Returns:
    None
    """
    try:
        # Fetch query_text and count their occurrences
        query = "SELECT query_text FROM query_logs;"
        cursor.execute(query)
        queries = cursor.fetchall()

        # Use Counter to count occurrences of each query
        query_counter = Counter(queries)

        # Fetch the top N most common queries
        most_common_queries = query_counter.most_common(top_n)

        if most_common_queries:
            print(f"Top {top_n} Most Repeatable Queries:")
            print("----------------------------")
            for query_text, count in most_common_queries:
                print(f"Query: {query_text}\t\tExecution Count: {count}")
        else:
            print("No query logs found.")

    except sqlite3.Error as e:
        print(f"Error: {e}")


def perform_boundary_check(conn, cursor, table_name, column_name, min_value, max_value):
    """
    Perform a boundary check on a specified column in an SQLite table.

    Parameters:
    - conn (sqlite3.Connection): SQLite database connection.
    - cursor (sqlite3.Cursor): SQLite database cursor.
    - table_name (str): Name of the table to perform the check.
    - column_name (str): Name of the column to check.
    - min_value (int): Minimum allowed value.
    - max_value (int): Maximum allowed value.

    Returns:
    None
    """
    try:
        query = f"SELECT * FROM {table_name} WHERE {column_name} < ? OR {column_name} > ?"
        cursor.execute(query, (min_value, max_value))
        violating_rows = cursor.fetchall()
        if violating_rows:
            print(f"Rows in table '{table_name}' with {column_name} outside the specified bounds:")
            for row in violating_rows:
                print(row)
            else:
                print(f"All rows in table '{table_name}' are within specified bounds.")
    except sqlite3.Error as e:
        print(f"Error: {e}")


def validate_age_entries(conn, cursor, table_name, column_name, age_threshold):
    """
    Validate age entries in a specified column of an SQLite table.

    Parameters:
    - conn (sqlite3.Connection): SQLite database connection.
    - cursor (sqlite3.Cursor): SQLite database cursor.
    - table_name (str): Name of the table to validate.
    - column_name (str): Name of the column containing age entries.
    - age_threshold (int): Minimum allowed age.

    Returns:
    None
    """
    try:
        query = f"SELECT user_id, age FROM {table_name};"
        cursor.execute(query)
        age_entries = cursor.fetchall()

        # Convert age values to integers
        age_entries = [(user_id, int(age)) for user_id, age in age_entries]

        invalid_entries = [(user_id, age) for user_id, age in age_entries if age < age_threshold]
        if invalid_entries:
            print(f"Validation Results - Age Entries in '{table_name}':")
            print("-----------------------------------------------------")
            print("User ID\t\tAge\t\tValidation Result")
            print("-----------------------------------------------------")
            for user_id, age in invalid_entries:
                print(f"{user_id}\t\t{age}\t\tInvalid (Age should be >= {age_threshold})")
        else:
            print(f"All age entries in '{table_name}' are valid.")
    except sqlite3.Error as e:
        print(f"Error: {e}")


def main():
    database_path = "Users.db"
    csv_file_path = "C:\\Users\\Asus\\Desktop\\ML-intern\\users.csv"
    table_name = "Users"
    column_name = "email"
    min_value = 20
    max_value = 40
    age_threshold = 18
    sample_query = "SELECT * FROM Users WHERE email LIKE '%@example.com';"

    conn, cursor = connect_sqlite(database_path)
    import_csv_to_sqlite(conn, cursor, csv_file_path, table_name)
    perform_boundary_check(conn, cursor, table_name, column_name, min_value, max_value)
    validate_age_entries(conn, cursor, table_name, column_name, age_threshold)
    validate_email_addresses(conn, cursor, table_name, column_name)
    log_query(cursor, sample_query)
    # Predict most repeatable queries
    predict_most_repeatable_queries(conn, cursor)
    conn.close()


if __name__ == "__main__":
    main()
