import psycopg2
import logging
from decimal import Decimal
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG


class EmployeeDB:

    # ----------------------------------------
    # CONNECT TO DATABASE
    # ----------------------------------------
    def connect(self):
        logging.debug("Connecting to PostgreSQL...")
        try:
            conn = psycopg2.connect(
                **DB_CONFIG,
                cursor_factory=RealDictCursor
            )
            logging.debug("Connection successful.")
            return conn
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise e

    # --------------------------------------------------------------
    # Convert Decimal → float for JSON (applies to dict / list)
    # --------------------------------------------------------------
    def json_safe(self, data):
        if isinstance(data, list):
            return [self.json_safe(item) for item in data]

        if isinstance(data, dict):
            return {k: self.json_safe(v) for k, v in data.items()}

        if isinstance(data, Decimal):
            return float(data)

        return data

    # --------------------------------------------------------------
    # CREATE TABLE
    # --------------------------------------------------------------
    def create_table(self):
        logging.info("Creating employee table if not exists...")
        query = """
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            salary NUMERIC(10,2),
            department VARCHAR(50),
            age INTEGER
        );
        """

        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            logging.info("Table created or already exists.")
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise e
        finally:
            cur.close()
            conn.close()
            logging.debug("Database connection closed after table creation.")

    # --------------------------------------------------------------
    # INSERT EMPLOYEE
    # --------------------------------------------------------------
    def insert_employee(self, emp_id, name, salary, department, age):
        logging.info(f"Inserting employee ID: {emp_id}")
        query = """
        INSERT INTO employees (id, name, salary, department, age)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING *;
        """

        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(query, (emp_id, name, salary, department, age))
            row = cur.fetchone()

            conn.commit()
            row = self.json_safe(row)

            logging.info(f"Employee inserted successfully: {row}")
            return row
        except Exception as e:
            logging.error(f"Error inserting employee: {e}")
            raise e
        finally:
            cur.close()
            conn.close()
            logging.debug("Connection closed after insert.")

    # --------------------------------------------------------------
    # GET SINGLE EMPLOYEE
    # --------------------------------------------------------------
    def get_employee(self, emp_id):
        logging.info(f"Fetching employee with ID: {emp_id}")
        query = "SELECT * FROM employees WHERE id = %s;"

        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(query, (emp_id,))
            row = cur.fetchone()

            row = self.json_safe(row)

            if row:
                logging.debug(f"Employee found: {row}")
            else:
                logging.warning(f"No employee found with ID: {emp_id}")

            return row
        except Exception as e:
            logging.error(f"Error fetching employee: {e}")
            raise e
        finally:
            cur.close()
            conn.close()
            logging.debug("Connection closed after get_employee.")

    # --------------------------------------------------------------
    # GET ALL EMPLOYEES
    # --------------------------------------------------------------
    def get_all_employees(self):
        logging.info("Fetching all employees...")
        query = "SELECT * FROM employees;"

        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()

            rows = self.json_safe(rows)

            logging.debug(f"Total employees fetched: {len(rows)}")
            return rows
        except Exception as e:
            logging.error(f"Error fetching all employees: {e}")
            raise e
        finally:
            cur.close()
            conn.close()
            logging.debug("Connection closed after get_all.")

    # --------------------------------------------------------------
    # UPDATE EMPLOYEE
    # --------------------------------------------------------------
   
    def update_employee(self, emp_id, name=None, salary=None, department=None, age=None):
        logging.info(f"Updating employee ID: {emp_id}")
        query = """
        UPDATE employees
        SET 
            name = COALESCE(NULLIF(%s, ''), name),
            salary = COALESCE(%s, salary),
            department = COALESCE(NULLIF(%s, ''), department),
            age = COALESCE(%s, age)
        WHERE id = %s
        RETURNING *;
        """

        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(query, (name, salary, department, age, emp_id))
            row = cur.fetchone()
            conn.commit()
            return self.json_safe(row)
        except Exception as e:
            logging.error(f"Update failed: {e}")
            raise
        finally:
            cur.close()
            conn.close()


    # --------------------------------------------------------------
    # DELETE EMPLOYEE
    # --------------------------------------------------------------
    def delete_employee(self, emp_id):
        logging.info(f"Deleting employee with ID: {emp_id}")

        query = "DELETE FROM employees WHERE id = %s RETURNING id;"

        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(query, (emp_id,))
            row = cur.fetchone()

            conn.commit()

            if row:
                logging.info(f"Employee deleted: {emp_id}")
                return {"deleted_id": emp_id}
            else:
                logging.warning(f"No employee found to delete with ID: {emp_id}")
                return None
        except Exception as e:
            logging.error(f"Error deleting employee: {e}")
            raise e
        finally:
            cur.close()
            conn.close()
            logging.debug("Connection closed after delete.")

