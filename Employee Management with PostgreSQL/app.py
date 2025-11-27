
from flask import Flask, request, jsonify ,render_template
import logging
from employee_db import EmployeeDB


app=Flask(__name__)
db=EmployeeDB()


logging.basicConfig(
    level=logging.DEBUG,
    filename="app.log",
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Flask application started")


@app.route("/")
def index_page():
    return render_template("index.html")

# 1️⃣ Get a specific employee by ID
@app.route("/employee/<int:emp_id>", methods=["GET"])
def api_get_employee(emp_id):
    logging.debug(f"GET /employee/{emp_id} invoked")
    employee = db.get_employee(emp_id)
    if employee:
        logging.info(f"Employee {emp_id} fetched successfully")
        return jsonify(employee), 200
    logging.warning(f"Employee {emp_id} not found")
    return jsonify({"error": "Employee not found"}), 404

# 2️⃣ Get all employees
@app.route("/employees", methods=["GET"])
def api_get_all_employees():
    logging.debug("GET /employees invoked")
    employees = db.get_all_employees()
    logging.info(f"{len(employees)} employees returned")
    return jsonify(employees), 200

# 3️⃣ Add a new employee
@app.route("/employee", methods=["POST"])
def api_add_employee():
    logging.debug("POST /employee invoked")
    data = request.json
    if not data:
        logging.error("POST /employee: No JSON body received")
        return jsonify({"error": "Invalid input"}), 400

    try:
        new_employee = db.insert_employee(
            emp_id=data.get("id"),
            name=data.get("name"),
            salary=data.get("salary"),
            department=data.get("department"),
            age=data.get("age")
        )
        logging.info(f"New employee added: {new_employee}")
        return jsonify({"message": "Employee added", "employee": new_employee}), 201

    except Exception as e:
        logging.error(f"Failed to add employee: {e}")
        return jsonify({"error": "Failed to add employee"}), 500


# 4️⃣ Update existing employee
@app.route("/employee/<int:emp_id>", methods=["PATCH"])
def api_patch_employee(emp_id):
    data = request.json or {}

    # convert empty or missing values => None so SQL keeps old value
    clean_data = {}
    for key, value in data.items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            clean_data[key] = None
        else:
            clean_data[key] = value

    updated = db.update_employee(
        emp_id=emp_id,
        name=clean_data.get("name"),
        salary=clean_data.get("salary"),
        department=clean_data.get("department"),
        age=clean_data.get("age")
    )

    if updated:
        return jsonify({"message": "Employee updated", "employee": updated}), 200
    return jsonify({"error": "Employee not found"}), 404


# 5️⃣ Delete employee
@app.route("/employee/<int:emp_id>", methods=["DELETE"])
def api_delete_employee(emp_id):
    logging.debug(f"DELETE /employee/{emp_id} invoked")
    try:
        deleted = db.delete_employee(emp_id)
        if deleted:
            logging.info(f"Employee {emp_id} deleted successfully")
            return jsonify({"message": "Employee deleted", "deleted_id": emp_id}), 200

        logging.warning(f"Employee {emp_id} not found for deletion")
        return jsonify({"error": "Employee not found"}), 404

    except Exception as e:
        logging.error(f"Failed to delete employee {emp_id}: {e}")
        return jsonify({"error": "Failed to delete employee"}), 500


if __name__ == "__main__":
    db.create_table()
    app.run(debug=True)