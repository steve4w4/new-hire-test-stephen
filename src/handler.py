import json
import os
import dateparser
from pymongo import MongoClient
from bson.json_util import dumps

db_uri = os.environ.get("MONGO_DB_URI", "localhost")
db_name = os.environ.get("MONGO_DB_NAME", "new_hire_test")

db = MongoClient(db_uri)[db_name]

expected_columns = ["Name", "Email", "Manager", "Salary", "Hire Date"]


def handle_csv_upload(event, context):
    response_body = {
        "numCreated": 0,
        "numUpdated": 0,
        "errors": [],
    }

    # Split out the input into separate rows
    input_rows = event.split('\n')

    # Check that the input columns are correct
    columns = input_rows[0].split(',')
    if not validate_input_columns(columns):
        exp_cols = str(expected_columns)
        response_body["errors"].append("Input columns must match: " + exp_cols)
        response = {
            "statusCode": 400,
            "body": json.dumps(response_body)
        }

    # Separate out the employees
    employees = input_rows[1:]

    for i in range(0, len(employees)):
        # parse employee data and ignore the entry if there are missing colunns
        employee = employees[i].split(',')
        if not employee or len(employee) != 5:
            continue

        # get a reference to the employee's manager
        mgr_id = None
        mgr_match = None
        if employee[2]:
            manager_query = {"normalized_email": employee[2]}
            mgr_match = db.user.find_one(manager_query)
            mgr_id = mgr_match.get("_id")

        # generate a new record for the employee
        emp_record = {"name": employee[0], "manager_id": mgr_id}
        try:
            emp_record["salary"] = int(employee[3])
        except:
            response_body["errors"].append(
                "Salary must be a valid number, recieved: " + employee[3])

        try:
            emp_record["hire_date"] = dateparser.parse(employee[4])
        except:
            response_body["errors"].append(
                "Hire date must be a valid date, recieved: " + employee[4])

        # check if the employee is already in the db, update or insert
        employee_query = {"normalized_email": employee[1]}
        emp_match = db.user.find_one(employee_query)

        emp_id = None
        if emp_match:
            db.user.update_one(employee_query, {
                "$set": emp_record
            })
            emp_id = emp_match.get("_id")
            response_body["numUpdated"] = response_body["numUpdated"] + 1
        else:
            emp_record["normalized_email"] = employee[1]
            emp_record["is_active"] = False
            emp_record["hashed_password"] = None
            emp_id = db.user.insert(emp_record)
            response_body["numCreated"] = response_body["numCreated"] + 1

        manager_coc_query = {"user_id": mgr_id}
        mgr_coc_obj = db.chain_of_command.find_one(manager_coc_query)

        if mgr_coc_obj:
            mgr_coc = mgr_coc_obj.get("chain_of_command")
            coc_record = None
            if mgr_id:
                coc_record = [mgr_id] + mgr_coc

            if emp_match:
                db.chain_of_command.update_one({"user_id": emp_id}, {
                    "$set": {"chain_of_command": [coc_record]}
                })
            else:
                coc_record = {
                    "user_id": emp_id,
                    "chain_of_command": coc_record
                }
                db.chain_of_command.insert(coc_record)
        else:
            if not emp_match:
                coc_record = {
                    "user_id": emp_id,
                    "chain_of_command": []
                }
                db.chain_of_command.insert(coc_record)

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response


def validate_input_columns(columns):
    if len(columns) != len(expected_columns):
        return False
    for i in range(0, len(columns)):
        if (columns[i] != expected_columns[i]):
            return False
    return True
