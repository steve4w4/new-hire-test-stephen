from handler import db, handle_csv_upload
import json
import datetime
import pymongo
import bcrypt
from bson import ObjectId


def dummy_data_decorator(test_function):
    def f():
        '''
          Drop any existing data and fill in some dummy test data,
        as well as creating indexes; the data will be dropped after
        the test as well
        '''

        db.user.drop()
        db.user.create_index([
            ("normalized_email", pymongo.ASCENDING),
        ], unique=True)

        dummy_users = [
            {
                "_id": ObjectId(),
                "name": "Brad Jones",
                "normalized_email": "bjones@performyard.com",
                "manager_id": None,
                "salary": 90000,
                "hire_date": datetime.datetime(2010, 2, 10),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"password", bcrypt.gensalt()),
            },
            {
                "_id": ObjectId(),
                "name": "Ted Harrison",
                "normalized_email": "tharrison@performyard.com",
                "manager_id": None,
                "salary": 50000,
                "hire_date": datetime.datetime(2012, 10, 20),
                "is_active": True,
                "hashed_password": bcrypt.hashpw(b"correct horse battery staple", bcrypt.gensalt()),
            }
        ]

        # Give Ted a manager
        dummy_users[1]["manager_id"] = dummy_users[0]["_id"]

        for user in dummy_users:
            db.user.insert(user)

        db.chain_of_command.drop()
        db.chain_of_command.create_index([
            ("user_id", pymongo.ASCENDING),
        ], unique=True)

        dummy_chain_of_commands = [
            {"user_id": dummy_users[0]["_id"], "chain_of_command":[]},
            {"user_id": dummy_users[1]["_id"], "chain_of_command":[
                dummy_users[0].get("_id")]},
        ]

        for chain_of_command in dummy_chain_of_commands:
            db.chain_of_command.insert(chain_of_command)

        test_function()
        db.user.drop()
        db.chain_of_command.drop()
    return f


@dummy_data_decorator
def test_setup():
    '''
    This test should always pass if your environment is set up correctly
    '''
    assert(True)


@dummy_data_decorator
def test_simple_csv():
    '''
    This should successfully update one user and create one user,
    also updating their chain of commands appropriately
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Brad Jones,bjones@performyard.com,,100000,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 0)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 100000)

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_number():
    '''
    This test should still update Brad and create John, but should return
    a single error because the salary field for Brad isn't a number
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,NOT A NUMBER,02/10/2010
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 1)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's salary was updated
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["salary"] == 90000)
    assert(brad["name"] == "Bradley Jones")

    # Check that Brad's chain of command is still empty
    brad_chain_of_command = db.chain_of_command.find_one(
        {"user_id": brad["_id"]})
    assert(len(brad_chain_of_command["chain_of_command"]) == 0)

    # Check that John's data was inserted correctly
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    assert(john["name"] == "John Smith")
    assert(john["salary"] == 80000)
    assert(john["manager_id"] == brad["_id"])
    assert(john["hire_date"] == datetime.datetime(2018, 7, 16))

    # Check that Brad is in John's chain of command
    john_chain_of_command = db.chain_of_command.find_one(
        {"user_id": john["_id"]})
    assert(len(john_chain_of_command["chain_of_command"]) == 1)
    assert(john_chain_of_command["chain_of_command"][0] == brad["_id"])


@dummy_data_decorator
def test_invalid_date():
    '''
    This test should still update Brad and create John, but should return
    a single error because the salary field for Brad isn't a number
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,100000,NOT A DATE
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 1)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 1)

    # Check that we added the correct number of users
    assert(db.user.count() == 3)
    assert(db.chain_of_command.count() == 3)

    # Check that Brad's hire date was not updated, but other fields were
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    assert(brad["hire_date"] == datetime.datetime(2010, 2, 10))
    assert(brad["salary"] == 100000)
    assert(brad["name"] == "Bradley Jones")



@dummy_data_decorator
def test_chain_of_command_multi_level():
    '''
    This test should create a fourth employee, Stephen, who works
    under John Smith, to demonstrate the multi level chain of command
    '''

    body = '''Name,Email,Manager,Salary,Hire Date
Bradley Jones,bjones@performyard.com,,100000,NOT A DATE
John Smith,jsmith@performyard.com,bjones@performyard.com,80000,07/16/2018
Stephen White,swhite@performyard.com,jsmith@performyard.com,99999,10/27/2019
'''

    response = handle_csv_upload(body, {})
    assert(response["statusCode"] == 200)
    body = json.loads(response["body"])

    # Check the response counts
    assert(body["numCreated"] == 2)
    assert(body["numUpdated"] == 1)
    assert(len(body["errors"]) == 1)

    # Check that we added the correct number of users
    assert(db.user.count() == 4)
    assert(db.chain_of_command.count() == 4)

    # Check that Stephen's record was created
    steve = db.user.find_one({"normalized_email": "swhite@performyard.com"})
    assert(steve["hire_date"] == datetime.datetime(2019, 10, 27))
    assert(steve["salary"] == 99999)
    assert(steve["name"] == "Stephen White")

    # Check that Stephen's manager and skiplevel are John and Brad respectively
    john = db.user.find_one({"normalized_email": "jsmith@performyard.com"})
    brad = db.user.find_one({"normalized_email": "bjones@performyard.com"})
    steve_chain_of_command = db.chain_of_command.find_one(
        {"user_id": steve["_id"]})
    assert(len(steve_chain_of_command["chain_of_command"]) == 2)
    assert(steve_chain_of_command["chain_of_command"][0] == john["_id"])
    assert(steve_chain_of_command["chain_of_command"][1] == brad["_id"])