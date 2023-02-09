import yaml
import sys
from datetime import datetime


def get_db_credentials(file_path):
    """
    Gets the necessary parameters to connect to the provided
    database from a yaml file.
    """
    with open(file_path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    db_service = data["services"]["db"]
    port = db_service["ports"][0].split(":")[0]
    environment = db_service["environment"]
    dbname = environment["POSTGRES_DB"]
    user = environment["POSTGRES_USER"]
    password = environment["POSTGRES_PASSWORD"]
    return dbname, user, password, port


def get_operation_date():
    """
    Reads the date for the operations provided by the user.
    """
    date_str = sys.argv[1]
    if not date_str:
        operation_date = datetime.today()
    else:
        try:
            operation_date = datetime.strptime(date_str, "%Y-%m-%d")
            if operation_date > datetime.today():
                print(
                    """
                    Please provide the present date or a date from the past.
                    """
                )
                sys.exit(1)
        except:
            print(
                """
                Please provide a date in the format 'YYYY-MM-DD' or no date at
                all to extract today's data.
                """
            )
            sys.exit(1)
    return operation_date
