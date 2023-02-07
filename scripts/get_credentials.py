import yaml

CREDENTIALS_PATH = 'docker-compose.yml'

def get_db_credentials(file_path):
    '''
        Gets the necessary parameters to connect to the provided 
        database from a yaml file.
    '''
    with open(file_path) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    db_service = data['services']['db']
    port = db_service['ports'][0].split(':')[0]
    environment = db_service['environment']
    dbname = environment['POSTGRES_DB']
    user = environment['POSTGRES_USER']
    password = environment['POSTGRES_PASSWORD']
    return dbname, user, password, port
