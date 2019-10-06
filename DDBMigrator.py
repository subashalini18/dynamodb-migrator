# Initialize client
import boto3
import time
import logging
from botocore.exceptions import ClientError
from dynamodb_migrator_exceptions import invalid_key

logging.basicConfig(level=logging.INFO)

SOURCE_ACC = 'personal-research'
DEST_ACC = 'personal-research-2'

class Sessions:

    def __init__(self, aws_profile):
        session = boto3.session.Session(profile_name = aws_profile)
        self.ddb = session.client('dynamodb')

    def get_table_attr(self, table_name):
        logging.info('Getting table description/attributes')
        table_desc = self.ddb.describe_table(TableName = table_name)
        try:
            table_value = table_desc['Table']
            attr_def = table_value['AttributeDefinitions']
            key_schema = table_value['KeySchema']
            provisioned_val = table_value['ProvisionedThroughput']
        except KeyError as e:
            raise(invalid_key)
        provisioned_throughput = {'ReadCapacityUnits': provisioned_val['ReadCapacityUnits'],
                                  'WriteCapacityUnits': provisioned_val['WriteCapacityUnits']}
        return attr_def, key_schema, provisioned_throughput

    def compare_tables(self, src_table, dest_table):
        if src_table == dest_table:
            logging.info('THE CONTENTS OF THE TABLES ARE SAME')
        else:
            logging.info('THE CONTENTS OF THE TABLES ARE DIFFERENT')

class BackUpDDBData(Sessions):

    def get_ddb_tables(self):
        response = self.ddb.list_tables()
        tables = response['TableNames']
        return tables

    def get_table_data(self, table):
            response = self.ddb.scan(TableName=table)
            data = response['Items']
            return data

class UploadDataToDDB(Sessions):
    def upload_data(self, table, data):
        resp = self.ddb.put_item(TableName = table, Item = data)
        return resp

    def parse_for_data_other(self, data):
        input_item = {}
        key_list = data.keys()
        for key in key_list:
            value = data[key]
            input_item[key] = value
        return input_item

    def create_table(self, table_name, attr_def, key_schema, provisioned_throughput):
        try:
            resp = self.ddb.create_table(AttributeDefinitions = attr_def,
                                     TableName = table_name,
                                     KeySchema = key_schema,
                                     ProvisionedThroughput = provisioned_throughput)
        except ClientError as e:
            raise(e)

def main():

    source_account = BackUpDDBData(SOURCE_ACC)
    src_tables_list = source_account.get_ddb_tables()

    if len(src_tables_list) == 0:
        logging.info(('NO TABLES FOUND IN ACCOUNT {}').format(source_account))
    else:
        logging.info(('TABLES FOUND IN SOURCE AWS ACCOUNT: {} \n {}').format(SOURCE_ACC, src_tables_list))

    dest_misc = BackUpDDBData(DEST_ACC)
    dest_table_list = dest_misc.get_ddb_tables()

    dest_account = UploadDataToDDB(DEST_ACC)

    # VERIFY IF THE TABLES ARE IN CREATED IN BOTH ACCOUNTS.
    if src_tables_list == dest_table_list:
        logging.info("ALL TABLES ARE FOUND IN BOTH ACCOUNTS!")
    else:
        missing_tables_dest = list(set(src_tables_list) - set(dest_table_list))
        logging.info(('MISSING TABLES IN DESTINATION: \n {}').format(missing_tables_dest))
        for table in missing_tables_dest:
            logging.info(('CREATING TABLE {} IN DESTNATION ACCOUNT. THIS WOULD TAKE FEW SECONDS').format(table))
            try:
                attr_def, key_schema, provisioned_throughput = source_account.get_table_attr(table)
            except invalid_key as e:
                logging.error('Key not found in while retrieving attributes from source table')
                raise
            dest_account.create_table(table, attr_def, key_schema, provisioned_throughput)
            time.sleep(20)

    # GET TABLE DATA
    for table in src_tables_list:
        logging.info(('*** EXPORTING DATA FROM: {}').format(table))
        table_data = source_account.get_table_data(table)
        # FOR EACH TABLE'S JSON, UPLOAD IT OT THE CORRESPONDING DEST TABLE.
        logging.info(('*** UPLOADING DATA TO: {}').format(table))
        for item in table_data:
            resp = dest_account.ddb.put_item(TableName = table, Item = item)
        logging.info(('UPLOADED {} ITEMS INTO TABLE {}').format(len(table_data), table))

if __name__ == "__main__":
    main()



