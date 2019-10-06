# dynamodb-migrator
This simple tool helps you in migrating the tables from one account to another

###How to Use:

Configure your aws credentials file in ˜/.aws/credetials to include the source and destination accounts.


`[source-account]
aws_access_key_id = xxxxx
aws_secret_access_key = xxxxx 
region=<AWS-REGION>`

`[destination-account]
aws_access_key_id = XXXXXXXX
aws_secret_access_key = xxxxxxxx 
region=<AWS-REGION>`

###run the script
run the file DDBMigrator.py