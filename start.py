
import os
from termcolor import colored
import sys
import pandas as pd
import time

import _params
from _params import hostTarget, portDbTarget, userDbTarget, nameDbTarget, pwdDbTarget, prefixTableTarget, domainTarget
from _params import hostSource, portDbSource, userDbSource, nameDbSource, pwdDbSource, prefixTableSource, domainSource

from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError

# CLOSE EXISTING CONNECTIONS AND VARIABLES
try:
    engineTarget.dispose()
    engineSource.dispose()
    del listStepScripts
except:
    pass


#  CONNECTIONS
engineTarget = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbTarget, pwdDbTarget, hostTarget, portDbTarget, nameDbTarget), connect_args={'connect_timeout': 60, 'read_timeout': 60, 'write_timeout': 60}, poolclass=QueuePool, pool_recycle=3600)
engineSource = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbSource, pwdDbSource, hostSource, portDbSource, nameDbSource), connect_args={'connect_timeout': 60, 'read_timeout': 60, 'write_timeout': 60}, poolclass=QueuePool, pool_recycle=3600)


############### FUNCTION: ESCAPE SOME CHAR DURING SQL TRANSFERT
def escape_value(value):
    value_str = str(value)
    value_str = value_str.replace("\\", "\\\\")
    value_str = value_str.replace("'", "\\'")
    value_str = value_str.replace(":", "\\:")

    return value_str


############### FUNCTION TO RUN AN UPDATE QUERY UNTIL IT WORKS (TO MANAGE INTERNET CONNECTION OR TRANSACTION ISSUES)
def query_management_update(session, query, max_retries, retry_delay):
    attempts = 0
    while attempts < max_retries:
        try:
            session.execute(text(query))
            session.commit()
            print(colored('OK, update executed on the first try.', 'green'))
            attempts = max_retries
        except OperationalError as e:
            attempts += 1
            if attempts < max_retries:
                print(colored(f'Connection issue, attempt {attempts}/{max_retries}. Retry in {retry_delay} seconds...', 'yellow'))
                time.sleep(retry_delay)
            else:
                print(colored('All attempts did not work!', 'red'))
                raise e


############### FUNCTION TO RUN A SELECT QUERY UNTIL IT WORKS (TO MANAGE INTERNET CONNECTION OR TRANSACTION ISSUES)
############### FUNCTION FOR TARGET
def query_management_select_t(query, max_retries, retry_delay):
    global myResultTarget
    attempts = 0
    while attempts < max_retries:
        try:
            with engineTarget.connect() as conTarget:
                myResultTarget = conTarget.execute(text(query)).scalar()
                print('Select executed on the first try.')
                attempts = max_retries
        except OperationalError as e:
            attempts += 1
            if attempts < max_retries:
                print(colored(f'Connection issue, attempt {attempts}/{max_retries}. Retry in {retry_delay} seconds...', 'yellow'))
                time.sleep(retry_delay)
            else:
                print(colored('All attempts did not work!', 'red'))
                raise e


############### FUNCTION TO RUN A SELECT QUERY UNTIL IT WORKS (TO MANAGE INTERNET CONNECTION OR TRANSACTION ISSUES)
############### FUNCTION FOR SOURCE
def query_management_select_s(query, max_retries, retry_delay):
    global myResultSource
    attempts = 0
    while attempts < max_retries:
        try:
            with engineSource.connect() as conSource:
                myResultSource = conSource.execute(text(query)).scalar()
                print('Select executed on the first try.')
                attempts = max_retries
        except OperationalError as e:
            attempts += 1
            if attempts < max_retries:
                print(colored(f'Connection issue, attempt {attempts}/{max_retries}. Retry in {retry_delay} seconds...', 'yellow'))
                time.sleep(retry_delay)
            else:
                print(colored('All attempts did not work!', 'red'))
                raise e


############### FUNCTION: FIND IF A TABLE EXIST IN TARGET
def isTableExistInTarget(myTable):

    sqlTableTargetExist = '''SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = \'''' + nameDbTarget + '''\' AND TABLE_NAME = \'''' + prefixTableTarget + myTable + '''\' ;'''

    query_management_select_t(sqlTableTargetExist, 5, 5)

    # MANAGE RESULT
    if myResultTarget == prefixTableTarget + myTable:
        myAnswerTarget = 'Yes'
    else:
        myAnswerTarget = 'No'

    return myAnswerTarget

    time.sleep(1)


############### FUNCTION: FIND IF A TABLE EXIST IN SOURCE
def isTableExistInSource(myTable):

    sqlTableSourceExist = '''SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = \'''' + nameDbSource + '''\' AND TABLE_NAME = \'''' + prefixTableSource + myTable + '''\' ;'''

    query_management_select_s(sqlTableSourceExist, 5, 5)

    # MANAGE RESULT
    if myResultSource == prefixTableSource + myTable:
        myAnswerSource = 'Yes'
    else:
        myAnswerSource = 'No'

    return myAnswerSource

    time.sleep(1)


############### GET SPECIFIC SOURCE VALUES
print(colored('\nGET SPECIFIC SOURCE VALUES', 'blue'))
conSource = engineSource.connect()

# GET SUPER-USER OF THE SOURCE WEBSITE
sqlSourceSuperId = '''SELECT MIN(user_id) AS super_id FROM ''' + prefixTableSource + '''user_usergroup_map WHERE group_id = 8 ;'''
superIdSource = conSource.execute(text(sqlSourceSuperId)).scalar()

# GET CONTENTS RULES JSON OF THE SOURCE WEBSITE
sqlContentsRulesJsonSource = '''SELECT rules FROM ''' + prefixTableSource + '''assets where name = "com_content" ORDER BY id ASC LIMIT 0,1 ;'''
ContentsRulesJsonSource = conSource.execute(text(sqlContentsRulesJsonSource)).scalar()

# CLOSE CONNECTION
# conSource.close()
# engineSource.dispose()

# DISPLAY SPECIFIC SOURCE VALUES
if superIdSource:
    print(colored('Source super-user id: ' + str(superIdSource) + ' (superIdSource)', 'green'))
    print(colored('Source contents rules json: ' + str(ContentsRulesJsonSource) + ' (ContentsRulesJsonSource)', 'green'))
else:
    print(colored('Warning : we can not get the source super-user id!', 'yellow'))


############### GET SPECIFIC TARGET VALUES
print(colored('\nGET SPECIFIC TARGET VALUES', 'blue'))
conTarget = engineTarget.connect()

# GET SUPER-USER OF THE TARGET WEBSITE
sqlTargetSuperId = '''SELECT MIN(user_id) AS super_id FROM ''' + prefixTableTarget + '''user_usergroup_map WHERE group_id = 8 ;'''
superIdTarget = conTarget.execute(text(sqlTargetSuperId)).scalar()

# GET WORKFLOW BASIC STAGE ID OF THE TARGET WEBSITE
sqlTargetBasicStageId = '''SELECT MIN(id) AS id_workflow_stages FROM ''' + prefixTableTarget + '''workflow_stages WHERE title LIKE "COM_WORKFLOW_BASIC_STAGE" ;'''
BasicStageIdTarget = conTarget.execute(text(sqlTargetBasicStageId)).scalar()

# GET ARTICLES TYPE ID OF THE TARGET WEBSITE
sqlTargetArticlesTypeId = '''SELECT MIN(type_id) AS type_id_content FROM ''' + prefixTableTarget + '''content_types WHERE type_alias LIKE "com_content.article" AND type_title LIKE "Article" ;'''
ArticlesTypeIdTarget = conTarget.execute(text(sqlTargetArticlesTypeId)).scalar()

# GET CONTENT ASSET ID OF THE TARGET WEBSITE
sqlTargetContentAssetId = '''SELECT MIN(id) AS id_content FROM ''' + prefixTableTarget + '''assets WHERE name LIKE "com_content" ;'''
ContentAssetIdTarget = conTarget.execute(text(sqlTargetContentAssetId)).scalar()

# CLOSE CONNECTION
# conTarget.close()
# engineTarget.dispose()

# DISPLAY SPECIFIC TARGET VALUES
if BasicStageIdTarget:
    pass
else:
    print(colored('Warning : we can not get the target workflow basic stage id!', 'red'))
    sys.exit()

if ArticlesTypeIdTarget:
    pass
else:
    print(colored('Warning : we can not get the target article type id!', 'red'))
    sys.exit()

if superIdTarget:
    print(colored('Target super-user id: ' + str(superIdTarget) + ' (superIdTarget)', 'green'))
    print(colored('Target workflow basic stage id: ' + str(BasicStageIdTarget) + ' (BasicStageIdTarget)', 'green'))
    print(colored('Target articles type id: ' + str(ArticlesTypeIdTarget) + ' (ArticlesTypeIdTarget)', 'green'))
    print(colored('Target content asset id: ' + str(ContentAssetIdTarget) + ' (ContentAssetIdTarget)', 'green'))
else:
    print(colored('Warning : we can not get the target super-user id!', 'yellow'))


############### RUN STEP SCRIPTS
print(colored('\nDATA MIGRATION IN PROGRESS...', 'blue'))

# GET SCRIPT STEPS
script_directory = os.path.dirname(os.path.abspath(__file__))
dirStepScripts = os.path.join(script_directory, 'transferts')
listStepScripts = sorted(os.listdir(dirStepScripts), reverse=False)

# print(listStepScripts)

# LOOP ON SCRIPTS FROM DIRECTORY TRANSFERTS
for stepScript in listStepScripts:

    # EXECUTE SCRIPTS
    pathStepScript = dirStepScripts + '\\' + stepScript
    exec(compile(open(pathStepScript, 'rb').read(), pathStepScript, 'exec'))

    time.sleep(5)


print(colored('\nEND OF DATA MIGRATION!', 'green'))

print(colored('Please:', 'yellow'))
print(colored('- Repatriate the listed images and attachments.', 'yellow'))
print(colored('- Clear cache from the Joomla administration.', 'yellow'))
print(colored('- Update the database structure from the Joomla administration.', 'yellow'))
print(colored('- Unlock the database from the Joomla administration.', 'yellow'))

time.sleep(1)


# CLOSE EXISTING CONNECTIONS
try:
    conTarget.close()
except:
    pass

try:
    conSource.close()
except:
    pass

try:
    engineTarget.dispose()
except:
    pass

try:
    engineSource.dispose()
except:
    pass
