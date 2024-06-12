
import os
from termcolor import colored
import sys
import pandas as pd

import _params
from _params import hostTarget, portDbTarget, userDbTarget, nameDbTarget, pwdDbTarget, prefixTableTarget, domainTarget
from _params import hostSource, portDbSource, userDbSource, nameDbSource, pwdDbSource, prefixTableSource, domainSource

from sqlalchemy import create_engine, text

engineTarget = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbTarget, pwdDbTarget, hostTarget, portDbTarget, nameDbTarget))
engineSource = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbSource, pwdDbSource, hostSource, portDbSource, nameDbSource))


############### FUNCTION: ESCAPE SOME CHAR DURING SQL TRANSFERT
def escape_value(value):
    value_str = str(value)
    value_str = value_str.replace("\\", "\\\\")
    value_str = value_str.replace("'", "\\'")
    value_str = value_str.replace(":", "\\:")

    return value_str


############### FUNCTION: FIND IF A TABLE EXIST IN TARGET
def isTableExistInTarget(myTable):
    sqlTableTargetExist = '''SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = \'''' + nameDbTarget + '''\' AND TABLE_NAME = \'''' + prefixTableTarget + myTable + '''\' ;'''
    conTarget = engineTarget.connect()
    myResult = conTarget.execute(text(sqlTableTargetExist)).scalar()

    # CLOSE CONNECTION
    conTarget.close()
    engineTarget.dispose()

    # MANAGE RESULT
    if myResult == prefixTableTarget + myTable:
        myAnswer = 'Yes'
    else:
        myAnswer = 'No'

    return myAnswer


############### FUNCTION: FIND IF A TABLE EXIST IN SOURCE
def isTableExistInSource(myTable):
    sqlTableSourceExist = '''SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = \'''' + nameDbSource + '''\' AND TABLE_NAME = \'''' + prefixTableSource + myTable + '''\' ;'''
    conSource = engineSource.connect()
    myResult = conSource.execute(text(sqlTableSourceExist)).scalar()

    # CLOSE CONNECTION
    conSource.close()
    engineSource.dispose()

    # MANAGE RESULT
    if myResult == prefixTableSource + myTable:
        myAnswer = 'Yes'
    else:
        myAnswer = 'No'

    return myAnswer


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
conSource.close()
engineSource.dispose()

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
conTarget.close()
engineTarget.dispose()

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


print(colored('\nEND OF DATA MIGRATION!', 'green'))

conTarget.close()
engineTarget.dispose()

engineSource.dispose()
