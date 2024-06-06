
import os
from termcolor import colored

print(colored('\nJOOMLA DATA MIGRATION', 'blue'))
import _params
from _params import hostTarget, portDbTarget, userDbTarget, nameDbTarget, pwdDbTarget, prefixTableTarget, domainTarget
from _params import hostSource, portDbSource, userDbSource, nameDbSource, pwdDbSource, prefixTableSource, domainSource

from sqlalchemy import create_engine, text

engineTarget = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbTarget, pwdDbTarget, hostTarget, portDbTarget, nameDbTarget))
engineSource = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbSource, pwdDbSource, hostSource, portDbSource, nameDbSource))


############### GET SPECIFIC TARGET IDS
print(colored('\nGET SPECIFIC TARGET IDS', 'blue'))
conTarget = engineTarget.connect()

# GET SUPER-USER OF THE TARGET WEBSITE
sqlTargetSuperId = '''SELECT user_id AS super_id FROM ''' + prefixTableTarget + '''user_usergroup_map WHERE group_id = (SELECT MIN(id) AS super_group FROM ''' + prefixTableTarget + '''usergroups WHERE title LIKE "Super Users") ;'''
superIdTarget = conTarget.execute(text(sqlTargetSuperId)).scalar()

# GET WORKFLOW BASIC STAGE ID OF THE TARGET WEBSITE
conTarget = engineTarget.connect()
sqlTargetBasicStageId = '''SELECT id FROM ''' + prefixTableTarget + '''workflow_stages WHERE title LIKE "COM_WORKFLOW_BASIC_STAGE" ;'''
BasicStageIdTarget = conTarget.execute(text(sqlTargetBasicStageId)).scalar()

# GET ARTICLES TYPE ID OF THE TARGET WEBSITE
conTarget = engineTarget.connect()
sqlTargetArticlesTypeId = '''SELECT type_id FROM ''' + prefixTableTarget + '''content_types WHERE type_alias LIKE "com_content.article" AND type_title LIKE "Article" ;'''
ArticlesTypeIdTarget = conTarget.execute(text(sqlTargetArticlesTypeId)).scalar()

# CLOSE CONNECTION
conTarget.close()
engineTarget.dispose()

# DISPLAY SPECIFIC TARGET IDS
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
else:
    print(colored('Warning : we can not get the target super-user id!', 'red'))
    sys.exit()


############### RUN STEP SCRIPTS
print(colored('\nDATA MIGRATION IN PROGRESS...', 'blue'))

# GET SCRIPT STEPS
script_directory = os.path.dirname(os.path.abspath(__file__))
dirStepScripts = os.path.join(script_directory, 'transferts')
listStepScripts = os.listdir(dirStepScripts)

# LOOP ON SCRIPTS FROM DIRECTORY TRANSFERTS
for stepScript in listStepScripts:

    # EXECUTE SCRIPTS
    pathStepScript = dirStepScripts + '\\' + stepScript
    exec(compile(open(pathStepScript, 'rb').read(), pathStepScript, 'exec'))

