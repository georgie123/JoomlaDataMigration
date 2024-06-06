
import os
from termcolor import colored

import _params
from _params import hostTarget, portDbTarget, userDbTarget, nameDbTarget, pwdDbTarget, prefixTableTarget, domainTarget
from _params import hostSource, portDbSource, userDbSource, nameDbSource, pwdDbSource, prefixTableSource, domainSource

from sqlalchemy import create_engine

engineTarget = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbTarget, pwdDbTarget, hostTarget, portDbTarget, nameDbTarget))
engineSource = create_engine('mysql+mysqldb://%s:%s@%s:%i/%s' % (userDbSource, pwdDbSource, hostSource, portDbSource, nameDbSource))


print(colored('\nData migration in progress...', 'blue'))

# GET SCRIPT STEPS
script_directory = os.path.dirname(os.path.abspath(__file__))
dirStepScripts = os.path.join(script_directory, 'transferts')
listStepScripts = os.listdir(dirStepScripts)

# DISPLAY
for stepScript in listStepScripts:

    # EXECUTE SCRIPTS
    pathStepScript = dirStepScripts + '\\' + stepScript
    exec(compile(open(pathStepScript, 'rb').read(), pathStepScript, 'exec'))
