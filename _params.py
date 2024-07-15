
import os
from termcolor import colored
import sys

print(colored('\nJOOMLA DATA MIGRATION', 'blue'))

############### PYCHARM DIRECTORY
pycharmDirectoryGeorgesSamsungC = r'C:/Users/georg/PycharmProjects/'
pycharmDirectoryGeorgesDesktopD = r'C:/Users/Georges/PycharmProjects/'

############### DECLARE THE PYCHARM DIRECTORY AND FIND THE ACCESS FILE INTO
if os.path.isdir(pycharmDirectoryGeorgesSamsungC):
    print(colored('We work from ' + pycharmDirectoryGeorgesSamsungC, 'green'))
    pycharmDirectory = pycharmDirectoryGeorgesSamsungC
    accessFile = pycharmDirectory + 'gjcY8d4q6mvC2WXy.ztxt'

if os.path.isdir(pycharmDirectoryGeorgesDesktopD):
    print(colored('We work from ' + pycharmDirectoryGeorgesDesktopD, 'green'))
    pycharmDirectory = pycharmDirectoryGeorgesDesktopD
    accessFile = pycharmDirectory + 'gjcY8d4q6mvC2WXy.ztxt'

############### FIND THE ACCESS DATA
file = open(accessFile, 'r', encoding='utf8')
lines = file.readlines()

for num, x in enumerate(lines):
    if x == '>>> TARGET Infomaniak MasterGéomatique J4Binv\n':
        hosterTarget = x.replace('\n', '')
        hostTarget = lines[num+1].replace('host:', '').replace('\n', '')
        portDbTarget = int(lines[num+2].replace('port:', '').replace('\n', ''))
        userDbTarget = lines[num+3].replace('DB user:', '').replace('\n', '')
        pwdDbTarget = lines[num+4].replace('DB password:', '').replace('\n', '')
        nameDbTarget = lines[num+5].replace('DB name:', '').replace('\n', '')
        prefixTableTarget = lines[num+6].replace('Tables prefix:', '').replace('\n', '')
        domainTarget = lines[num+7].replace('Domain:', '').replace('\n', '')

        print('\n' + hosterTarget)
        print('Host target:\t\t\t' + hostTarget)
        print('Port target:\t\t\t' + str(portDbTarget))
        print('User DB target:\t\t\t' + userDbTarget)
        print('Password DB target:\t\t' + pwdDbTarget)
        print('Name DB target:\t\t\t' + nameDbTarget)
        print('Prefix table target:\t' + prefixTableTarget)
        print('Domain target:\t' + domainTarget)

    if x == '>>> SOURCE Infomaniak MasterGéomatique\n':
        hosterSource = x.replace('\n', '')
        hostSource = lines[num + 1].replace('host:', '').replace('\n', '')
        portDbSource = int(lines[num + 2].replace('port:', '').replace('\n', ''))
        userDbSource = lines[num + 3].replace('DB user:', '').replace('\n', '')
        pwdDbSource = lines[num + 4].replace('DB password:', '').replace('\n', '')
        nameDbSource = lines[num + 5].replace('DB name:', '').replace('\n', '')
        prefixTableSource = lines[num + 6].replace('Tables prefix:', '').replace('\n', '')
        domainSource = lines[num+7].replace('Domain:', '').replace('\n', '')

        print('\n' + hosterSource)
        print('Host source:\t\t\t' + hostSource)
        print('Port source:\t\t\t' + str(portDbSource))
        print('User DB source:\t\t\t' + userDbSource)
        print('Password DB source:\t\t' + pwdDbSource)
        print('Name DB source:\t\t\t' + nameDbSource)
        print('Prefix table source:\t' + prefixTableSource)
        print('Domain source:\t' + domainSource)
