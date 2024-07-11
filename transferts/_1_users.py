
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate as tab
import time

usersTables = [
    'users', 'user_keys', 'user_notes', 'user_profiles',
    'contact_details', 'messages', 'messages_cfg', 'privacy_consents', 'privacy_requests',
    'usergroups', 'user_usergroup_map', 'viewlevels',
    'session', 'user_mfa', 'action_logs', 'action_logs_users', 'webauthn_credentials'
]

print(colored('\nUSERS DATA', 'blue'))

################ CLEANING
print('Cleaning...')

mySessionCleanTarget = sessionmaker(bind=engineTarget)
SessionCleanTarget = mySessionCleanTarget()
SessionCleanTarget.begin()

# EMPTY TARGET TABLES
for t in usersTables:

    if isTableExistInTarget(t) == 'Yes':
        SessionCleanTarget.execute(text('''TRUNCATE ''' + prefixTableTarget + t + ''' ;'''))
        print(colored('The target table #_' + t + ' has been emptied.', 'green'))

    else:
        pass

SessionCleanTarget.commit()
SessionCleanTarget.close()


################ IMPORTING
print('\nImporting...')

for t in usersTables:

    print(colored('\nTable #_' + t, 'blue'))

    if isTableExistInTarget(t) == 'Yes':
        print('The table exists in target DB.')

        if isTableExistInSource(t) == 'Yes':
            print('The table exists in source DB.')

            # IF BOTH CONDITIONS ARE SATISFIED
            # GET FIELDS OF TARGET TABLE
            sqlTargetFields = '''SELECT column_name AS column_name FROM information_schema.COLUMNS WHERE table_schema = \'''' + nameDbTarget + '''\' AND TABLE_NAME = \'''' + prefixTableTarget + t + '''\' ;'''
            dfTargetFields = pd.read_sql_query(sqlTargetFields, engineTarget)
            listTargetFields = dfTargetFields['column_name'].tolist()

            # print('\nFields of target table:')
            # print(tab(dfTargetFields.head(10), headers='keys', tablefmt='psql', showindex=False))
            # print(dfTargetFields.shape[0])
            # print(listTargetFields)

            # GET FIELDS OF SOURCE TABLE
            sqlSourceFields = '''SELECT column_name AS column_name FROM information_schema.COLUMNS WHERE table_schema = \'''' + nameDbSource + '''\' AND TABLE_NAME = \'''' + prefixTableSource + t + '''\' ;'''
            dfSourceFields = pd.read_sql_query(sqlSourceFields, engineSource)
            listSourceFields = dfSourceFields['column_name'].tolist()

            # print('\nFields of source table:')
            # print(tab(dfSourceFields.head(10), headers='keys', tablefmt='psql', showindex=False))
            # print(dfSourceFields.shape[0])
            # print(listSourceFields)

            # COMPARE FIELDS BETWEEN SOURCE AND TARGET
            CommonsFieldsList = sorted(list(set(listTargetFields) & set(listSourceFields)))
            missingTargetFieldsList = sorted(list(set(listTargetFields) - set(listSourceFields)))
            missingSourceFieldsList = sorted(list(set(listSourceFields) - set(listTargetFields)))

            if len(CommonsFieldsList) == len(listTargetFields) and len(CommonsFieldsList) == len(listSourceFields):
                print(colored('The tables #_' + t + ' have the same fields.', 'green'))
            else:
                print(colored('Warning, the tables #_' + t + ' do not have the same fields:', 'yellow'))
                # print('Target fields #_' + t + ' (' + str(len(listTargetFields)) + '):', listTargetFields)
                # print('Source fields #_' + t + ' (' + str(len(listSourceFields)) + '):', listSourceFields)
                # print('Commons fields (' + str(len(CommonsFieldsList)) + '):', CommonsFieldsList)
                print('Missing fields in target (' + str(len(missingTargetFieldsList)) + '):', missingTargetFieldsList)
                print('Missing fields in source (' + str(len(missingSourceFieldsList)) + '):', missingSourceFieldsList,
                      '\n')

            if len(CommonsFieldsList) > 0:
                print('Tables #_' + t + ' working...')

                # GET DATA FROM SOURCE
                listFields = str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", '').replace('"', '').replace('fulltext', '`fulltext`')
                sqlSourceUsers = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' ;'''
                dfSource = pd.read_sql_query(sqlSourceUsers, engineSource)

                # print('\ndfSource:')
                # print(tab(dfSource.head(10), headers='keys', tablefmt='psql', showindex=False))
                # print(dfSource.shape[0])

                # SQL TO INSERT IN TARGET
                my_session = sessionmaker(bind=engineTarget)
                session = my_session()
                session.begin()
                for index, row in dfSource.iterrows():
                    columns = ', '.join(row.index)
                    values = ', '.join(f"'{escape_value(v)}'" for v in row.values)
                    queryInsert = '''INSERT IGNORE INTO ''' + prefixTableTarget + t + f''' ({columns}) VALUES ({values}) ;'''
                    # print(queryInsert)

                    session.execute(text(queryInsert.replace('fulltext', '`fulltext`')))

                session.commit()
                session.close()

                print(colored('OK, ' + str(dfSource.shape[0]) + ' insert(s) in the target table #_' + t + '.', 'green'))

            else:
                print(colored('\nTables #_' + t + ' do not have common fields!', 'red'))
                sys.exit()


        else:
            print(colored('But the table does not exist in source DB.', 'yellow'))
            pass

    else:
        print(colored('The table does not exist in target DB.', 'yellow'))
        pass

    time.sleep(2)