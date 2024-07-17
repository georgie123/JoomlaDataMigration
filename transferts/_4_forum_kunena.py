
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate as tab
import time

kunenaTables = [
    'kunena_aliases', 'kunena_announcement', 'kunena_categories',
    'kunena_attachments',
    'kunena_ranks', 'kunena_thankyou', 'kunena_topics', 'kunena_sessions',
    'kunena_smileys',
    'kunena_user_read', 'kunena_user_topics',
    'kunena_users', 'kunena_users_banned', 'kunena_user_categories',
    'kunena_messages', 'kunena_messages_text'
]

print(colored('\nKUNENA DATA', 'blue'))

################ CLEANING
print('Cleaning...')

mySessionCleanTarget = sessionmaker(bind=engineTarget)
SessionCleanTarget = mySessionCleanTarget()
SessionCleanTarget.begin()

# EMPTY TARGET TABLES
for t in kunenaTables:

    if isTableExistInTarget(t) == 'Yes':
        SessionCleanTarget.execute(text('''TRUNCATE ''' + prefixTableTarget + t + ''' ;'''))
        print(colored('The target table #_' + t + ' has been emptied.', 'green'))

    else:
        pass

SessionCleanTarget.commit()
time.sleep(1)
# SessionCleanTarget.close()


################ IMPORTING
print('\nImporting...')

for t in kunenaTables:

    my_session_kunena_import = sessionmaker(bind=engineTarget)
    sessionKunenaImport = my_session_kunena_import()
    sessionKunenaImport.begin()

    if t == 'kunena_announcement':
        print(colored('\nTable #_' + t + ' not imported.', 'blue'))

    else:

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

                # FIX FIELDS IF THEIR NAMES CHANGED
                if t == 'kunena_ranks' and 'rank_id' in listSourceFields:
                    print('Fix field names in table ' + t + ' (1)')
                    print('\nSource fields before:\t' + str(listSourceFields).replace('[', '').replace(']', '').replace("'", ''))
                    _listSourceFields = []
                    for c in listSourceFields:
                        c = c.replace('_i', 'I').replace('_m', 'M').replace('_s', 'S').replace('_t', 'T')
                        _listSourceFields.append(c)
                    listSourceFields = _listSourceFields
                    print('Source fields after:\t' + str(listSourceFields).replace('[', '').replace(']', '').replace("'", '') + '\n')

                if t == 'kunena_categories' and 'parent_id' in listSourceFields:
                    print('Fix field names in table ' + t + ' (1)')
                    print('\nSource fields before:\t' + str(listSourceFields).replace('[', '').replace(']', '').replace("'", ''))
                    _listSourceFields = []
                    for c in listSourceFields:
                        c = c.replace('parent_id', 'parentid').replace('pub_access', 'pubAccess').replace('pub_recurse', 'pubRecurse').replace('admin_access', 'adminAccess').replace('admin_recurse', 'adminRecurse').replace('allow_anonymous', 'allowAnonymous').replace('post_anonymous', 'postAnonymous').replace('allow_polls', 'allowPolls').replace('topic_ordering', 'topicOrdering')
                        _listSourceFields.append(c)
                    listSourceFields = _listSourceFields
                    print('Source fields after:\t' + str(listSourceFields).replace('[', '').replace(']', '').replace("'", '') + '\n')


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
                    print('Missing fields in source (' + str(len(missingSourceFieldsList)) + '):', missingSourceFieldsList, '\n')

                if len(CommonsFieldsList) > 0:
                    print('Tables #_' + t + ' working...')

                    # FIX FIELDS IF THEIR NAMES CHANGED
                    if t == 'kunena_ranks' and 'rankId' in CommonsFieldsList:
                        print('Fix field names in table ' + t + ' (2)')
                        print('\nCommon fields before:\t' + str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", ''))
                        _CommonsFieldsList = []
                        for c in CommonsFieldsList:
                            c = c.replace('I', '_i').replace('M', '_m').replace('S', '_s').replace('T', '_t')
                            _CommonsFieldsList.append(c)
                        CommonsFieldsList = _CommonsFieldsList
                        print('Common fields after:\t' + str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", '') + '\n')

                    if t == 'kunena_categories' and 'parentid' in CommonsFieldsList:
                        print('Fix field names in table ' + t + ' (2)')
                        print('\nCommon fields before:\t' + str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", ''))
                        _CommonsFieldsList = []
                        for c in CommonsFieldsList:
                            c = c.replace('parentid', 'parent_id').replace('pubAccess', 'pub_access').replace('pubRecurse', 'pub_recurse').replace('adminAccess', 'admin_access').replace('adminRecurse', 'admin_recurse').replace('allowAnonymous', 'allow_anonymous').replace('postAnonymous', 'post_anonymous').replace('allowPolls', 'allow_polls').replace('topicOrdering', 'topic_ordering')
                            _CommonsFieldsList.append(c)
                        CommonsFieldsList = _CommonsFieldsList
                        print('Common fields after:\t' + str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", '') + '\n')

                    # GET DATA FROM SOURCE
                    listFields = str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", '').replace('"', '').replace('fulltext', '`fulltext`')
                    sqlSourceKunena = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' ;'''
                    dfSource = pd.read_sql_query(sqlSourceKunena, engineSource)

                    # print('\ndfSource:')
                    # print(tab(dfSource.head(10), headers='keys', tablefmt='psql', showindex=False))
                    # print(dfSource.shape[0])

                    # For work in batch
                    batch_queries = []
                    batch_size = 100

                    # SQL TO INSERT IN TARGET
                    for index, row in dfSource.iterrows():
                        columns = ', '.join(row.index)

                        # FIX FIELDS IF THEIR NAMES CHANGED
                        if t == 'kunena_ranks' and 'rank_id' in columns:
                            print('Fix field names in table ' + t + ' (3)')
                            columns = columns.replace('_i', 'I').replace('_m', 'M').replace('_s', 'S').replace('_t', 'T')

                        if t == 'kunena_categories' and 'parent_id' in columns:
                            print('Fix field names in table ' + t + ' (3)')
                            columns = columns.replace('parent_id', 'parentid').replace('pub_access', 'pubAccess').replace('pub_recurse', 'pubRecurse').replace('admin_access', 'adminAccess').replace('admin_recurse', 'adminRecurse').replace('allow_anonymous', 'allowAnonymous').replace('post_anonymous', 'postAnonymous').replace('allow_polls', 'allowPolls').replace('topic_ordering', 'topicOrdering')

                        values = ', '.join(f"'{escape_value(v)}'" for v in row.values)
                        queryInsert = '''INSERT IGNORE INTO ''' + prefixTableTarget + t + f''' ({columns}) VALUES ({values}) ;'''
                        # print(queryInsert)

                        queryInsert = queryInsert.replace('fulltext', '`fulltext`')

                        # Add in batch
                        batch_queries.append(queryInsert)

                        # Run batch
                        if len(batch_queries) == batch_size:
                            # Build batch
                            full_query = ' '.join(batch_queries)
                            sessionKunenaImport.execute(text(full_query))
                            print(str(len(batch_queries)) + ' queries in batch')
                            # Empty batch
                            batch_queries.clear()

                    # Queries out the last batch
                    if batch_queries:
                        full_query = ' '.join(batch_queries)
                        sessionKunenaImport.execute(text(full_query))
                        print(str(len(batch_queries)) + ' queries in last batch')

                    print(colored('OK, ' + str(dfSource.shape[0]) + ' insert(s) in the target table #_' + t + '.', 'green'))

                    sessionKunenaImport.commit()
                    sessionKunenaImport.close()
                    engineTarget.dispose()

                    time.sleep(1)

                else:
                    print(colored('\nTables #_' + t + ' do not have common fields!', 'red'))
                    sys.exit()


            else:
                print(colored('But the table does not exist in source DB.', 'yellow'))
                pass

        else:
            print(colored('The table does not exist in target DB.', 'yellow'))
            pass
