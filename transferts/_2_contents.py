
from sqlalchemy.orm import sessionmaker

contentsTables = [
    'content', 'contentitem_tag_map', 'categories', 'tags',
    'ucm_content', 'assets',
    'workflow_associations', 'history'
]

print(colored('\nCONTENTS DATA', 'blue'))

################ CLEANING
print('Cleaning...')

mySessionCleanTarget = sessionmaker(bind=engineTarget)
SessionCleanTarget = mySessionCleanTarget()
SessionCleanTarget.begin()

# CLEAN TARGET TABLES
for t in contentsTables:
    if t == 'assets':
        SessionCleanTarget.execute(text('''DELETE FROM ''' + prefixTableTarget + t + ''' WHERE name LIKE "com_content.article.%" OR name LIKE "com_content.category.%" OR name LIKE "#__ucm_content.%" ;'''))
        print(colored('The target table #_' + t + ' has been cleaned.', 'green'))

    elif t == 'workflow_associations':
        SessionCleanTarget.execute(text('''DELETE FROM ''' + prefixTableTarget + t + ''' WHERE extension LIKE "com_content.article" ;'''))
        print(colored('The target table #_' + t + ' has been cleaned.', 'green'))

    elif t == 'tags':
        SessionCleanTarget.execute(text('''DELETE FROM ''' + prefixTableTarget + t + ''' WHERE id > 1 ;'''))
        print(colored('The target table #_' + t + ' has been cleaned.', 'green'))

    else:
        SessionCleanTarget.execute(text('''TRUNCATE ''' + prefixTableTarget + t + ''' ;'''))
        print(colored('The target table #_' + t + ' has been emptied.', 'green'))

SessionCleanTarget.commit()
SessionCleanTarget.close()




################ IMPORTING
print('\nImporting...')

for t in contentsTables:

    print(colored('\nTable #_' + t, 'blue'))

    if isTableExistInTarget(t) == 'Yes':
        print('The table exists in target DB.')

        if isTableExistInSource(t) == 'Yes':
            print('The table exists in source DB.')

            # IF BOTH CONDITIONS ARE SATISFIED
            # GET FIELDS OF TARGET TABLE
            sqlTargetFields = '''SELECT column_name FROM information_schema.COLUMNS WHERE table_schema = \'''' + nameDbTarget + '''\' AND TABLE_NAME = \'''' + prefixTableTarget + t + '''\' ;'''
            dfTargetFields = pd.read_sql_query(sqlTargetFields, engineTarget)
            listTargetFields = dfTargetFields['column_name'].tolist()

            # print('\nFields of target table:')
            # print(tab(dfTargetFields.head(10), headers='keys', tablefmt='psql', showindex=False))
            # print(dfTargetFields.shape[0])
            # print(listTargetFields)

            # GET FIELDS OF SOURCE TABLE
            sqlSourceFields = '''SELECT column_name FROM information_schema.COLUMNS WHERE table_schema = \'''' + nameDbSource + '''\' AND TABLE_NAME = \'''' + prefixTableSource + t + '''\' ;'''
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
                # print('Target fields #_users (' + str(len(listTargetFields)) + '):', listTargetFields)
                # print('Source fields #_users (' + str(len(listSourceFields)) + '):', listSourceFields)
                # print('Commons fields (' + str(len(CommonsFieldsList)) + '):', CommonsFieldsList)
                print('Missing fields in target (' + str(len(missingTargetFieldsList)) + '):', missingTargetFieldsList)
                print('Missing fields in source (' + str(len(missingSourceFieldsList)) + '):', missingSourceFieldsList,
                      '\n')

            if len(CommonsFieldsList) > 0:
                print('Tables #_' + t + ' working...')

                # GET DATA FROM SOURCE

                # REMOVE ASSET_ID FIELD FOR #_CONTENT
                if t == 'content':
                    CommonsFieldsList.remove('asset_id')
                    print('Field asset_id removed (will be fill later).')

                # REMOVE ASSET_ID FIELD FOR #_ASSETS
                elif t == 'assets':
                    CommonsFieldsList.remove('id')
                    print('Field id removed (to use the target id without deletion).')

                else:
                    pass

                # QUERY TO GET DATA
                listFields = str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", '').replace('"', '').replace('fulltext', '`fulltext`')

                # TABLE #_ASSETS
                if t == 'assets':
                    sqlSourceUsers = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE name LIKE "com_content.article.%%" OR name LIKE "com_content.category.%%" OR name LIKE "#__ucm_content.%%" ;'''
                    print('WHERE name LIKE "com_content.article.%" OR name LIKE "com_content.category.%" OR name LIKE "#__ucm_content.%" ;')

                # TABLE #_TAGS
                elif t == 'tags':
                    sqlSourceUsers = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE id > 1 ;'''
                    print('WHERE id > 1')

                # TABLE #_CONTENTITEM_TAG_MAP
                elif t == 'contentitem_tag_map':
                    sqlSourceUsers = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE type_alias = "com_content.article" ;'''
                    print('WHERE type_alias = "com_content.article"')

                # TABLE #_UCM_CONTENT
                elif t == 'ucm_content':
                    sqlSourceUsers = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE core_type_alias = "com_content.article" ;'''
                    print('WHERE core_type_alias = "com_content.article"')

                else:
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

                # UPDATE PUBLISH/DOWN FIELDS IN #_CONTENT AND "_TAGS
                if t == 'content' or t == 'tags':
                    my_session_content = sessionmaker(bind=engineTarget)
                    mySessionContent = my_session_content()
                    mySessionContent.begin()

                    mySessionContent.execute(text('''UPDATE ''' + prefixTableTarget + t + ''' SET publish_up = NULL WHERE publish_up LIKE "0000-00-00 00:00:00" ;'''))
                    mySessionContent.execute(text('''UPDATE ''' + prefixTableTarget + t + ''' SET publish_down = NULL WHERE publish_down LIKE "0000-00-00 00:00:00" ;'''))
                    print(colored('The publish_up/down fields in target table #_' + t + ' has been fixed.', 'green'))

                    mySessionContent.commit()
                    mySessionContent.close()
                else:
                    pass

            else:
                print(colored('\nTables #_' + t + ' do not have common fields!', 'red'))
                sys.exit()


        else:
            print(colored('But the table does not exist in source DB.', 'yellow'))
            pass

    else:
        print(colored('The table does not exist in target DB.', 'yellow'))
        pass


################ UPDATE TEXT FIELDS FOR #_CONTENT
################ INSERTS IN #_WORKFLOW_ASSOCIATIONS FOR ALL ARTICLES
################ UPDATE FIELD ASSET_ID FOR #_CONTENT, #_CATEGORIES AND THEN #_UCM_CONTENT