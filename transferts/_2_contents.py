
from sqlalchemy.orm import sessionmaker

contentsTables = [
    'content', 'categories', 'contentitem_tag_map', 'tags',
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

    elif t == 'categories':
        SessionCleanTarget.execute(text('''DELETE FROM ''' + prefixTableTarget + t + ''' WHERE extension LIKE "com_content" ;'''))
        print(colored('The target table #_' + t + ' has been cleaned.', 'green'))

    else:
        SessionCleanTarget.execute(text('''TRUNCATE ''' + prefixTableTarget + t + ''' ;'''))
        print(colored('The target table #_' + t + ' has been emptied.', 'green'))

SessionCleanTarget.commit()
SessionCleanTarget.close()


################ IMPORTING
print('\nImporting...')

for t in contentsTables:

    # TO NOT TRANSFERT TABLE #_WORKFLOW_ASSOCIATION
    if t == 'workflow_associations':
        continue

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
                # print('Target fields #_' + t + ' (' + str(len(listTargetFields)) + '):', listTargetFields)
                # print('Source fields #_' + t + ' (' + str(len(listSourceFields)) + '):', listSourceFields)
                # print('Commons fields (' + str(len(CommonsFieldsList)) + '):', CommonsFieldsList)
                print('Missing fields in target (' + str(len(missingTargetFieldsList)) + '):', missingTargetFieldsList)
                print('Missing fields in source (' + str(len(missingSourceFieldsList)) + '):', missingSourceFieldsList,
                      '\n')

            if len(CommonsFieldsList) > 0:
                print('Tables #_' + t + ' working...')

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

                # QUERY TO GET DATA FROM SOURCE
                listFields = str(CommonsFieldsList).replace('[', '').replace(']', '').replace("'", '').replace('"', '').replace('fulltext', '`fulltext`')

                # TABLE #_CATEGORIES
                if t == 'categories':
                    sqlSourceContent = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE extension LIKE "com_content" ;'''
                    print('Using: WHERE extension LIKE "com_content" ;')

                # TABLE #_ASSETS
                elif t == 'assets':
                    sqlSourceContent = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE name LIKE "com_content.article.%%" OR name LIKE "com_content.category.%%" OR name LIKE "#__ucm_content.%%" ;'''
                    print('Using: WHERE name LIKE "com_content.article.%" OR name LIKE "com_content.category.%" OR name LIKE "#__ucm_content.%" ;')

                # TABLE #_TAGS
                elif t == 'tags':
                    sqlSourceContent = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE id > 1 ;'''
                    print('Using: WHERE id > 1')

                # TABLE #_CONTENTITEM_TAG_MAP
                elif t == 'contentitem_tag_map':
                    sqlSourceContent = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE type_alias = "com_content.article" ;'''
                    print('Using: WHERE type_alias = "com_content.article"')

                # TABLE #_UCM_CONTENT
                elif t == 'ucm_content':
                    sqlSourceContent = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' WHERE core_type_alias = "com_content.article" ;'''
                    print('Using: WHERE core_type_alias = "com_content.article"')

                else:
                    sqlSourceContent = '''SELECT ''' + listFields + ''' FROM ''' + prefixTableSource + t + ''' ;'''

                dfSource = pd.read_sql_query(sqlSourceContent, engineSource)

                # LIST ID FROM #_CONTENT
                if t == 'content':
                    listMigratedArticles = dfSource['id'].drop_duplicates().dropna().sort_values(ascending=True).tolist()

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


################ INSERTS IN #_WORKFLOW_ASSOCIATIONS FOR ALL ARTICLES
print(colored('\nTable #_workflow_associations', 'blue'))

my_session_workflow_associations = sessionmaker(bind=engineTarget)
sessionWorkflowAssociations = my_session_workflow_associations()
sessionWorkflowAssociations.begin()

for a in listMigratedArticles:
    queryInsert = '''INSERT INTO ''' + prefixTableTarget + f'''workflow_associations (item_id, stage_id, extension) VALUES (''' + str(a) + ''', ''' + str(BasicStageIdTarget) + ''', "com_content.article") ;'''
    # print(queryInsert)

    sessionWorkflowAssociations.execute(text(queryInsert))

sessionWorkflowAssociations.commit()
sessionWorkflowAssociations.close()
print(colored(str(len(listMigratedArticles)) + ' insert(s) in target table #_workflow_associations.', 'green'))
print('Using: listMigratedArticles')


################ FIELD ASSET_ID IN TABLE #_CONTENT
print(colored('\nField asset_id in table #_content', 'blue'))
my_session_content_assets = sessionmaker(bind=engineTarget)
sessionContentAssets = my_session_content_assets()
sessionContentAssets.begin()

sessionContentAssets.execute(text('''UPDATE ''' + prefixTableTarget + '''content, ''' + prefixTableTarget + '''assets SET ''' + prefixTableTarget + '''content.asset_id = ''' + prefixTableTarget + '''assets.id WHERE CONCAT("com_content.article.", ''' + prefixTableTarget + '''content.id) = ''' + prefixTableTarget + '''assets.name AND ''' + prefixTableTarget + '''content.asset_id = 0 ;'''))
print(colored('The asset_id field in target table #_content has been fixed.', 'green'))

sessionContentAssets.commit()
sessionContentAssets.close()


################ FIELD ASSET_ID IN TABLE #_CATEGORIES
print(colored('\nField asset_id in table #_categories', 'blue'))
my_session_categories_assets = sessionmaker(bind=engineTarget)
sessionCategoriesAssets = my_session_categories_assets()
sessionCategoriesAssets.begin()

sessionCategoriesAssets.execute(text('''UPDATE ''' + prefixTableTarget + '''categories, ''' + prefixTableTarget + '''assets SET ''' + prefixTableTarget + '''categories.asset_id = ''' + prefixTableTarget + '''assets.id WHERE CONCAT("com_content.category.", ''' + prefixTableTarget + '''categories.id) = ''' + prefixTableTarget + '''assets.name AND ''' + prefixTableTarget + '''categories.asset_id = 0 ;'''))
print(colored('The asset_id field in target table #_categories has been fixed.', 'green'))

sessionCategoriesAssets.commit()
sessionCategoriesAssets.close()


################ FIELD ASSET_ID IN TABLE #_UCM_CONTENT
print(colored('\nField asset_id in table #_ucm_content', 'blue'))
my_session_ucm_content_assets = sessionmaker(bind=engineTarget)
sessionUcmContentAssets = my_session_ucm_content_assets()
sessionUcmContentAssets.begin()

sessionUcmContentAssets.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content, ''' + prefixTableTarget + '''content SET ''' + prefixTableTarget + '''ucm_content.asset_id = ''' + prefixTableTarget + '''content.asset_id WHERE ''' + prefixTableTarget + '''ucm_content.core_content_item_id = ''' + prefixTableTarget + '''content.id ;'''))
print(colored('The asset_id field in target table #_ucm_content has been fixed.', 'green'))

sessionUcmContentAssets.commit()
sessionUcmContentAssets.close()


################ FIELD TYPE_ID IN TABLE #_CONTENTITEM_TAG_MAP
print(colored('\nField type_id in table #_contentitem_tag_map', 'blue'))
my_session_contentitem_tag_map_type = sessionmaker(bind=engineTarget)
sessionContentItemTagMapType = my_session_contentitem_tag_map_type()
sessionContentItemTagMapType.begin()

sessionContentItemTagMapType.execute(text('''UPDATE ''' + prefixTableTarget + '''contentitem_tag_map SET type_id = ''' + str(ArticlesTypeIdTarget) + ''' WHERE type_alias LIKE "com_content.article" ;'''))
print(colored('The type_id field in target table #_contentitem_tag_map has been fixed.', 'green'))

sessionContentItemTagMapType.commit()
sessionContentItemTagMapType.close()


################ FIELD TYPE_ID IN TABLE #_UCM_CONTENT
print(colored('\nField type_id in table #_ucm_content', 'blue'))
my_session_ucm_content_type = sessionmaker(bind=engineTarget)
sessionUcmContentType = my_session_ucm_content_type()
sessionUcmContentType.begin()

sessionUcmContentType.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_type_id = ''' + str(ArticlesTypeIdTarget) + ''' WHERE core_type_alias LIKE "com_content.article" ;'''))
print(colored('The type_id field in target table #_ucm_content has been fixed.', 'green'))

sessionUcmContentType.commit()
sessionUcmContentType.close()



################ UPDATE TEXT FIELDS FOR #_CONTENT
################ UPDATE TEXT FIELD FOR #_UCM_CONTENT