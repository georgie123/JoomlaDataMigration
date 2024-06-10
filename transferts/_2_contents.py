
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

# CLEAN OR EMPTY TARGET TABLES
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

                # REMOVE ASSET_ID FIELD FOR #_CATEGORIES
                if t == 'categories':
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


################ FIELD PARENT_ID IN TABLE #_ASSETS FOR ARTICLES
print(colored('\nField parent_id in table #_assets for articles', 'blue'))
sqlTargetAssetsParentIdFix = '''SELECT CONCAT("UPDATE ''' + prefixTableTarget + '''assets SET parent_id = ", ''' + prefixTableTarget + '''assets_2.id, " WHERE id = ", ''' + prefixTableTarget + '''assets_1.id, " ;") AS my_query FROM ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_1 LEFT JOIN ''' + prefixTableTarget + '''content ON ''' + prefixTableTarget + '''assets_1.name = CONCAT("com_content.article.", ''' + prefixTableTarget + '''content.id) LEFT JOIN ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_2 ON ''' + prefixTableTarget + '''content.catid = REPLACE(''' + prefixTableTarget + '''assets_2.name, "com_content.category.", "") WHERE ''' + prefixTableTarget + '''assets_1.name LIKE "com_content.article.%%" ;'''
dfTargetAssetsParentIdFix = pd.read_sql_query(sqlTargetAssetsParentIdFix, engineTarget)

# print('\ndfTargetAssetsParentIdFix:')
# print(tab(dfTargetAssetsParentIdFix.head(10), headers='keys', tablefmt='psql', showindex=False))
# print(dfTargetAssetsParentIdFix.shape[0])

my_session_target_assets_parent_id_fix = sessionmaker(bind=engineTarget)
sessionTargetAssetsParentIdFix = my_session_target_assets_parent_id_fix()
sessionTargetAssetsParentIdFix.begin()
for index, row in dfTargetAssetsParentIdFix.iterrows():
    myQuery = row['my_query']
    sessionTargetAssetsParentIdFix.execute(text(myQuery))
    # print(myQuery)

sessionTargetAssetsParentIdFix.commit()
sessionTargetAssetsParentIdFix.close()
print(colored('The field parent_id has been fixed in table #_assets for articles.', 'green'))


################ FIELD PARENT_ID IN TABLE #_ASSETS FOR CATEGORIES
print(colored('\nField parent_id in table #_assets for categories', 'blue'))
sqlTargetAssetsCatParentIdFix = '''SELECT IF(''' + prefixTableTarget + '''assets_2.id IS NULL, CONCAT("UPDATE ''' + prefixTableTarget + '''assets_2 SET parent_id = ''' + str(ContentAssetIdTarget) + '''  WHERE id = ", ''' + prefixTableTarget + '''assets_1.id, " ;"), CONCAT("UPDATE ''' + prefixTableTarget + '''assets_2 SET parent_id = ", ''' + prefixTableTarget + '''assets_2.id, " WHERE id = ", ''' + prefixTableTarget + '''assets_1.id, " ;")) AS my_query FROM ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_1 LEFT JOIN ''' + prefixTableTarget + '''categories ON ''' + prefixTableTarget + '''assets_1.name = CONCAT("com_content.category.", ''' + prefixTableTarget + '''categories.id) LEFT JOIN ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_2 ON ''' + prefixTableTarget + '''categories.parent_id = REPLACE(''' + prefixTableTarget + '''assets_2.name, "com_content.category.", "") WHERE ''' + prefixTableTarget + '''assets_1.name LIKE "com_content.category.%" ;'''
dfTargetAssetsCatParentIdFix = pd.read_sql_query(sqlTargetAssetsCatParentIdFix, engineTarget)

# print('\ndfTargetAssetsCatParentIdFix:')
# print(tab(dfTargetAssetsCatParentIdFix.head(10), headers='keys', tablefmt='psql', showindex=False))
# print(dfTargetAssetsCatParentIdFix.shape[0])

my_session_target_assets_cat_parent_id_fix = sessionmaker(bind=engineTarget)
sessionTargetAssetsCatParentIdFix = my_session_target_assets_cat_parent_id_fix()
sessionTargetAssetsCatParentIdFix.begin()
for index, row in dfTargetAssetsCatParentIdFix.iterrows():
    myQuery = row['my_query']
    sessionTargetAssetsCatParentIdFix.execute(text(myQuery))
    # print(myQuery)

sessionTargetAssetsCatParentIdFix.commit()
sessionTargetAssetsCatParentIdFix.close()
print(colored('The field parent_id has been fixed in table #_assets for categories.', 'green'))


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
print(colored('\nFields text in table #_content', 'blue'))
my_session_text_content = sessionmaker(bind=engineTarget)
sessionTextContent = my_session_text_content()
session.begin()

sessionTextContent.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, \'''' + domainSource + '''\', \'''' + domainTarget + ''''), `fulltext` = REPLACE(`fulltext`, \'''' + domainSource + '''', \'''' + domainTarget + '''\') ;'''))
sessionTextContent.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), introtext = REPLACE(introtext, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/'), `fulltext` = REPLACE(`fulltext`, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), `fulltext` = REPLACE(`fulltext`, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''))
print(colored('Links to the website has been fixed in the text fields of target table #_content (but not images).', 'green'))

sessionTextContent.commit()
sessionTextContent.close()


################ FIX URL REWRITE
sqlTargetTextUrlFix = '''SELECT CONCAT('UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, "/', id, '-', alias, '", "/', alias, '") ;') AS query1, CONCAT('UPDATE ''' + prefixTableTarget + '''content SET `fulltext` = REPLACE(`fulltext`, "/', id, '-', alias, '", "/', alias, '") ;') AS query2 FROM ''' + prefixTableTarget + '''content ;'''
dfTargetTextUrlFix = pd.read_sql_query(sqlTargetTextUrlFix, engineTarget)

# print('\ndfTargetTextUrlFix:')
# print(tab(dfTargetTextUrlFix.head(10), headers='keys', tablefmt='psql', showindex=False))
# print(dfTargetTextUrlFix.shape[0])

my_session_urlrewrite_content = sessionmaker(bind=engineTarget)
sessionUrlRewriteContent = my_session_urlrewrite_content()
sessionUrlRewriteContent.begin()
for index, row in dfTargetTextUrlFix.iterrows():

    myQuery1 = row['query1']
    myQuery2 = row['query2']

    sessionUrlRewriteContent.execute(text(myQuery1))
    sessionUrlRewriteContent.execute(text(myQuery2))

    # print(myQuery1)
    # print(myQuery2)

sessionUrlRewriteContent.commit()
sessionUrlRewriteContent.close()
print(colored('The URL rewrite links has been fixed in the text fields of target table #_content (removing id from URL articles).', 'green'))


################ UPDATE TEXT FIELDS FOR #_UCM_CONTENT
print(colored('\nFields text in table #_ucm_content', 'blue'))
my_session_text_ucm = sessionmaker(bind=engineTarget)
sessionTextUcm = my_session_text_ucm()
sessionTextUcm.begin()

sessionTextUcm.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, \'''' + domainSource + '''\', \'''' + domainTarget + '''') ;'''))
sessionTextUcm.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), core_body = REPLACE(core_body, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''))
print(colored('Links to the website has been fixed in the field core_body of target table #_ucm_content (but not images).', 'green'))

sessionTextUcm.commit()
sessionTextUcm.close()

################ FIX URL REWRITE
sqlTargetTextUrlFix = '''SELECT CONCAT('UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, "/', id, '-', alias, '", "/', alias, '") ;') AS query1 FROM ''' + prefixTableTarget + '''content ;'''
dfTargetTextUrlFix = pd.read_sql_query(sqlTargetTextUrlFix, engineTarget)

# print('\ndfTargetTextUrlFix:')
# print(tab(dfTargetTextUrlFix.head(10), headers='keys', tablefmt='psql', showindex=False))
# print(dfTargetTextUrlFix.shape[0])

my_session_urlrewrite_ucm_content = sessionmaker(bind=engineTarget)
sessionUrlRewriteUcmContent = my_session_urlrewrite_ucm_content()
sessionUrlRewriteUcmContent.begin()
for index, row in dfTargetTextUrlFix.iterrows():

    myQuery1 = row['query1']

    sessionUrlRewriteUcmContent.execute(text(myQuery1))

    # print(myQuery1)

sessionUrlRewriteUcmContent.commit()
sessionUrlRewriteUcmContent.close()

print(colored('The URL rewrite links has been fixed in the text fields of target table #_ucm_content (removing id from URL articles).', 'green'))
