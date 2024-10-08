
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate as tab
import time

contentsTables = [
    'categories', 'tags', 'contentitem_tag_map',
    'content_rating', 'content_frontpage',
    'ucm_base', 'assets', 'content', 'ucm_content',
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

    if t == 'workflow_associations' or t == 'history':
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

                    # For work in batch
                    batch_queries = []
                    batch_size = 100

                    # SQL TO INSERT IN TARGET
                    my_session = sessionmaker(bind=engineTarget)
                    session = my_session()
                    session.begin()

                    for index, row in dfSource.iterrows():
                        columns = ', '.join(row.index)
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
                            session.execute(text(full_query))
                            print(str(len(batch_queries)) + ' queries in batch')
                            # Empty batch
                            batch_queries.clear()

                    # Queries out the last batch
                    if batch_queries:
                        full_query = ' '.join(batch_queries)
                        session.execute(text(full_query))
                        print(str(len(batch_queries)) + ' queries in last batch')

                    print(colored('OK, ' + str(dfSource.shape[0]) + ' insert(s) in the target table #_' + t + '.', 'green'))

                    session.commit()
                    session.close()

                    time.sleep(1)

                    # UPDATE PUBLISH/DOWN FIELDS IN #_CONTENT AND "_TAGS
                    if t == 'content' or t == 'tags':
                        my_session_content_publish = sessionmaker(bind=engineTarget)
                        mySessionContentPublish = my_session_content_publish()
                        mySessionContentPublish.begin()

                        mySessionContentPublish.execute(text('''UPDATE ''' + prefixTableTarget + t + ''' SET publish_up = NULL WHERE publish_up LIKE "0000-00-00 00:00:00" ;'''))
                        mySessionContentPublish.execute(text('''UPDATE ''' + prefixTableTarget + t + ''' SET publish_down = NULL WHERE publish_down LIKE "0000-00-00 00:00:00" ;'''))
                        print(colored('The publish_up/down fields in target table #_' + t + ' has been fixed.', 'green'))

                        mySessionContentPublish.commit()
                        mySessionContentPublish.close()

                    else:
                        pass

                    # UPDATE CHECKED FIELDS IN #_CONTENT, #_CATEGORIES AND "_TAGS
                    if t == 'content' or t == 'categories' or t == 'tags':
                        my_session_content_checked = sessionmaker(bind=engineTarget)
                        mySessionContentChecked = my_session_content_checked()
                        mySessionContentChecked.begin()

                        mySessionContentChecked.execute(text('''UPDATE ''' + prefixTableTarget + t + ''' SET checked_out = NULL ;'''))
                        mySessionContentChecked.execute(text('''UPDATE ''' + prefixTableTarget + t + ''' SET checked_out_time = NULL ;'''))
                        print(colored('The checked fields in target table #_' + t + ' has been fixed.', 'green'))

                        mySessionContentChecked.commit()
                        mySessionContentChecked.close()

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

        time.sleep(2)


################ TARGET INSERTS IN #_WORKFLOW_ASSOCIATIONS FOR ALL ARTICLES
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


################ TARGET FIELD RULES IN TABLE #_ASSETS
print(colored('\nField rules in table #_assets (for content)', 'blue'))
my_session_assets_content_rules = sessionmaker(bind=engineTarget)
sessionAssetsContentRules = my_session_assets_content_rules()
sessionAssetsContentRules.begin()

sessionAssetsContentRules.execute(text('''UPDATE ''' + prefixTableTarget + '''assets SET rules = \'''' + escape_value(ContentsRulesJsonSource) + '''\' WHERE name = "com_content" ;'''))
print(colored('The rules field in target table #_assets has been fixed.', 'green'))

sessionAssetsContentRules.commit()
sessionAssetsContentRules.close()


################ TARGET FIELD ASSET_ID IN TABLE #_CONTENT
print(colored('\nField asset_id in table #_content', 'blue'))
my_session_content_assets = sessionmaker(bind=engineTarget)
sessionContentAssets = my_session_content_assets()
sessionContentAssets.begin()

sessionContentAssets.execute(text('''UPDATE ''' + prefixTableTarget + '''content, ''' + prefixTableTarget + '''assets SET ''' + prefixTableTarget + '''content.asset_id = ''' + prefixTableTarget + '''assets.id WHERE CONCAT("com_content.article.", ''' + prefixTableTarget + '''content.id) = ''' + prefixTableTarget + '''assets.name AND ''' + prefixTableTarget + '''content.asset_id = 0 ;'''))
print(colored('The asset_id field in target table #_content has been fixed.', 'green'))

sessionContentAssets.commit()
sessionContentAssets.close()


################ TARGET FIELD ASSET_ID IN TABLE #_CATEGORIES
print(colored('\nField asset_id in table #_categories', 'blue'))
my_session_categories_assets = sessionmaker(bind=engineTarget)
sessionCategoriesAssets = my_session_categories_assets()
sessionCategoriesAssets.begin()

sessionCategoriesAssets.execute(text('''UPDATE ''' + prefixTableTarget + '''categories, ''' + prefixTableTarget + '''assets SET ''' + prefixTableTarget + '''categories.asset_id = ''' + prefixTableTarget + '''assets.id WHERE CONCAT("com_content.category.", ''' + prefixTableTarget + '''categories.id) = ''' + prefixTableTarget + '''assets.name AND ''' + prefixTableTarget + '''categories.asset_id = 0 ;'''))
print(colored('The asset_id field in target table #_categories has been fixed.', 'green'))

sessionCategoriesAssets.commit()
sessionCategoriesAssets.close()


################ TARGET FIELD ASSET_ID IN TABLE #_UCM_CONTENT
print(colored('\nField asset_id in table #_ucm_content', 'blue'))
my_session_ucm_content_assets = sessionmaker(bind=engineTarget)
sessionUcmContentAssets = my_session_ucm_content_assets()
sessionUcmContentAssets.begin()

sessionUcmContentAssets.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content, ''' + prefixTableTarget + '''content SET ''' + prefixTableTarget + '''ucm_content.asset_id = ''' + prefixTableTarget + '''content.asset_id WHERE ''' + prefixTableTarget + '''ucm_content.core_content_item_id = ''' + prefixTableTarget + '''content.id ;'''))
print(colored('The asset_id field in target table #_ucm_content has been fixed.', 'green'))

sessionUcmContentAssets.commit()
sessionUcmContentAssets.close()


################ TARGET FIELD PARENT_ID IN TABLE #_ASSETS FOR ARTICLES
print(colored('\nField parent_id in table #_assets for articles', 'blue'))
sqlTargetAssetsParentIdFix = '''SELECT CONCAT("UPDATE ''' + prefixTableTarget + '''assets SET parent_id = ", ''' + prefixTableTarget + '''assets_2.id, " WHERE id = ", ''' + prefixTableTarget + '''assets_1.id, " ;") AS my_query FROM ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_1 LEFT JOIN ''' + prefixTableTarget + '''content ON ''' + prefixTableTarget + '''assets_1.name = CONCAT("com_content.article.", ''' + prefixTableTarget + '''content.id) LEFT JOIN ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_2 ON ''' + prefixTableTarget + '''content.catid = REPLACE(''' + prefixTableTarget + '''assets_2.name, "com_content.category.", "") WHERE ''' + prefixTableTarget + '''assets_1.name LIKE "com_content.article.%%" ;'''
dfTargetAssetsParentIdFix = pd.read_sql_query(sqlTargetAssetsParentIdFix, engineTarget)

# EXCLUDE EMPTY RESULTS
dfTargetAssetsParentIdFix = dfTargetAssetsParentIdFix[
    (dfTargetAssetsParentIdFix['my_query'].notnull()) &
    (dfTargetAssetsParentIdFix['my_query'].notna()) &
    (dfTargetAssetsParentIdFix['my_query'] != '') &
    (dfTargetAssetsParentIdFix['my_query'] != 'nan')
]

# print('\ndfTargetAssetsParentIdFix:')
# print(tab(dfTargetAssetsParentIdFix.head(10), headers='keys', tablefmt='psql', showindex=False))
# print(dfTargetAssetsParentIdFix.shape[0])

my_session_target_assets_parent_id_fix = sessionmaker(bind=engineTarget)
sessionTargetAssetsParentIdFix = my_session_target_assets_parent_id_fix()
sessionTargetAssetsParentIdFix.begin()

for index, row in dfTargetAssetsParentIdFix.iterrows():
    myQuery = row['my_query']
    # print(myQuery)
    sessionTargetAssetsParentIdFix.execute(text(myQuery))

sessionTargetAssetsParentIdFix.commit()
sessionTargetAssetsParentIdFix.close()
print(colored('The field parent_id has been fixed in table #_assets for articles.', 'green'))


################ TARGET FIELD PARENT_ID IN TABLE #_ASSETS FOR CATEGORIES
print(colored('\nField parent_id in table #_assets for categories', 'blue'))
sqlTargetAssetsCatParentIdFix = '''SELECT IF(''' + prefixTableTarget + '''assets_2.id IS NULL, CONCAT("UPDATE ''' + prefixTableTarget + '''assets SET parent_id = ''' + str(ContentAssetIdTarget) + ''' WHERE id = ",''' + prefixTableTarget + '''assets_1.id, " ;"), CONCAT("UPDATE ''' + prefixTableTarget + '''assets SET parent_id = ", ''' + prefixTableTarget + '''assets_2.id, " WHERE id = ", ''' + prefixTableTarget + '''assets_1.id, " ;")) AS my_query FROM ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_1 LEFT JOIN ''' + prefixTableTarget + '''categories ON ''' + prefixTableTarget + '''assets_1.name = CONCAT("com_content.category.", ''' + prefixTableTarget + '''categories.id) LEFT JOIN ''' + prefixTableTarget + '''assets AS ''' + prefixTableTarget + '''assets_2 ON ''' + prefixTableTarget + '''categories.parent_id = REPLACE(''' + prefixTableTarget + '''assets_2.name, "com_content.category.", "") WHERE ''' + prefixTableTarget + '''assets_1.name LIKE "com_content.category.%%" ;'''
dfTargetAssetsCatParentIdFix = pd.read_sql_query(sqlTargetAssetsCatParentIdFix, engineTarget)

# print('\ndfTargetAssetsCatParentIdFix:')
# print(tab(dfTargetAssetsCatParentIdFix.head(10), headers='keys', tablefmt='psql', showindex=False))
# print(dfTargetAssetsCatParentIdFix.shape[0])

# EXCLUDE EMPTY RESULTS
dfTargetAssetsCatParentIdFix = dfTargetAssetsCatParentIdFix[
    (dfTargetAssetsCatParentIdFix['my_query'].notnull()) &
    (dfTargetAssetsCatParentIdFix['my_query'].notna()) &
    (dfTargetAssetsCatParentIdFix['my_query'] != '') &
    (dfTargetAssetsCatParentIdFix['my_query'] != 'nan')
    ]

my_session_target_assets_cat_parent_id_fix = sessionmaker(bind=engineTarget)
sessionTargetAssetsCatParentIdFix = my_session_target_assets_cat_parent_id_fix()
sessionTargetAssetsCatParentIdFix.begin()

for index, row in dfTargetAssetsCatParentIdFix.iterrows():
    myQuery = row['my_query']
    # print(myQuery)
    sessionTargetAssetsCatParentIdFix.execute(text(myQuery))

sessionTargetAssetsCatParentIdFix.commit()
sessionTargetAssetsCatParentIdFix.close()
print(colored('The field parent_id has been fixed in table #_assets for categories.', 'green'))


################ TARGET FIELD TYPE_ID IN TABLE #_CONTENTITEM_TAG_MAP
print(colored('\nField type_id in table #_contentitem_tag_map', 'blue'))
my_session_contentitem_tag_map_type = sessionmaker(bind=engineTarget)
sessionContentItemTagMapType = my_session_contentitem_tag_map_type()
sessionContentItemTagMapType.begin()

sessionContentItemTagMapType.execute(text('''UPDATE ''' + prefixTableTarget + '''contentitem_tag_map SET type_id = ''' + str(ArticlesTypeIdTarget) + ''' WHERE type_alias LIKE "com_content.article" ;'''))
print(colored('The type_id field in target table #_contentitem_tag_map has been fixed.', 'green'))

sessionContentItemTagMapType.commit()
sessionContentItemTagMapType.close()


################ TARGET FIELD TYPE_ID IN TABLE #_UCM_CONTENT
print(colored('\nField type_id in table #_ucm_content', 'blue'))
my_session_ucm_content_type = sessionmaker(bind=engineTarget)
sessionUcmContentType = my_session_ucm_content_type()
sessionUcmContentType.begin()

sessionUcmContentType.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_type_id = ''' + str(ArticlesTypeIdTarget) + ''' WHERE core_type_alias LIKE "com_content.article" ;'''))
print(colored('The type_id field in target table #_ucm_content has been fixed.', 'green'))

sessionUcmContentType.commit()
sessionUcmContentType.close()