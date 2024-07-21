
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from tabulate import tabulate as tab
import time


print(colored('\nIMAGES AND LINKS', 'blue'))

################ HARD LINKS FOR IMAGES
print(colored('\nHard links for images', 'blue'))
my_session_images = sessionmaker(bind=engineTarget)
sessionImages = my_session_images()
sessionImages.begin()

query1 = u'''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="users/', 'src="https://''' + domainTarget + '''/users/') WHERE introtext LIKE '%src="users/%' ;'''
query_management_update(sessionImages, query1, 5, 5)

query2 = u'''UPDATE ''' + prefixTableTarget + '''content SET `fulltext` = REPLACE(`fulltext`, 'src="users/', 'src="https://''' + domainTarget + '''/users/') WHERE `fulltext` LIKE '%src="users/%' ;'''
query_management_update(sessionImages, query2, 5, 5)

query3 = u'''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="images/', 'src="https://''' + domainTarget + '''/images/') WHERE introtext LIKE '%src="images/%' ;'''
query_management_update(sessionImages, query3, 5, 5)

query4 = u'''UPDATE ''' + prefixTableTarget + '''content SET `fulltext` = REPLACE(`fulltext`, 'src="images/', 'src="https://''' + domainTarget + '''/images/') WHERE `fulltext` LIKE '%src="images/%' ;'''
query_management_update(sessionImages, query4, 5, 5)

query5 = u'''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'href="images/', 'href="https://''' + domainTarget + '''/images/') WHERE introtext LIKE '%href="images/%' ;'''
query_management_update(sessionImages, query5, 5, 5)

query6 = u'''UPDATE ''' + prefixTableTarget + '''content SET `fulltext` = REPLACE(`fulltext`, 'href="images/', 'href="https://''' + domainTarget + '''/images/') WHERE `fulltext` LIKE '%href="images/%' ;'''
query_management_update(sessionImages, query6, 5, 5)

print(colored('Now images use hard links.', 'green'))
sessionImages.close()
time.sleep(1)


# FIX URL REWRITE
print(colored('\nFix URL rewrite in #_content', 'blue'))
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
    query_management_update(sessionUrlRewriteContent, myQuery1, 5, 5)

for index, row in dfTargetTextUrlFix.iterrows():
    myQuery2 = row['query2']
    query_management_update(sessionUrlRewriteContent, myQuery2, 5, 5)

sessionUrlRewriteContent.close()
time.sleep(1)

print(colored('The URL rewrite links has been fixed in the text fields of target table #_content (removing id from URL articles).', 'green'))


################ UPDATE TARGET TEXT FIELDS FOR #_UCM_CONTENT
print(colored('\nFields text in table #_ucm_content', 'blue'))

my_session_text_ucm = sessionmaker(bind=engineTarget)
sessionTextUcm = my_session_text_ucm()
sessionTextUcm.begin()

query1 = '''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, \'''' + domainSource + '''\', \'''' + domainTarget + '''\') ;'''
query_management_update(sessionTextUcm, query1, 5, 5)

query2 = '''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), core_body = REPLACE(core_body, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''
query_management_update(sessionTextUcm, query2, 5, 5)

print(colored('Links to the website has been fixed in the field core_body of target table #_ucm_content.', 'green'))

sessionTextUcm.close()
time.sleep(1)


# FIX URL REWRITE
print(colored('\nFix URL rewrite in #_ucm_content', 'blue'))

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
    query_management_update(sessionUrlRewriteUcmContent, myQuery1, 5, 5)

sessionUrlRewriteUcmContent.close()
time.sleep(1)

print(colored('The URL rewrite links has been fixed in the text fields of target table #_ucm_content (removing id from URL articles).', 'green'))
