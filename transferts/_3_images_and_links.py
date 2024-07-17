
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate as tab
import time


print(colored('\nIMAGES AND LINKS', 'blue'))

################ HARD LINKS FOR IMAGES
print(colored('\nHard links for images', 'blue'))
my_session_images = sessionmaker(bind=engineTarget)
sessionImages = my_session_images()
sessionImages.begin()

sessionImages.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="users/', 'src="https://''' + domainTarget + '''/users/') ;'''))
sessionImages.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET `fulltext` = REPLACE(`fulltext`, 'src="users/', 'src="https://''' + domainTarget + '''/users/') ;'''))

sessionImages.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="images/', 'src="https://''' + domainTarget + '''/images/') ;'''))
sessionImages.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET `fulltext` = REPLACE(`fulltext`, 'src="images/', 'src="https://''' + domainTarget + '''/images/') ;'''))

print(colored('Now images use hard links.', 'green'))

sessionImages.commit()
time.sleep(1)
# sessionImages.close()

time.sleep(1)


################ UPDATE TARGET TEXT FIELDS FOR #_CONTENT
print(colored('\nFields text in table #_content', 'blue'))
my_session_text_content = sessionmaker(bind=engineTarget)
sessionTextContent = my_session_text_content()
sessionTextContent.begin()

query1 = '''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, \'''' + domainSource + '''\', \'''' + domainTarget + ''''), `fulltext` = REPLACE(`fulltext`, \'''' + domainSource + '''', \'''' + domainTarget + '''\') ;'''
sessionTextContent.execute(text(query1))
time.sleep(1)

query2 = '''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), introtext = REPLACE(introtext, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/'), `fulltext` = REPLACE(`fulltext`, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), `fulltext` = REPLACE(`fulltext`, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''
sessionTextContent.execute(text(query2))
time.sleep(1)

print(colored('Links to the website has been fixed in the text fields of target table #_content.', 'green'))

sessionTextContent.commit()
time.sleep(1)
# sessionTextContent.close()


# FIX URL REWRITE
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
    sessionUrlRewriteContent.execute(text(myQuery1))

for index, row in dfTargetTextUrlFix.iterrows():
    myQuery2 = row['query2']
    sessionUrlRewriteContent.execute(text(myQuery2))

sessionUrlRewriteContent.commit()
# sessionUrlRewriteContent.close()
time.sleep(1)

print(colored('The URL rewrite links has been fixed in the text fields of target table #_content (removing id from URL articles).', 'green'))


################ UPDATE TARGET TEXT FIELDS FOR #_UCM_CONTENT
print(colored('\nFields text in table #_ucm_content', 'blue'))
my_session_text_ucm = sessionmaker(bind=engineTarget)
sessionTextUcm = my_session_text_ucm()
sessionTextUcm.begin()

query1 = '''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, \'''' + domainSource + '''\', \'''' + domainTarget + '''\') ;'''
# print(query1)
sessionTextUcm.execute(text(query1))
time.sleep(1)

query2 = '''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), core_body = REPLACE(core_body, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''
# print(query2)
sessionTextUcm.execute(text(query2))
time.sleep(1)

print(colored('Links to the website has been fixed in the field core_body of target table #_ucm_content.', 'green'))

sessionTextUcm.commit()
time.sleep(1)
# sessionTextUcm.close()


# FIX URL REWRITE
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
# sessionUrlRewriteUcmContent.close()
time.sleep(1)

print(colored('The URL rewrite links has been fixed in the text fields of target table #_ucm_content (removing id from URL articles).', 'green'))

