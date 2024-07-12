
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate as tab


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
sessionImages.close()


################ UPDATE TARGET TEXT FIELDS FOR #_CONTENT
print(colored('\nFields text in table #_content', 'blue'))
my_session_text_content = sessionmaker(bind=engineTarget)
sessionTextContent = my_session_text_content()
session.begin()

sessionTextContent.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, \'''' + domainSource + '''\', \'''' + domainTarget + ''''), `fulltext` = REPLACE(`fulltext`, \'''' + domainSource + '''', \'''' + domainTarget + '''\') ;'''))
sessionTextContent.execute(text('''UPDATE ''' + prefixTableTarget + '''content SET introtext = REPLACE(introtext, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), introtext = REPLACE(introtext, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/'), `fulltext` = REPLACE(`fulltext`, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), `fulltext` = REPLACE(`fulltext`, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''))
print(colored('Links to the website has been fixed in the text fields of target table #_content.', 'green'))

sessionTextContent.commit()
sessionTextContent.close()

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
    myQuery2 = row['query2']

    sessionUrlRewriteContent.execute(text(myQuery1))
    sessionUrlRewriteContent.execute(text(myQuery2))

    # print(myQuery1)
    # print(myQuery2)

sessionUrlRewriteContent.commit()
sessionUrlRewriteContent.close()
print(colored('The URL rewrite links has been fixed in the text fields of target table #_content (removing id from URL articles).', 'green'))


################ UPDATE TARGET TEXT FIELDS FOR #_UCM_CONTENT
print(colored('\nFields text in table #_ucm_content', 'blue'))
my_session_text_ucm = sessionmaker(bind=engineTarget)
sessionTextUcm = my_session_text_ucm()
sessionTextUcm.begin()

sessionTextUcm.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, \'''' + domainSource + '''\', \'''' + domainTarget + '''') ;'''))
sessionTextUcm.execute(text('''UPDATE ''' + prefixTableTarget + '''ucm_content SET core_body = REPLACE(core_body, 'src="https://''' + domainTarget + '''/images/', 'src="https://''' + domainSource + '''/images/'), core_body = REPLACE(core_body, 'src="http://''' + domainTarget + '''/images/', 'src="http://''' + domainSource + '''/images/') ;'''))
print(colored('Links to the website has been fixed in the field core_body of target table #_ucm_content.', 'green'))

sessionTextUcm.commit()
sessionTextUcm.close()

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
sessionUrlRewriteUcmContent.close()

print(colored('The URL rewrite links has been fixed in the text fields of target table #_ucm_content (removing id from URL articles).', 'green'))
