
from sqlalchemy.orm import sessionmaker

contentsTables = ['content', 'contentitem_tag_map', 'ucm_content', 'history', 'assets', 'categories', 'workflow_associations', 'tags']

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

    elif t == 'categories':
        SessionCleanTarget.execute(text('''DELETE FROM ''' + prefixTableTarget + t + ''' WHERE id > 1 ;'''))
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