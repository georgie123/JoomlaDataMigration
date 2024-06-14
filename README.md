
<h1 align="center">Joomla Data Migration</h1>

This is a Python process to transfert Joomla core data from a website to another.

## Core Data
When I say "Joomla core data", I mean the users (with their access rights, messages, privacy contents ...) and the articles (with their rights, categories, tags ...).
<br>Some other tables are related to this core data and will be managed during the migration.

## Why
From my experience, it is very usefull to try a new template, a new extension, prepare the new version of your Joomla website, configure some specific backups ...
<br>Once installed, the process is fast and simple, just one-click and you will be happy.

## How it works
Set up a small text file in your computer to store the access of your two Joomla databases.
<br>In my code, this file is named "gjcY8d4q6mvC2WXy.ztxt", at the root of my PyCharm directory.
<br>Into this file, you need the database access, the tables prefix, but also the domain (without http/https, and even if it is "localhost").
<br>Then the code will search your two access from the first line describing an access.
<br>Call your access file from the script named "_params.py", changing his path but also the strings searching the access.

<br>In my "_params.py" script, you will see two blocks managing my PyCharm directory and my two computers, arf.
<br>You can simplified this script to only find your access file, no more.

<br>OK, then, just run the script named "start.py"!

## Validated
<ul>
<li>Tested without issue from J3.9.23 to J5.0.2!</li>
<li>Tested without issue from J3.9.23 to a J5.1.1!</li>

</ul>

## What's next


<br>And sorry for my english ... I am french, lol.

<b><a href="https://hg-map.fr/">HG-map.fr</a></b>

