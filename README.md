
<h1 align="center">Joomla Data Migration</h1>

This is a Python process to transfert Joomla core data from a website to another.

## Core Data
When I say "Joomla core data", I mean the users (with their access rights, messages, privacy contents ...) and the articles (with their rights, categories, tags ...).
<br>Some other tables are related to this core data and will be managed during the migration.

## Why
From my experience, it is very usefull for try a new template, a new extension, prepare the new version of your Joomla website, configure some specific backups, clean up a website ...
<br>Once installed, the process is fast and simple, just one-click and you will be happy.

## How it works
Set up a small text file in your computer to store the access of your two Joomla databases.
<br>In my code, this file is named "gjcY8d4q6mvC2WXy.ztxt", at the root of my PyCharm directory (but put it where you want).
<br>Into this file, set up the two database access, the tables prefix, but also the domains (without http/https, and even if it is "localhost").
<br>Then the code will search your two access from the first line describing an access.
<br>Call your access file from the script named "_params.py", changing his path but also the two first lines describing the access.

<br>In my "_params.py" script, you will see two code blocks managing my PyCharm directories on my two computers. Of course you can simplified this script to only find your access file, no more.

<br>OK, then, just run the script named "start.py"!
<br>This script will run all the scripts placed in the directory named "transferts".

## Validated
<ul>
<li>OK from J3.9.23 to J5.0.2!</li>
<li>OK from J3.9.23 to a J5.1.1!</li>
</ul>

## What's next
I plan to add an automatic listing of the paths of all the images and files used in the articles.
<br>With a such list, we will be able to build some command lines to get/move them in another server.

<br>And sorry for my english ... I am french, lol.

<b><a href="https://hg-map.fr/">HG-map.fr</a></b>

