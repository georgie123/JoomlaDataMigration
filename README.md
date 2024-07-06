
<h1 align="center">Joomla Data Migration</h1>

This is a Python process to transfert Joomla core data from a website to another.

## Core Data
When I say <b><i>Joomla core data</i></b>, I mean the users (with their access rights, messages, privacy contents ...) and the articles (with their rights, categories, tags ...).
<br>Some other tables are related to this core data and will be managed during the migration.

## Why
From my experience, it is very usefull for try a new template, a new extension, prepare the new version of your Joomla website, configure some specific backups, clean up a website ...
<br>Once installed, the process is fast and simple, just one-click and you will be happy.

## How it works
Set up a small text file in your computer to store the access of your two Joomla databases.
<br>In my code, this file is named <b><i>gjcY8d4q6mvC2WXy.ztxt</i></b>, at the root of my PyCharm directory (but put it where you want).
<br>Into this file, set up the two database access, the tables prefix, but also the domains (without http/https, and even if it is <b><i>localhost</i></b>).

<pre>
<h3 align="center">Access file example (utf8)</h3>
>>> SOURCE My original Joomla website
host:xxxxxxxxxxxx
port:330X
DB user:xxxxxxxxxxxx
DB password:xxxxxxxxxxx
DB name:xxxxxxxxxxx
Tables prefix:xx_
Domain:mydomain.fr

>>> TARGET My brand new Joomla website online
host:xxxxxxxxxxxx
port:330X
DB user:xxxxxxxxxxxx
DB password:xxxxxxxxxxxx
DB name:xxxxxxxxxxxx
Tables prefix:xx_
Domain:test.mydomain.fr

>>> TARGET My brand new Joomla website local
host:localhost
port:330X
DB user:xxxxxxxxxxxx
DB password:xxxxxxxxxxxx
DB name:xxxxxxxxxxxx
Tables prefix:xx_
Domain:localhost/MyBrandNewJoomlaWebsite
</pre>

<br>Call your access file from the script named <b><i>_params.py</i></b>, changing his path but also the two first lines describing the access.
<br>The code will search your two access from the first line describing an access.

<br>Take your time to set up the <b><i>_params.py</i></b>, because it manages the source and the target websites.
<br>The <b><i>source</i></b> is your original website, maybe in production 😯
<br>The <b><i>target</i></b> website is your brandnew website, <b>consider it as a sandbox during the first migration</b> (then you can work on it, but please consider it as a sandbox during the first time you run the process).

<br>In my <b><i>_params.py</i></b> script, you will see specific blocks managing my PyCharm directories on my two computers 😁
<br>Of course, you can simplify this script to only find and read your access file, no more.

## Start
OK, then, just run the script named <b><i>start.py</i></b>!
<br>Each time this script is launched, the core data of the target website is deleted. So you can run <b><i>start.py</i></b> as many times as you want, to get the last updates from the source website.

<br>The source website is never updated during the process, so it is secure but be careful to well understand the script <b><i>_params.py</i></b> and the access file.

<br>This script will run all the scripts placed in the directory named <b><i>transferts</i></b>. So you can add your own scripts to manage other specific tables if needy.

## URLs and images
In <b><i>transferts/_2_contents.py</i></b>, I manage images and links from/going to the website itself, and the URL rewriting (search "UPDATE TARGET TEXT FIELDS" or "FIX URL REWRITE"). 

Feel free to cancel this management if needy.

In addition, depending on the degree of customization of your site, the storage location of your images... it may be imperfect.
It will then be up to you to add another transfer script to execute a correction SQL query of this type for example:

<pre>
UPDATE MyDb.#_content SET introtext = REPLACE(introtext, 'src="users/', 'src="http://localhost/MyBrandNewWebsite/users/') ;
UPDATE MyDb.#_content SET `fulltext` = REPLACE(`fulltext`, 'src="users/', 'src="http://localhost/MyBrandNewWebsite/users/') ;
</pre>

## End
At the end of the process, via the Joomla administration, Maintetance tab, empty all caches, update the database structure and unlock all elements.

## Validated
<ul>
<li>OK from J3.9.23 to J5.0.2</li>
<li>OK from J3.9.23 to a J5.1.1</li>
<li>OK from J3.7.4 to a J4.2.8</li>
</ul>

## What's next
I plan to add an automatic listing of the paths of all the images and files used in the articles.
<br>With a such list, we will be able to build some command lines to get/move them in another server.

And sorry for my english ... I am french, lol.

<b><a href="https://hg-map.fr/">HG-map.fr</a></b>

