
Users Data File Structure (users.csv)
--------

user_id 
first_name
last_name
birth_date
country


Ratings Data File Structure (ratings.csv)
-----------------------------------------

user_id
movie_id
rating
created_at

Ratings are made on a 5-star scale, with half-star increments (0.5 stars - 5.0 stars).


Tags Data File Structure (tags.csv)
-----------------------------------

user_id
movie_id
tag
created_at

Tags are user-generated metadata about movies. Each tag is typically a single word or short phrase. The meaning, value, and purpose of a particular tag is determined by each user.


Movies Data File Structure (movies.csv)
---------------------------------------

movie_id
title
genres

Movie titles include the year of release in parentheses. Errors and inconsistencies may exist in these titles.

Genres are a pipe-separated list, and are selected from the following:

* Action
* Adventure
* Animation
* Children's
* Comedy
* Crime
* Documentary
* Drama
* Fantasy
* Film-Noir
* Horror
* Musical
* Mystery
* Romance
* Sci-Fi
* Thriller
* War
* Western
* (no genres listed)


