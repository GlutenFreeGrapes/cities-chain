# Cities Chain
Cities chain bot

## Commands:
`/set channel [channel]`: sets channel to listen to. **You must use this before .**

`/add react [city] ([administrative-division][country])`: bot autoreacts an emoji when a given city is said

`/remove react [city] ([administrative-division][country])`: bot removes autoreact for given city

`/add repeat [city] ([administrative-division][country])`: bot will ignore no repeats rule for given city

`/remove repeat [city] ([administrative-division][country])`: bot removes repeating exception for given city

`/set repeat [num]`: sets number of different cities that have to be said before a city can be repeated again - if set to -1, repeating is disallowed

`/set population [population]`: sets minimum population for cities

`/set prefix [prefix]`: sets prefix to use when listening for cities

`/set choose-city`: if turned on, allows bot to choose the city that begins the next chain

`/stats cities-all`: displays all cities in the chain

`/stats cities`: displays cities that cannot be repeated

`/stats server`: displays server stats

`/stats user ([member])`: displays user stats

`/stats slb`: displays server leaderboard

`/stats lb`: displays global leaderboard

`/stats best-rds`: displays 5 longest chains

`/stats popular-cities`: displays 10 most popular cities and countries in the chain

`/stats round [round]`: gets list of cities for a specific round

`/stats react`: gets all cities with reactions

`/stats repeat`: gets all cities that can be repeated anytime

`/city-info [city] ([administrative-division][country])`: gets information about the given city

`/alt-names [city] ([administrative-division][country])`: gets the given city's alternate names

`/ping`: shows bot latency
