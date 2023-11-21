# Cities Chain
Cities chain bot

## Commands:
### Set commands:
`/set channel [channel]`: sets the channel the bot will listen to. **Must be done before using the bot**
    
`/set prefix ([prefix])`: sets prefix to use when listening for cities
    
`/set choose-city [option]`: if turned on, allows bot to choose the city that begins the next chain

`/set population`: sets minimum population for cities
    
`/set repeat [num]`: sets number of different cities that have to be said before a city can be repeated again. If set to -1, repeating is disallowed
    
### Reaction/Repeat commands: 
`/add react [city] ([administrative-division][country])`: bot autoreacts an emoji when a given city is said
    
`/remove react [city] ([administrative-division][country])`: bot removes autoreact for given city

`/add repeat [city] ([administrative-division][country])`: bot will ignore no repeats rule for given city
    
`/remove repeat [city] ([administrative-division][country])`: bot removes repeating exception for given city
    
### Stats commands:
`/stats cities-all ([show-everyone])`: displays all cities in the chain
    
`/stats cities ([show-everyone])`: displays cities that cannot be repeated
    
`/stats server ([show-everyone])`: displays server stats
    
`/stats user ([member][show-everyone])`: displays user stats
    
`/stats slb ([show-everyone])`: displays server leaderboard
    
`/stats lb ([show-everyone])`: displays global leaderboard
    
`/stats best-rounds ([show-everyone])`: displays 5 longest chains
   
`/stats popular-cities ([show-everyone])`: displays 10 most popular cities and countries in the chain
    
`/stats round [round]([show-everyone])`: gets list of cities for a specific round
    
`/stats react ([show-everyone])`: gets all cities with reactions
   
`/stats repeat ([show-everyone])`: gets all cities that can be repeated anytime
    
`/stats blocked-users ([show-everyone])`: gets the list of users in the server blocked from using the bot

### Other commands:
`/city-info [city] ([administrative-division][country])`: gets information about the given city
   
`/country-info [country]`: gets information about the given country
   
`delete-stats`: deletes stats for your server
   
`/ping`: shows bot latency

`/block [user]`: blocks a certain user if they are purposefully ruining the chain
  
`/unblock [user]`: unblocks a certain user
  
`/help`: sends a message containing all the commands and their descriptions
