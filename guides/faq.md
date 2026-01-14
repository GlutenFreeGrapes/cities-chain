# Frequently Asked questions
### Q: Some cities aren't recognized by the bot. Why?**
A: The bot takes its data from Geonames, and only cities with a listed population (that is, greater than 0 people listed) are considered by the bot.

### Q: I added some cities to Geonames, but they still aren't recognized by the bot. Why?
A: The Geonames dump updates the cities list daily, but the bot's record of cities is not updated on a regular basis, so it might take until I decide to update it again for those cities to show up.

### Q: Why does the bot go down sometimes for a few seconds before coming back up?
A: Usually, this is because I have just updated the bot. The way this is set up, the bot will check every 15 minutes whethere there is a new update, and if so, will restart. Just to be safe, when this happens, use `/stats server` to check what the next letter is.

### Q: Why are some of the romanizations for cities incorrect?
A: That's a thing to do with the Python library I use to romanize the characters (`anyascii`) - characters are romanized one-by-one instead of with context. It is still better than the previous library I was using (`unidecode`), though. 

### Q: How do I suggest feedback to the bot?
A: There is a support server and support channels listed in the `/about` command for this bot.