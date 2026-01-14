# Rules
1. The next letter of each city must start with the last letter of the previous city. (e.g, if someone names `Auckland`, the next city to be named must start with `D`, like `Damascus` or `Den Haag`.)
2. You may not go twice in a row. 
3. Cities must meet the minimum population requirement. This number may be checked with `/stats server`. 
4. Unless specified otherwise, cities cannot be repeated within a certain number of cities as each other. This number may be checked with `/stats server`. 
5. Cities must exist on Geonames and must have a minimum population of 1. 
6. Cities must have a prefix at the beginning of the message they are sent in for the bot to count them. This may be checked with `/stats server`, with `None` meaning that all messages will count. 
7. Cities with alternate names will be counted as the same city, but may start and end with different letters. (e.g., despite being the same city, `The Hague` starts with `t` and ends with `e`, and `Den Haag` starts with `d` and ends with `g`. In the example where someone names `Auckland`, `Den Haag` would be accepted, but `The Hague` would not.)
8. If need be, users may be banned from using the bot. Reasons for banning include, but are not limited to:
  - Placing deliberately incorrect cities
  - Falsifying cities on Geonames
  - Creating alternate accounts to sidestep bans