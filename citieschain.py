import discord, re, math, mariadb, asyncio, io, json, aiohttp, datetime, pytz, traceback
from discord import app_commands
from typing import Optional,Literal
from os import environ as env
from dotenv import load_dotenv
from mpl_toolkits.basemap import Basemap
from anyascii import anyascii
# from threading import Thread, Lock, Condition
import matplotlib.pyplot as plt
import polars as pl
import logging
import functools
import ast

load_dotenv()

LOGGING_FILE = 'cities-chain-discord'

intents = discord.Intents.default()
intents.message_content = True

citydata=pl.read_csv('data/cities.txt',separator='\t',null_values='',schema=pl.Schema({
    'geonameid':pl.Int32,
    'name':pl.String,
    'population':pl.Int32,
    'country':pl.String,
    'admin1':pl.String,
    'admin2':pl.String,
    'alt-country':pl.String,
    'default':pl.Int8,
    'latitude':pl.Float32,
    'longitude':pl.Float32,
    'decoded':pl.String,
    'punct-space':pl.String,
    'punct-empty':pl.String,
    'first-letter':pl.String,
    'last-letter':pl.String,
    'deleted':pl.Int8
}))
countriesdata=pl.read_csv('data/countries.txt',separator='\t',null_values='',schema = pl.Schema({
    'geonameid':pl.Int32,
    'country':pl.String,
    'iso3':pl.String,
    'name':pl.String,
    'default':pl.Int8,
}))
admin1data=pl.read_csv('data/admin1.txt',separator='\t',null_values='',schema=pl.Schema({
    'geonameid':pl.Int32,
    'country':pl.String,
    'admin1':pl.String,
    'name':pl.String,
    'default':pl.Int8,
}))
admin2data=pl.read_csv('data/admin2.txt',separator='\t',null_values='',schema=pl.Schema({
    'geonameid':pl.Int32,
    'country':pl.String,
    'admin1':pl.String,
    'admin2':pl.String,
    'name':pl.String,
    'default':pl.Int8,
}))
metadata = json.load(open('data/metadata.json','r'))

GREEN = discord.Colour.from_rgb(0,255,0)
RED = discord.Colour.from_rgb(255,0,0)
BLUE = discord.Color.from_rgb(0,0,255)

# client = discord.Client(intents=intents)
client = discord.AutoShardedClient(intents=intents)
tree=app_commands.tree.CommandTree(client)

city_default=citydata.filter(pl.col('default') == 1)
admin2_default=admin2data.filter(pl.col('default') == 1)
admin1_default=admin1data.filter(pl.col('default') == 1)
countrydefaults=countriesdata.filter(pl.col('default') == 1)
allcountries=list(countrydefaults['name'])
iso2={i:allcountries[n] for n,i in enumerate(countrydefaults['country'])}
iso3={i:allcountries[n] for n,i in enumerate(countrydefaults['iso3'])}

allcountries=sorted(allcountries)
regionalindicators={chr(97+i):chr(127462+i) for i in range(26)}
flags = {i:regionalindicators[i[0].lower()]+regionalindicators[i[1].lower()] for i in iso2}
stats_time_offset = {"24 Hours" : datetime.timedelta(days=1), 
                     "48 Hours" : datetime.timedelta(days=2), 
                     "7 Days" : datetime.timedelta(days=7), 
                     "14 Days" : datetime.timedelta(days=14), 
                     "1 Month" : datetime.timedelta(days=30), 
                     "3 Months" : datetime.timedelta(days=90)}
block_time_offset = {"1 Hour" : datetime.timedelta(hours=1),
                     "6 Hours" : datetime.timedelta(hours=6),
                     "24 Hours" : datetime.timedelta(days=1), 
                     "48 Hours" : datetime.timedelta(days=2), 
                     "7 Days" : datetime.timedelta(days=7), 
                     "1 Month" : datetime.timedelta(days=30)}
NEVER_TIMESTAMP = datetime.datetime.fromtimestamp(999999)

env.setdefault("DB_NAME", "cities_chain")
conn = mariadb.connect(
    user=env["DB_USER"],
    password=env["DB_PASSWORD"],
    host=env["DB_HOST"],
    database=env["DB_NAME"])
cur = conn.cursor() 

# cur.execute("CREATE DATABASE IF NOT EXISTS cities_chain", (env["DB_NAME"],))
# cur.execute("USE cities_chain" + (env["DB_NAME"],))
cur.execute("SET @@session.wait_timeout = 2592000") # max 30 day wait timeout
cur.execute("SET @@session.interactive_timeout = 28800") # max 8hr interactive timeout

def exec_sql_file(cursor, sql_file):
    print ("\n[INFO] Executing SQL script file: '%s'" % (sql_file))
    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r';$', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            try:
                cursor.execute(statement)
            except (mariadb.OperationalError, mariadb.ProgrammingError) as e:
                print ("\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args)))

            statement = ""
exec_sql_file(cur, "pre_run_tasks.sql")

# round_info
# cur.execute('''CREATE TABLE IF NOT EXISTS round_info(
#             server_id BIGINT PRIMARY KEY,
#             round_number INT PRIMARY KEY,
#             length INT,
#             leaderboard_length INT,
#             participants INT,
#             start_time INT,
#             end_time INT,
#             start_city_id INT,
#             end_city_id INT)''')

# conn.commit()
# print("committed")

cur.execute("SELECT server_id, MIN(time_placed) FROM chain_info GROUP BY server_id")
max_ages = {i[0]:i[1] for i in cur.fetchall()}
earliest_time = min(max_ages.values()) if len(max_ages) else datetime.datetime.fromtimestamp(0)

server_info = pl.read_database("SELECT * FROM server_info", conn)
server_info_dicts = server_info.to_dicts()
cache = {}
for d in server_info_dicts:
    guildid = d["server_id"]
    del d["server_id"]
    cache[guildid] = d

def is_blocked(user_id,guild_id):
    cur.execute("SELECT user_id, blocked FROM global_user_info WHERE user_id = ?", (user_id,))
    g = cur.fetchone()
    if g:
        if g[1]:
            return 1
    else:
        return 0
    cur.execute("SELECT server_id, user_id, blocked FROM server_user_info WHERE user_id = ? and server_id = ?", (user_id,guild_id))
    g = cur.fetchone()
    if g:
        return g[2]
    else:
        return 0


# cache because the only way this will change is if bot shuts down to update
@functools.cache
def admin1name(country,admin1):
    return admin1_default.row(by_predicate=(pl.col('country') == (country)) & (pl.col('admin1') == (admin1)))[admin1_default.get_column_index('name')] if admin1 else None

@functools.cache
def admin2name(country,admin1,admin2):
    return admin2_default.row(by_predicate=(pl.col('country') == (country)) & (pl.col('admin1') == (admin1)) & (pl.col('admin2') == (admin2)))[admin2_default.get_column_index('name')] if admin2 else None

def city_string(name,admin1,admin2,country,alt_country):
    city_str = name
    if country:
        if admin1:
            if admin2:
                city_str+=f", {admin2}"
            city_str+=f", {admin1}"
        city_str+=f", {iso2[country]} {flags[country]}"
        if alt_country:
            city_str+=f"/{iso2[alt_country]} {flags[alt_country]}"
    return city_str

@functools.cache
def city_name_matches(city, min_pop, check_apostrophes, include_deleted):
    city=re.sub(',$','',city.lower().strip())
    if city[-1]==',':
        city=city[:-1]

    # get all cities with name city
    res1=citydata.filter(pl.col('name').str.to_lowercase()==city)
    res2=citydata.filter(pl.col('decoded').str.to_lowercase()==city)
    res3=citydata.filter(pl.col('punct-space').str.to_lowercase()==city)
    res4=citydata.filter(pl.col('punct-empty').str.to_lowercase()==city)
    res1=res1.with_columns(pl.lit(0).alias("match"))
    res2=res2.with_columns(pl.lit(1).alias("match"))
    res3=res3.with_columns(pl.lit(2).alias("match"))
    res4=res4.with_columns(pl.lit(3).alias("match"))
    results=pl.concat([res1,res2,res3,res4])

    # if including search for deleted cities
    if not include_deleted:
        results = results.filter(~pl.col('deleted').cast(pl.Boolean))
    # if there are results
    if results.shape[0]:
        # sort the results
        results=results.sort(['default','population','match'],descending=[True,True,False])
        return results
    else:
        if check_apostrophes:
            return results
        else:
            return city_name_matches(re.sub("[`’ʻʼ]","'",city),min_pop,1,include_deleted)

def search_cities(city,other_arguments,min_pop,include_deleted,country_list_mode, country_list):
    city_names = city_name_matches(city, min_pop, 0, include_deleted)

    # length of other arguments is 1
    if len(other_arguments)==1:
        # separate into default and non-default names
        a1choice_def = admin1_default.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        a2choice_def = admin2_default.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        cchoice_def = countrydefaults.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        a1choice_alt=admin1data.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        a2choice_alt=admin2data.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        cchoice_alt=set(countriesdata.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower())|(pl.col('country').str.to_lowercase()==other_arguments[0].lower()))['country'])
        results = pl.concat([
            city_names.join(cchoice_def[['country', 'admin1']],['country', 'admin1'], 'inner').with_columns(pl.lit(1).alias("match-province")),
            city_names.join(a1choice_def[['country', 'admin1']],['country', 'admin1'], 'inner').with_columns(pl.lit(1).alias("match-province")),
            city_names.join(a2choice_def[['country', 'admin1', 'admin2']],['country', 'admin1', 'admin2'], 'inner').with_columns(pl.lit(1).alias("match-province")),
            city_names.filter(pl.col('country').is_in(cchoice_alt)|pl.col('alt-country').is_in(cchoice_alt)).with_columns(pl.lit(2).alias("match-province")),
            city_names.join(a1choice_alt[['country', 'admin1']],['country', 'admin1'], 'inner').with_columns(pl.lit(2).alias("match-province")),
            city_names.join(a2choice_alt[['country', 'admin1', 'admin2']],['country', 'admin1', 'admin2'], 'inner').with_columns(pl.lit(2).alias("match-province")),
        ]).unique(subset=["geonameid"], keep="first", maintain_order=True)
    elif len(other_arguments)==2:
        # other_args[0] is admin2 or admin1
        a2choice=admin2data.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        a1choice_1=admin1data.filter((pl.col('name').str.to_lowercase()==other_arguments[1].lower()))
        # other_args[1] is admin1 or country
        a1choice_2=admin1data.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        cchoice=set(countriesdata.filter((pl.col('name').str.to_lowercase()==other_arguments[1].lower())|(pl.col('country').str.to_lowercase()==other_arguments[1].lower()))['country'])

        # admin1 & admin2
        a1a2choice=a2choice.join(a1choice_1[['country', 'admin1']], ['country', 'admin1'], 'inner')
        results = city_names.join(a1a2choice[['country', 'admin1','admin2']], ['country', 'admin1','admin2'], 'inner')
        # country & admin1
        a1cchoice=city_names.join(a1choice_2[['country', 'admin1']], ['country', 'admin1'], 'inner')
        a1cchoice=a1cchoice.filter(pl.col('country').is_in(cchoice)|pl.col('alt-country').is_in(cchoice))
        results = pl.concat([results, a1cchoice])
        # country & admin2
        a2cchoice=city_names.join(a2choice[['country', 'admin1','admin2']], ['country', 'admin1','admin2'], 'inner')
        a2cchoice=a2cchoice.filter(pl.col('country').is_in(cchoice)|pl.col('alt-country').is_in(cchoice))
        results = pl.concat([results, a2cchoice]).unique()
    elif len(other_arguments) >= 3:
        a1choice=admin1data.filter((pl.col('name').str.to_lowercase()==other_arguments[1].lower()))
        a2choice=admin2data.join(a1choice,['country','admin1'],'inner')
        a2choice=a2choice.filter((pl.col('name').str.to_lowercase()==other_arguments[0].lower()))
        cchoice=set(countriesdata.filter((pl.col('name').str.to_lowercase()==other_arguments[2].lower())|(pl.col('country').str.to_lowercase()==other_arguments[2].lower()))['country'])
        results = city_names.join(a2choice[['country','admin1','admin2']],['country','admin1','admin2'],'inner')
        results=results.filter(pl.col('country').is_in(cchoice)|pl.col('alt-country').is_in(cchoice)).unique()
    else:
        results = city_names
    if results.shape[0]:
        results=results.sort(['default','population','match'],descending=[True,True,False])
        # return the results in different sortings depending on if the minimum population requirement is fulfilled
        # if population too small, look for larger options. if none, return original result
        if results.row(0)[city_default.get_column_index('population')] < min_pop:
            if results['population'].max() >= min_pop:
                results = results.sort(['population','default','match'],descending=[True,True,False])
        r = results.row(0, named=True)
        if country_list_mode:
            allowed_countries = country_list.split(',')
            country_selection = (pl.col('country').is_in(allowed_countries))|(pl.col('alt-country').is_in(allowed_countries))
            # blacklist
            if country_list_mode == 1:
                results = results.filter(~country_selection)
            # whitelist
            else:
                results = results.filter(country_selection)
            # choose top of those results, failing that choose the default result
            if results.shape[0]:
                r = results.row(0, named=True)
        return r['geonameid'], r, r[('name','decoded','punct-space','punct-empty')[r['match']]]
    return None

def search_cities_command(city,province,otherprovince,country,min_pop,include_deleted,country_list_mode,country_list):
    # otherprovince only used if specified, other administrative division
    # maybe also specify geonameid? 
    results = city_name_matches(city, min_pop, 0, include_deleted)

    # if province is specified
    if province:
        if otherprovince:
            # (admin1, admin2) = (province, otherprovince)
            a1choice=admin1data.filter((pl.col('name').str.to_lowercase()==province.lower()))
            a2choice=admin2data.join(a1choice[['country','admin1']],['country','admin1'],'inner')
            a2choice=a2choice.filter((pl.col('name').str.to_lowercase()==otherprovince.lower()))
            results = results.join(a2choice[['country','admin1','admin2']],['country','admin1','admin2'],'inner')
        else:    
            a1choice=admin1data.filter((pl.col('name').str.to_lowercase()==province.lower()))
            a2choice=admin2data.filter((pl.col('name').str.to_lowercase()==province.lower()))
            results = pl.concat([results.join(a1choice[['country','admin1']],['country','admin1'],'inner'), 
                                 results.join(a2choice[['country','admin1','admin2']],['country','admin1','admin2'],'inner')])
    # if country is specified
    if country:
        cchoice=set(countriesdata.filter((pl.col('name').str.to_lowercase()==country.lower())|(pl.col('country').str.to_lowercase()==country.lower()))['country'])
        results = results.filter(pl.col('country').is_in(cchoice)|pl.col('alt-country').is_in(cchoice))

    if results.shape[0]:
        results=results.sort(['default','population','match'],descending=[True,True,False])
        # return the results in different sortings depending on if the minimum population requirement is fulfilled
        # if population too small, look for larger options. if none, return original result
        if results.row(0)[city_default.get_column_index('population')] < min_pop:
            if results['population'].max() >= min_pop:
                results = results.sort(['population','default','match'],descending=[True,True,False])
        r = results.row(0, named=True)
        if country_list_mode:
            allowed_countries = country_list.split(',')
            country_selection = (pl.col('country').is_in(allowed_countries))|(pl.col('alt-country').is_in(allowed_countries))
            # blacklist
            if country_list_mode == 1:
                results = results.filter(~country_selection)
            # whitelist
            else:
                results = results.filter(country_selection)
            # choose top of those results, failing that choose the default result
            if results.shape[0]:
                r = results.row(0, named=True)
        return r['geonameid'], r, r[('name','decoded','punct-space','punct-empty')[r['match']]]
    return None

def sanitize_query(query):
    query = re.sub('\s*,\s*',',',query).strip()
    if query.endswith(','):
        query=query[:-1]
    if query.startswith(','):
        query=query[1:]
    return tuple(i for i in query.split(',') if i!='')

def generate_map(city_id_list):
    coordinates = {i[city_default.get_column_index('geonameid')]:(i[city_default.get_column_index('latitude')],i[city_default.get_column_index('longitude')]) for i in city_default.filter(pl.col("geonameid").is_in(city_id_list)).iter_rows()}
    coords = [coordinates[i] for i in city_id_list]
    if len(coords):
        lats,lons = zip(*coords)

        # add padding
        SCALING_FACTOR = .25
        mlon=min(lons)
        Mlon=max(lons)
        mlat=min(lats)
        Mlat=max(lats)

        latdif = Mlat-mlat # height
        londif = Mlon-mlon # width

        maxmargins = SCALING_FACTOR*max(latdif,londif)
        if latdif>londif:
            mlon+=londif/2
            Mlon-=londif/2
            londif=(maxmargins+londif)/(1+SCALING_FACTOR)
            mlon-=londif/2
            Mlon+=londif/2
        else:
            mlat+=latdif/2
            Mlat-=latdif/2
            latdif=(maxmargins+latdif)/(1+SCALING_FACTOR)
            mlat-=latdif/2
            Mlat+=latdif/2
        if len(set(coords))>1:
            m = Basemap(llcrnrlon=max(-180,mlon-londif*(SCALING_FACTOR/2)),llcrnrlat=max(-90,mlat-latdif*(SCALING_FACTOR/2)),urcrnrlon=min(180,Mlon+londif*(SCALING_FACTOR/2)),urcrnrlat=min(90,Mlat+latdif*(SCALING_FACTOR/2)),resolution='l',projection='cyl')
        else:
            MARGIN_DEGREES = 5 # on each side
            m = Basemap(llcrnrlon=lons[0]-MARGIN_DEGREES,llcrnrlat=lats[0]-MARGIN_DEGREES,urcrnrlon=lons[0]+MARGIN_DEGREES,urcrnrlat=lats[0]+MARGIN_DEGREES,resolution='l',projection='cyl')
        x,y = m(lons,lats)
        m.fillcontinents()
        m.drawcountries(color='w')
        m.plot(x,y,marker='.',color='tab:blue',mfc='k',mec='k',linewidth=1,markersize=2.5)
    else:
        m=Basemap(resolution='l',projection='cyl')
        m.fillcontinents()
        m.drawcountries(color='w')
    img_buf = io.BytesIO()
    plt.savefig(img_buf,format='png',bbox_inches='tight')
    img_buf.seek(0)
    plt.clf()
    return img_buf.read()

# class ThreadWithReturnValue(Thread):
    
#     def __init__(self, group=None, target=None, name=None,
#                  args=(), kwargs={}, Verbose=None):
#         Thread.__init__(self, group, target, name, args, kwargs)
#         self._return = None

#     def run(self):
#         if self._target is not None:
#             self._return = self._target(*self._args,
#                                                 **self._kwargs)
#     def join(self, *args):
#         Thread.join(self, *args)
#         return self._return

class Selector(discord.ui.Select):
    def __init__(self,messages,options,placeholder):
        self.messages=messages
        super().__init__(placeholder=placeholder,max_values=1,min_values=1,options=options,disabled=1)
    async def callback(self, interaction:discord.Interaction):
        interaction.message.embeds[0].description=self.messages[int(self.values[0])] if len(self.values) else "Choose a command group."
        await interaction.response.edit_message(embeds=interaction.message.embeds,view=self.view)

class Help(discord.ui.View):
    def __init__(self,help_messages,command_messages,author):
        super().__init__(timeout=None)
        self.command_messages=command_messages
        self.help_messages=help_messages
        self.add_item(Selector(command_messages,[discord.SelectOption(label="Settings",description="/set commands",value="0"),discord.SelectOption(label="Features",description="/add and /remove commands",value="1"),discord.SelectOption(label="Stats",description="/stats commands",value="2"),discord.SelectOption(label="Other Commands",description="Other commands",value="3")],"Choose a command group"))
        self.author=author
    async def interaction_check(self,interaction):
        return interaction.user.id==self.author

    @discord.ui.select(placeholder="Choose a category",max_values=1,min_values=1,options=[discord.SelectOption(label="Setup",description="Bot setup instructions",value="0"),discord.SelectOption(label="Rules",description="Rules of the game",value="1"),discord.SelectOption(label="Commands",description="Bot commands",value="2"),discord.SelectOption(label="Emojis",description="Meaning of emojis under messages",value="3"),discord.SelectOption(label="Tips",description="Tips on playing the game",value="4"),discord.SelectOption(label="FAQ",description="Frequently-asked questions",value="5")])
    async def categoryMenu(self, interaction,select):
        interaction.message.embeds[0].title = {"0":"Setup Instructions","1":"Rules","2":"Commands Guide","3":"Emoji Guide","4":"Tips","5":"Frequently-Asked Questions"}[select.values[0]]
        if select.values[0]=='2':
            self.children[1].disabled=False
            await self.children[1].callback(interaction)
        else:
            self.children[1].disabled=True
            interaction.message.embeds[0].description=self.help_messages[int(select.values[0])]
            await interaction.response.edit_message(embeds=interaction.message.embeds,view=self)

class Paginator(discord.ui.View):
    def __init__(self,page,blist,title,lens,user,embed, other_footer_text = ""):
        super().__init__(timeout=None)
        self.page=page
        self.blist=blist
        self.title=title
        self.lens=lens
        self.author=user
        self.embed=embed
        self.footer_text = other_footer_text
        self.update_buttons()

    # async def interaction_check(self,interaction):
    #     return interaction.user.id==self.author
    
    def update_buttons(self):
        if self.lens==1:
            for i in self.children:
                i.disabled=True
            self.embed.set_footer(text="Page %s/%s%s"%(1,1,self.footer_text))
        elif self.page<=1:
            self.page=1
            self.children[0].disabled=True
            self.children[1].disabled=True
            self.embed.set_footer(text="Page %s/%s%s"%(1,self.lens,self.footer_text))
            self.children[3].disabled=False
            self.children[4].disabled=False
        elif self.page>=self.lens:
            self.page=self.lens
            self.children[0].disabled=False
            self.children[1].disabled=False
            self.embed.set_footer(text="Page %s/%s%s"%(self.lens,self.lens,self.footer_text))
            self.children[3].disabled=True
            self.children[4].disabled=True
        else:
            for i in self.children:
                i.disabled=False
            self.embed.set_footer(text="Page %s/%s%s"%(self.page,self.lens,self.footer_text))

    async def updateembed(self,interaction:discord.Interaction):
        has_author = self.embed.author
        self.embed=discord.Embed(title=self.title, color=GREEN,description='\n'.join(self.blist[self.page*25-25:self.page*25]))
        self.update_buttons()
        if has_author:
            if interaction.guild.icon:
                self.embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
            else:
                self.embed.set_author(name=interaction.guild.name)
        await interaction.response.edit_message(embed=self.embed,view=self, attachments=interaction.message.attachments)
        self.message=await interaction.original_response()
        
    @discord.ui.button(emoji='\N{BLACK LEFT-POINTING DOUBLE TRIANGLE WITH VERTICAL BAR}\N{VARIATION SELECTOR-16}', style=discord.ButtonStyle.primary)
    async def front(self, interaction, button):
        self.page=1
        await self.updateembed(interaction)
    @discord.ui.button(emoji='\N{BLACK LEFT-POINTING TRIANGLE}\N{VARIATION SELECTOR-16}', style=discord.ButtonStyle.primary)
    async def prev(self, interaction, button):
        self.page=self.page-1
        await self.updateembed(interaction)
    @discord.ui.button(emoji='\N{OCTAGONAL SIGN}', style=discord.ButtonStyle.danger)
    async def stop(self, interaction, button):
        for i in self.children:
            i.disabled=True
        await interaction.response.edit_message(embeds=interaction.message.embeds,view=self, attachments=interaction.message.attachments)
    @discord.ui.button(emoji='\N{BLACK RIGHT-POINTING TRIANGLE}\N{VARIATION SELECTOR-16}', style=discord.ButtonStyle.primary)
    async def next(self, interaction, button):
        self.page=self.page+1
        await self.updateembed(interaction)
    @discord.ui.button(emoji='\N{BLACK RIGHT-POINTING DOUBLE TRIANGLE WITH VERTICAL BAR}\N{VARIATION SELECTOR-16}', style=discord.ButtonStyle.primary)
    async def back(self, interaction, button):
        self.page=self.lens
        await self.updateembed(interaction)

class Confirmation(discord.ui.View):
    def __init__(self,serverid,author):
        super().__init__(timeout=60)
        self.guild=serverid
        self.author=author
        self.to=True
    async def on_timeout(self):
        self.children[0].disabled=True
        self.children[1].disabled=True
        if self.to:
            await self.message.edit(embed=discord.Embed(color=RED,description='Interaction timed out. Server stats have not been reset.'),view=self)
    
    async def interaction_check(self,interaction):
        return interaction.user.id==self.author

    @discord.ui.button(label='Yes', style=discord.ButtonStyle.green)
    async def yes(self, interaction:discord.Interaction, button):
        if interaction.user.id==self.message.interaction.user.id:
            self.children[0].disabled=True
            self.children[1].disabled=True
            self.to=False
            guildid = interaction.guild_id
            cur.execute("DELETE FROM chain_info WHERE server_id = ?", (guildid,))
            cur.execute("DELETE FROM count_info WHERE server_id = ?", (guildid,))
            cur.execute("SELECT user_id, correct, incorrect, score FROM server_user_info WHERE server_id = ?", (guildid,))
            for i in cur.fetchall():
                cur.execute("SELECT correct, incorrect, score FROM global_user_info WHERE user_id = ?", (i[0],))
                j=cur.fetchone()
                if (i[1]-j[0]==0) and (i[2]-j[1]==0) and (i[3]-j[2]==0):
                    cur.execute("DELETE FROM global_user_info WHERE user_id = ?", (i[0],))
                else:
                    cur.execute("SELECT last_active FROM server_user_info WHERE user_id = ? AND server_id != ? ORDER BY last_active DESC", (i[0], guildid))
                    la=cur.fetchone()[0]
                    cur.execute("UPDATE global_user_info SET correct = ?, incorrect = ?, score = ?, last_active = ? WHERE user_id = ?", (j[0]-i[1], j[1]-i[2], j[2]-i[3], la, i[0]))
            cur.execute("DELETE FROM server_user_info WHERE server_id = ?", (guildid,))
            cur.execute("DELETE FROM react_info WHERE server_id = ?", (guildid,))
            cur.execute("DELETE FROM repeat_info WHERE server_id = ?", (guildid,))
            cur.execute("UPDATE server_info SET round_number = ?, current_letter = ?, last_user = ?, max_chain = ?, last_best = ?, chain_end = ?, last_message = ? WHERE server_id = ?", (0, '-', None, 0, None, True, 0, guildid))
            cache[guildid]["round_number"] = 0
            cache[guildid]["current_letter"] = '-'
            cache[guildid]["last_user"] = None
            cache[guildid]["max_chain"] = 0
            cache[guildid]["last_best"] = None
            cache[guildid]["chain_end"] = True
            cache[guildid]["last_message"] = 0

            conn.commit()
            await interaction.response.edit_message(embed=discord.Embed(color=GREEN,description='Server stats have been reset. Choose any city to continue.'),view=self)
            self.message = await interaction.original_response()
    @discord.ui.button(label='No', style=discord.ButtonStyle.red)
    async def no(self, interaction, button):
        if interaction.user.id==self.message.interaction.user.id:
            self.children[0].disabled=True
            self.children[1].disabled=True
            self.to=False
            await interaction.response.edit_message(embed=discord.Embed(color=RED,description='Server stats have not been reset.'),view=self)
            self.message = await interaction.original_response()

owner=None
cur.execute("SELECT server_id FROM server_info")
processes = {}
# 1 mutex per server
processes_mutexes = {}
processes_cvs = {}
for (server_id,) in cur.fetchall():
    processes[server_id] = []
    # processes_mutexes[server_id] = Lock()
    # processes_cvs[server_id] = Condition(processes_mutexes[server_id])

# # mutexes for sql tables - dict of mutexes
# sql_table_mutexes = dict()
# cur.execute("SHOW TABLES")
# for (table,) in cur.fetchall():
#     sql_table_mutexes[table] = Lock()
#     print(table)

async def timed_unblock(server_id, user_id, dt, is_global):
    expire_dt = dt
    if expire_dt > datetime.datetime.now():
        timeout = expire_dt - datetime.datetime.now()
        await asyncio.sleep(timeout.total_seconds())
    unblock(server_id, user_id, is_global)

@client.event
async def on_ready():
    global owner
    await tree.sync()
    await tree.sync(guild=discord.Object(1126556064150736999))
    app_info = await client.application_info()
    owner = await client.fetch_user(app_info.team.owner_id)
    max_ages.update({i.id:0 for i in client.guilds if i.id not in max_ages})
    # prepare github messages
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.github.com/repos/GlutenFreeGrapes/cities-chain/commits",params={"since":(datetime.datetime.now(tz=pytz.utc)-datetime.timedelta(minutes=20)).isoformat()}) as resp:
            json_response = await resp.json()
    cur.execute("SELECT server_id FROM server_info")
    alr={i for (i,) in cur.fetchall()}
    empty={i.id for i in client.guilds}-alr
    for i in empty:
        cur.execute("INSERT INTO server_info(server_id) VALUES (?)", (i,))

    server_info = pl.read_database("SELECT * FROM server_info", conn)
    server_info_dicts = server_info.to_dicts()
    cache = {}
    for d in server_info_dicts:
        guildid = d["server_id"]
        del d["server_id"]
        cache[guildid] = d
    conn.commit()
    # timed blocks

    cur.execute("SELECT server_id, user_id, block_expiry FROM server_user_info WHERE blocked = ? AND block_expiry > ?", (True, 0))
    for i in cur.fetchall():
        await timed_unblock(i[0],i[1],i[2],False)

    cur.execute("SELECT user_id, block_expiry FROM global_user_info WHERE blocked = ? AND block_expiry > ?", (True, 0))
    for i in cur.fetchall():
        await timed_unblock(0, i[0],i[1],True)
    commit_list = [(datetime.datetime.fromisoformat(i['commit']['author']['date']),i['commit']['message']) for i in json_response][::-1]
    embeds=[]
    for timestamp,message in commit_list:
        message_split = message.split('\n\n')
        header, body = message_split[0],'\n\n'.join(message_split[1:])
        embed = discord.Embed(color=GREEN,title=header,description=body,timestamp=timestamp)
        embed.set_footer(text='To disable these updates, use /set updates. To make suggestions, use the links in the /about command.')
        embeds.append(embed)
    
    if len(embeds):
        cur.execute("SELECT server_id, channel_id FROM server_info WHERE updates = ?", (True,))
        guild_to_channel = {i[0]:i[1] for i in cur.fetchall()}
        for g in client.guilds:
            try:
                client_member = g.get_member(client.user.id)
            except:
                client_member = None
                logging.getLogger().info(f"unable to write to fetch client user in server {g.id} ({g.name})")
            if g.id in guild_to_channel:
                # guild has updates on
                id_to_channel = {c.id:c for c in g.text_channels}
                if guild_to_channel[g.id] in id_to_channel:
                    # channel set up and channel exists in server
                    chosen_channel = id_to_channel[guild_to_channel[g.id]]
                else:
                    chosen_channel = None
                    # first one with messaging permissions
                    if client_member:
                        for c in id_to_channel.values():
                            if c.permissions_for(client_member).send_messages:
                                chosen_channel = c
                                break
                if chosen_channel:
                    try:
                        await chosen_channel.send(embeds=embeds)
                        logging.getLogger().info(f"successfully sent to channel {chosen_channel.id} ({chosen_channel.name}) of server {g.id} ({g.name})")
                    except:
                        logging.getLogger().info(f"unable to write to channel {chosen_channel.id} ({chosen_channel.name}) of server {g.id} ({g.name})")
    print(f'Logged in as {client.user} (ID: {client.user.id})\n------')

@client.event
async def on_guild_join(guild:discord.Guild):
    global processes
    processes[guild.id]=[]

    cur.execute("SELECT * FROM server_info WHERE server_id = ?", (guild.id,))
    if cur.rowcount==0:
        cur.execute("INSERT INTO server_info(server_id) VALUES (?)", (guild.id,))
        server_info_new = pl.read_database("SELECT * FROM server_info WHERE server_id = ?", conn, execute_options={"parameters":(guild.id,)})
        d = server_info_new.to_dicts()[0]
        del d["server_id"]
        cache[guild.id] = d
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            await channel.send('Hi! use /help to get more information on how to use this bot. ')
            break

mod_perms = discord.Permissions(moderate_members=True)

assign = app_commands.Group(name="set", description="Set different things for the chain.", guild_only=True, default_permissions=mod_perms)
@assign.command(description="Sets the channel for the bot to monitor for cities chain.")
@app_commands.describe(channel="The channel where the cities chain will happen")
async def channel(interaction: discord.Interaction, channel: discord.TextChannel|discord.Thread|discord.ForumChannel):
    await interaction.response.defer()
    if not interaction.guild_id:
        await interaction.followup.send('`channel` must be a channel in a server.')
        return
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute("UPDATE server_info SET channel_id = ? WHERE server_id = ?", (channel.id,interaction.guild_id))
    cache[interaction.guild_id]["channel_id"] = channel.id
    conn.commit()
    await interaction.followup.send('Channel set to <#%s>.'%channel.id)

@assign.command(description="Sets the gap between when a city can be repeated in the chain.")
@app_commands.describe(num="The minimum number of cities before they can repeat again, set to -1 to disallow any repeats")
async def repeat(interaction: discord.Interaction, num: app_commands.Range[int,-1,2500]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    if cache[interaction.guild_id]["chain_end"]:
        if num==-1:
            cur.execute("UPDATE server_info SET repeats = ? WHERE server_id = ?", (False, interaction.guild_id))
            cache[interaction.guild_id]["repeats"] = False
            await interaction.followup.send('Repeats set to **OFF**. ')
        else:
            if cache[interaction.guild_id]["repeats"]:
                await interaction.followup.send('Minimum number of cities before repeating set to **%s**.'%f'{num:,}')
            else:
                await interaction.followup.send('Repeats set to **ON**. ')
                await interaction.channel.send('Minimum number of cities before repeating set to **%s**.'%f'{num:,}')
            cur.execute("UPDATE server_info SET repeats = ?, min_repeat = ? WHERE server_id = ?", (True, num, interaction.guild_id))
            cache[interaction.guild_id]["repeats"] = True
            cache[interaction.guild_id]["min_repeat"] = num
        conn.commit()
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@assign.command(description="Sets the minimum population of cities in the chain.")
@app_commands.describe(population="The minimum population of cities in the chain")
async def population(interaction: discord.Interaction, population: app_commands.Range[int,1,1000000]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    if cache[interaction.guild_id]["chain_end"]:
        cur.execute("UPDATE server_info SET min_pop = ? WHERE server_id = ?", (population, interaction.guild_id))
        cache[interaction.guild_id]["min_pop"] = population
        conn.commit()
        await interaction.followup.send('Minimum city population set to **%s**.'%f'{population:,}')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@assign.command(description="Sets the prefix to listen to.")
@app_commands.describe(prefix="Prefix that all cities to be chained must begin with")
async def prefix(interaction: discord.Interaction, prefix: Optional[app_commands.Range[str,0,10]]=''):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    if cache[interaction.guild_id]["chain_end"]:
        cur.execute("UPDATE server_info SET prefix = ? WHERE server_id = ?", (prefix, interaction.guild_id))
        cache[interaction.guild_id]["prefix"] = prefix
        conn.commit()
        if prefix!='':
            await interaction.followup.send('Prefix set to **%s**.'%prefix)
        else:
            await interaction.followup.send('Prefix removed.')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@assign.command(name='choose-city',description="Toggles if bot can choose starting city for the next chain.")
@app_commands.describe(option="on to let the bot choose the next city, off otherwise")
async def choosecity(interaction: discord.Interaction, option:Literal["on","off"]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    if cache[interaction.guild_id]["chain_end"]:
        if option=='on':
            if cache[interaction.guild_id]["choose_city"]:
                await interaction.followup.send('Choose_city is already **on**.')
            else:
                # minimum population
                poss=city_default.filter((pl.col('population') >= cache[interaction.guild_id]["min_pop"]) & (~pl.col('deleted').cast(pl.Boolean)))
                # not blacklisted/are whitelisted
                if cache[interaction.guild_id]["list_mode"]:
                    countrieslist={i for i in cache[interaction.guild_id]["list"].split(',') if i}
                    if len(countrieslist):
                        if cache[interaction.guild_id]["list_mode"]==1:
                            poss=poss.filter(~(pl.col('country').is_in(countrieslist)|pl.col('alt-country').is_in(countrieslist)))
                        else:
                            poss=poss.filter(pl.col('country').is_in(countrieslist)|pl.col('alt-country').is_in(countrieslist))
                if poss.shape[0]:
                    entr=poss.sample(1).row(0, named=True)
                    nname=entr['name']
                    newid=entr['geonameid']
                    n=(nname,iso2[entr['country']],entr['country'],admin1name(entr['country'],entr['admin1']),admin2name(entr['country'],entr['admin1'],entr['admin2']),entr['alt-country'])
                    
                    new_message = await interaction.followup.send('Choose_city set to **ON**. Next city is `%s` (next letter `%s`).'%(nname,entr['last-letter']))
                    cur.execute("UPDATE server_info SET choose_city = ?, current_letter = ?, last_message = ? WHERE server_id = ?", (True, entr['last-letter'], new_message.id, guildid))
                    cache[interaction.guild_id]["choose_city"] = True
                    cache[interaction.guild_id]["current_letter"] = entr['last-letter']
                    cache[interaction.guild_id]["last_message"] = new_message.id
                    cur.execute("INSERT INTO chain_info(server_id, city_id, round_number, count, name, admin2, admin1, country, country_code, alt_country, time_placed, valid, leaderboard_eligible, message_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (guildid, newid, cache[interaction.guild_id]["round_number"]+1, 1, n[0], n[4], n[3], n[1], n[2], n[5], interaction.created_at, True, True, new_message.id))
                else:
                    await interaction.followup.send('Either your population requirement is too high or your country list settings are too restrictive. Fix those and try again.')
        else:
            cur.execute("UPDATE server_info SET choose_city = ?, current_letter = ?, last_message = ? WHERE server_id = ?", (False, '-', 0, guildid))
            cache[interaction.guild_id]["choose_city"] = False
            cache[interaction.guild_id]["current_letter"] = '-'
            cache[interaction.guild_id]["last_message"] = 0
            cur.execute("DELETE FROM chain_info WHERE server_id = ? AND round_number = ?", (guildid, cache[interaction.guild_id]["round_number"]+1))
            await interaction.followup.send('Choose_city set to **OFF**. Choose the next city to start the chain. ')
        conn.commit()
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@assign.command(name='country-list-mode',description="Sets the mode of the server's list of countries.")
@app_commands.describe(mode="Usage for the list of this server's countries")
async def listmode(interaction: discord.Interaction, mode: Literal['blacklist','whitelist','disabled']):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    if cache[interaction.guild_id]["chain_end"]:
        if mode=='disabled':
            m=0
        elif mode=='blacklist':
            m=1
        else:
            m=2
        cur.execute("UPDATE server_info SET list_mode = ? WHERE server_id = ?", (m, guildid))
        cache[guildid]["list_mode"] = m
        conn.commit()
        await interaction.followup.send('List mode set to **%s**.'%mode)
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@assign.command(description="Toggles if bot can send updates when it restarts.")
@app_commands.describe(option="on to let the bot send updates, off otherwise")
async def updates(interaction: discord.Interaction, option:Literal["on","off"]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    cur.execute("UPDATE server_info SET updates = ? WHERE server_id = ?", (option=='on', guildid))
    cache[guildid]["updates"] = (option=='on')
    await interaction.followup.send(f'Updates set to **{option.upper()}**.')
    conn.commit()

@assign.command(description="""Toggles if bot can reply "it's ok" to messages in this server""")
@app_commands.describe(option="on to let the bot send these kinds of messages, off otherwise")
async def nice(interaction: discord.Interaction, option:Literal["on","off"]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    cur.execute("UPDATE server_info SET nice = ? WHERE server_id = ?", (option=='on', guildid))
    cache[guildid]["nice"] = (option=='on')
    await interaction.followup.send(f'Nice set to **{option.upper()}**.')
    conn.commit()

@assign.command(description="Toggles if the bot will react to messages with flag and medal emojis")
@app_commands.describe(option="on to let the bot add these reactions, off otherwise")
async def emojis(interaction: discord.Interaction, option:Literal["on","off"]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    cur.execute("UPDATE server_info SET emoji = ? WHERE server_id = ?", (option=='on', guildid))
    cache[guildid]["emoji"] = (option=='on')
    await interaction.followup.send(f'Flag and medal emojis set to **{option.upper()}**.')
    conn.commit()

async def countrycomplete(interaction: discord.Interaction, search: str):
    await interaction.response.defer()
    if search=='':
        return []
    s=search.lower()
    results=[i for i in allcountries if i.lower().startswith(s)]
    results.extend([iso2[i] for i in iso2 if i.lower().startswith(s) and iso2[i] not in results])
    results.extend([iso3[i] for i in iso3 if i.lower().startswith(s) and iso3[i] not in results])
    return [app_commands.Choice(name=i,value=i) for i in results[:10]]

add = app_commands.Group(name='add', description="Adds features for the chain.", guild_only=True, default_permissions=mod_perms)
@add.command(description="Adds reaction for a city. When prompted, react to client's message with emoji to react to city with.")
@app_commands.describe(city="The city that the client will react to",province="State, province, etc that the city is located in",otherp='Subdivision of state/province, like a county within a state',country="Country the city is located in")
@app_commands.rename(province='administrative-division', otherp='administrative-division-2')
@app_commands.autocomplete(country=countrycomplete)
async def react(interaction: discord.Interaction, city:str, province:Optional[str]=None, otherp:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    
    minimum_population,country_list_mode,country_list = cache[interaction.guild_id]["min_pop"],cache[interaction.guild_id]["list_mode"],cache[interaction.guild_id]["list"]

    res=search_cities_command(city,province,otherp,country,minimum_population,0,country_list_mode,country_list)
    if res:
        await interaction.followup.send('What reaction do you want for **%s**? React to this message with the emoji. '%res[2])
        msg = await interaction.original_response()
        def check(reaction, user):
            return reaction.message.id == msg.id and user == interaction.user
        cur.execute("SELECT city_id FROM react_info WHERE server_id = ?", (interaction.guild_id,))
        if cur.rowcount>0:
            reactset=set(cur.fetchall())
        else:
            reactset=set()
        try:
            reaction,user=await client.wait_for('reaction_add', check=check,timeout=60)
            if (res[0],) in reactset:
                cur.execute("UPDATE react_info SET reaction = ? WHERE server_id = ? AND city_id = ?", (str(reaction.emoji), interaction.guild_id,res[0]))
            else:
                cur.execute("INSERT react_info(server_id, city_id, reaction) VALUES (?, ?, ?)", (interaction.guild_id, res[0], str(reaction.emoji)))
            conn.commit()
            await interaction.edit_original_response(content='%s successfully added as reaction for **%s**. '%(reaction.emoji,res[2]))
        except asyncio.TimeoutError:
            if city in reactset:
                await interaction.edit_original_response(content='Interaction timed out. Previous reaction kept. ')
            else:
                await interaction.edit_original_response(content='Interaction timed out. No reaction added. ')
    else:
        await interaction.followup.send('City not recognized. Please try again. ')

@add.command(description="Adds repeating exception for a city.")
@app_commands.describe(city="The city that the client will allow repeats for",province="State, province, etc that the city is located in",otherp='Subdivision of state/province, like a county within a state',country="Country the city is located in")
@app_commands.rename(province='administrative-division', otherp='administrative-division-2')
@app_commands.autocomplete(country=countrycomplete)
async def repeat(interaction: discord.Interaction, city:str, province:Optional[str]=None, otherp:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return

    if cache[interaction.guild_id]["chain_end"]:
        res=search_cities_command(city,province,otherp,country,cache[interaction.guild_id]["min_pop"],0,cache[interaction.guild_id]["list_mode"],cache[interaction.guild_id]["list"])
        if res:
            cur.execute("SELECT city_id FROM repeat_info WHERE server_id = ?", (interaction.guild_id,))
            if cur.rowcount>0:
                repeatset=set(cur.fetchall())
            else:
                repeatset=set()
            if (res[0],) not in repeatset:
                cur.execute("INSERT INTO repeat_info(server_id, city_id) VALUES (?, ?)", (interaction.guild_id, res[0]))
                conn.commit()
            await interaction.followup.send('**%s** can now be repeated. '%res[2])
        else:
            await interaction.followup.send('City not recognized. Please try again. ')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@add.command(name='country-list',description="Adds country to the whitelist/blacklist.")
@app_commands.describe(country="Country to add to the list")
@app_commands.autocomplete(country=countrycomplete)
async def countrylist(interaction: discord.Interaction, country:str):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    
    if cache[interaction.guild_id]["chain_end"]:
        #search countries
        countrysearch=country.lower().strip()
        res=countriesdata.filter((pl.col('name').str.to_lowercase()==countrysearch)|(pl.col('country').str.to_lowercase()==countrysearch))
        if res.shape[0]:
            res = res.row(0, named=True)
            country_list=cache[interaction.guild_id]["list"]
            dname = countriesdata.row(by_predicate=(pl.col('default')==1)&(pl.col('geonameid')==res['geonameid']))[countriesdata.get_column_index('name')]
            if res['country'] not in country_list.split(','):
                if country_list=='':
                    new_list=res['country']
                else:
                    new_list=country_list+','+res['country']
                cur.execute("UPDATE server_info SET list = ? WHERE server_id = ?",(new_list, interaction.guild_id))
                cache[interaction.guild_id]["list"] = new_list
                await interaction.followup.send('**%s %s (%s)** added to the list. '%(flags[res['country']],dname,res['country']))
            else:
                await interaction.followup.send('**%s %s (%s)** already in the list.'%(flags[res['country']],dname,res['country']))
        else:
            await interaction.followup.send('Country not recognized. Please try again. ')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

remove = app_commands.Group(name='remove', description="Removes features from the chain.", guild_only=True, default_permissions=mod_perms)
@remove.command(description="Removes reaction for a city.")
@app_commands.describe(city="The city that the client will not react to",province="State, province, etc that the city is located in",otherp='Subdivision of state/province, like a county within a state',country="Country the city is located in")
@app_commands.rename(province='administrative-division', otherp='administrative-division-2')
@app_commands.autocomplete(country=countrycomplete)
async def react(interaction: discord.Interaction, city:str, province:Optional[str]=None, otherp:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    
    minimum_population,country_list_mode,country_list = cache[interaction.guild_id]["min_pop"],cache[interaction.guild_id]["list_mode"],cache[interaction.guild_id]["list"]

    res=search_cities_command(city,province,otherp,country,minimum_population,1,country_list_mode,country_list)
    if res:
        try:
            cur.execute("DELETE FROM react_info WHERE server_id = ? AND city_id = ?", (interaction.guild_id, res[0]))
            conn.commit()
            await interaction.followup.send('Reaction for %s removed.'%res[2])
        except:
            await interaction.followup.send('%s had no reactions.'%res[2])
    else:
        await interaction.followup.send('City not recognized. Please try again. ')

@remove.command(description="Removes repeating exception for a city.")
@app_commands.describe(city="The city that the client will disallow repeats for",province="State, province, etc that the city is located in",otherp='Subdivision of state/province, like a county within a state',country="Country the city is located in")
@app_commands.rename(province='administrative-division', otherp='administrative-division-2')
@app_commands.autocomplete(country=countrycomplete)
async def repeat(interaction: discord.Interaction, city:str, province:Optional[str]=None, otherp:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return

    if cache[interaction.guild_id]["chain_end"]:
        res=search_cities_command(city,province,otherp,country,cache[interaction.guild_id]["min_pop"],1,cache[interaction.guild_id]["list_mode"],cache[interaction.guild_id]["list"])
        if res:
            try:
                cur.execute("DELETE FROM repeat_info WHERE server_id = ? AND city_id = ?", (interaction.guild_id,res[0]))
                conn.commit()
                await interaction.followup.send('Repeating for %s removed.'%res[2])
            except:
                await interaction.followup.send('%s had no repeats.'%res[2])
        else:
            await interaction.followup.send('City not recognized. Please try again. ')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@remove.command(name='country-list',description="Removes country from the whitelist/blacklist.")
@app_commands.describe(country="Country to remove from the list")
@app_commands.autocomplete(country=countrycomplete)
async def countrylist(interaction: discord.Interaction, country:str):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return

    if cache[interaction.guild_id]["chain_end"]:
        #search countries
        countrysearch=country.lower().strip()
        res=countriesdata.filter((pl.col('name').str.to_lowercase()==countrysearch)|(pl.col('country').str.to_lowercase()==countrysearch))
        if res.shape[0]:
            res = res.row(0, named=True)
            country_list=cache[interaction.guild_id]["list"]
            dname = countriesdata.row(by_predicate=(pl.col('default')==1)&(pl.col('geonameid')==res['geonameid']))[countriesdata.get_column_index('name')]
            if res['country'] in country_list.split(','):
                new_list=','.join([i for i in country_list.split(',') if i.strip() and i!=res['country']])
                cur.execute("UPDATE server_info SET list = ? WHERE server_id = ?", (new_list, interaction.guild_id))
                cache[interaction.guild_id]["list"] = new_list
                conn.commit()
                await interaction.followup.send('**%s %s (%s)** removed from the list. '%(flags[res['country']],dname,res['country']))
            else:
                await interaction.followup.send('**%s %s (%s)** is not in the list.'%(flags[res['country']],dname,res['country']))
        else:
            await interaction.followup.send('Country not recognized. Please try again. ')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@client.event
async def on_message_delete(message:discord.Message):
    if message.guild:
        guildid = message.guild.id
        # check for last message equal to this one
        if cache[guildid]["last_message"] == message.id:
            cur.execute("SELECT name, valid FROM chain_info WHERE message_id = ?", (message.id,))
            if cur.rowcount:
                (name,valid)=cur.fetchone()
                if valid:
                    await message.channel.send("<@%s>'s message was deleted. The original city is `%s` (next letter `%s`)."%(cache[guildid]["last_user"],name,cache[guildid]["current_letter"]))
@client.event
async def on_message_edit(message:discord.Message, after:discord.Message):
    if message.guild and not message.edited_at:
        guildid = message.guild.id
        if cache[guildid]["last_message"] == message.id:
            cur.execute("SELECT name, valid FROM chain_info WHERE message_id = ?",(message.id,))
            if cur.rowcount:
                (name,valid)=cur.fetchone()
                if valid:
                    await message.channel.send("<@%s>'s message was edited. The original city is `%s` (next letter `%s`)."%(cache[guildid]["last_user"],name,cache[guildid]["current_letter"]))

RESPOND_WORDS = {"m(y? ?)?b(ad)?", "(w(h?))?oops(y|ie)?", "so*r+y+", "sow+y+"}
@client.event
async def on_message(message:discord.Message):
    content = message.content
    global processes
    authorid=message.author.id
    if message.guild and authorid!=client.user.id:
        guildid=message.guild.id
        channel_id, prefix=cache[guildid]["channel_id"], cache[guildid]["prefix"]
        if message.channel.id==channel_id and not message.author.bot:
            if content.strip().startswith(prefix) and len(sanitize_query(message.content[len(prefix):])):
                if is_blocked(authorid,guildid):
                    try:
                        cur.execute("SELECT blocked FROM global_user_info WHERE user_id = ?", (authorid,))
                        if cur.fetchone()[0]:
                            await message.author.send("You are blocked from using this bot.")
                        else:
                            await message.author.send("You are blocked from using this bot in `%s`."%message.guild.name)
                        await message.delete()
                    except:
                        try:
                            await message.add_reaction('\N{NO PEDESTRIANS}')
                        except:
                            await message.reply('\N{NO PEDESTRIANS} User is blocked from using this bot.')
                    return
                msgref = discord.MessageReference.from_message(message,fail_if_not_exists=0)
                sanitized_query = sanitize_query(message.content[len(prefix):])
                args = (sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])
                index_in_process=len(processes[guildid])
                # with chain_pool as chain_pool:
                if index_in_process: 
                    # processes[guildid].append((message,guildid,authorid,content,msgref))
                    # processes[guildid].append((message,guildid,authorid,content,msgref,client.loop.run_in_executor(chain_pool, search_cities, *args)))
                    # processes[guildid].append((message,guildid,authorid,content,msgref,chain_pool.submit(search_cities,*args)))

                    processes[guildid].append((message,guildid,authorid,content,msgref,asyncio.to_thread(search_cities,*args)))
                else:
                    # processes[guildid]=[(message,guildid,authorid,content,msgref)]
                    # processes[guildid]=[(message,guildid,authorid,content,msgref,client.loop.run_in_executor(chain_pool, search_cities, *args))]
                    # processes[guildid]=[(message,guildid,authorid,content,msgref,chain_pool.submit(search_cities,*args))]
                    processes[guildid]=[(message,guildid,authorid,content,msgref,asyncio.to_thread(search_cities,*args))]
                # IF THERE IS A CITY BEING PROCESSED, ADD IT TO THE QUEUE AND EVENTUALLY IT WILL BE REACHED. OTHERWISE PROCESS IMMEDIATELY WHILE KEEPING IN MIND THAT IT IS CURRENTLY BEING PROCESSED
                # does city exist
                # res = pool.apply_async(search_cities,(sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])).get()
                # res = await asyncio.to_thread(search_cities,sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])
                # res = await asyncio.create_task(asyncio.to_thread(search_cities,sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"]))
                # res = await asyncio.create_task(search_cities(sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"]))
                # res = await client.loop.create_task(search_cities(sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"]))
                # res = await run_blocking(search_cities, sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])
                # res = await asyncio.get_running_loop().run_in_executor(chain_pool, search_cities,sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])
                # with concurrent.futures.ProcessPoolExecutor() as pool:
                #     # res = await client.loop.run_in_executor(pool, search_cities,sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])
                #     future = pool.submit(search_cities,sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"])
                #     if index_in_process==0:
                #         done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                # processes[guildid][index_in_process] += (res,)
                # search_thread = ThreadWithReturnValue(target=search_cities,args=(sanitized_query[0],sanitized_query[1:],cache[guildid]["min_pop"],0,cache[guildid]["list_mode"],cache[guildid]["list"]))
                # search_thread.start()
                # res = search_thread.join()

                # processes[guildid][index_in_process]+=(res,)

                # hastily bodged-together code to allow searching cities to continue without disturbing order
                # r = search_cities(*args)
                # for n in range(len(processes[guildid])):
                #     if processes[guildid][0][0].id == message.id:
                #         processes[guildid][n] += (r,)
                #         break
                if processes[guildid][0][0].id==message.id:#index_in_process==0:
                    await asyncio.create_task(process_chain(*processes[guildid][0]))
            elif cache[guildid]["nice"] and re.search(r"(?<!not )\b(("+')|('.join(RESPOND_WORDS)+r"))\b",message.content,re.I):
                await message.reply("it's ok")

async def process_chain(message:discord.Message,guildid,authorid,original_content,ref,res):
    res = await res

    round_num,chain_ended = cache[guildid]["round_number"], cache[guildid]["chain_end"]
    cur.execute("SELECT * FROM server_user_info WHERE user_id = ? AND server_id = ?", (authorid, guildid))
    if cur.rowcount==0:
        cur.execute("SELECT * FROM global_user_info WHERE user_id = ?", (authorid,))
        if cur.rowcount==0:
            cur.execute("INSERT INTO global_user_info(user_id) VALUES (?)", (authorid,))
        cur.execute("INSERT INTO server_user_info(user_id, server_id) VALUES (?, ?)",(authorid,guildid))
    if chain_ended:
        cur.execute("UPDATE server_info SET chain_end = ?, round_number = ?, leaderboard_eligible = ? WHERE server_id = ?", (False, round_num+1, True, guildid))
        cache[guildid]["chain_end"] = False
        cache[guildid]["round_number"] = round_num+1
        cache[guildid]["leaderboard_eligible"] = True

    if cache[guildid]["repeats"]:
        cur.execute("SELECT city_id, message_id, user_id FROM chain_info WHERE server_id = ? AND round_number = ? ORDER BY count DESC LIMIT ?", (guildid, cache[guildid]["round_number"], cache[guildid]["min_repeat"]))
    else:
        cur.execute("SELECT city_id, message_id, user_id FROM chain_info WHERE server_id = ? AND round_number = ? ORDER BY count DESC", (guildid, cache[guildid]["round_number"]))
    # run is leaderboard eligible
    if cur.rowcount:
        citieslist = {city:(message_id, author_id) for (city, message_id, author_id) in cur.fetchall()}
    #     l_eligible=l_eligibles[0]
    else:
        citieslist=[]
    #     l_eligible=1
    # l_eligible = cache[guildid]["leaderboard_eligible"]
    if not res:
        await fail(message,"**City not recognized.**",citieslist,None,None,False,ref)
        return
    
    # default names
    j=city_default.row(by_predicate=pl.col('geonameid') == res[0], named=True)
    name,adm2,adm1,country,altcountry=j['name'],j['admin2'],j['admin1'],j['country'],j['alt-country']
    if adm1:
        adm1=admin1name(country,adm1)
        if adm2:
            adm2=admin2name(country,j['admin1'],adm2)
    # city to string
    n=(res[2],(iso2[country],country),adm1,adm2,altcountry)
    letters=(res[1]['first-letter'],res[1]['last-letter'])

    # correct letter
    if not (cache[guildid]["current_letter"]=='-' or cache[guildid]["current_letter"]==letters[0]):
        await fail(message,"**Wrong letter.**",citieslist,res,n,True,ref)
        return
    
    # minimum population
    if cache[guildid]["min_pop"]>res[1]['population']:
        await fail(message,"**City must have a population of at least `%s`.**"%f"{cache[guildid]['min_pop']:,}",citieslist,res,n,True,ref)
        return
    
    # within repeats
    cur.execute("SELECT city_id FROM repeat_info WHERE server_id = ?", (guildid,))
    if cur.rowcount>0:
        repeatset={b[0] for b in cur.fetchall()}
    else:
        repeatset=set()
    # mark wrong if
    # not in repeat set and
    # - in past x cities if repeats on
    # - already said if repeats off
    if (res[0] not in repeatset) and ((cache[guildid]["repeats"] and res[0] in set(citieslist)) or (not cache[guildid]["repeats"] and res[0] in set(citieslist))):
        
        # find the repeated message id and its author
        repeated_message_id, repeated_author_id = citieslist[res[0]]
        # get link to message
        
        try:
            repeated_message = await message.channel.fetch_message(repeated_message_id)

            if cache[guildid]["repeats"]:
                await fail(message,"[**No repeats within `%s` cities.**](%s)"%(f"{cache[guildid]['min_repeat']:,}",repeated_message.jump_url),citieslist,res,n,True,ref)
            else:
                await fail(message,"[**No repeats.**](%s)"%repeated_message.jump_url,citieslist,res,n,True,ref)
        except: 
            if cache[guildid]["repeats"]:
                await fail(message,"**No repeats within `%s` cities.**"%(f"{cache[guildid]['min_repeat']:,}",),citieslist,res,n,True,ref)
            else:
                await fail(message,"**No repeats.**",citieslist,res,n,True,ref)
        return

    # country is not blacklisted/is whitelisted
    countrylist={i for i in cache[guildid]["list"].split(',') if i!=''}
    # accept if: 
    # list mode 0
    # list mode 1 (blacklist), reject either country being in the whitelist
    # list mode 2 (whitelist), accept either country being in the whitelist
    if (len(countrylist) and 
        ((cache[guildid]["list_mode"]==1 and (country in countrylist or altcountry in countrylist)) or 
         (cache[guildid]["list_mode"]==2 and (country not in countrylist and altcountry not in countrylist)))):
        if cache[guildid]["list_mode"]==1:
            await fail(message,f"**No using cities from the following countries: {','.join(['`%s`'%i for i in countrylist])}.**",citieslist,res,n,True,ref)
        elif cache[guildid]["list_mode"]==2:
            await fail(message,f"**Only use cities from the following countries: {','.join(['`%s`'%i for i in countrylist])}.**",citieslist,res,n,True,ref)
        return
    
    # last user
    if cache[guildid]["last_user"]==message.author.id:
        await fail(message,"**No going twice.**",citieslist,res,n,True,ref)
        return
        
    cur.execute("SELECT correct, score FROM server_user_info WHERE server_id = ? AND user_id = ?", (guildid, authorid))
    uinfo=cur.fetchone()
    cur.execute("UPDATE server_user_info SET correct = ?, score = ?, last_active = ? WHERE server_id = ? AND user_id = ?", (uinfo[0]+1, uinfo[1]+1, message.created_at, guildid, authorid))
    cur.execute("SELECT correct, score FROM global_user_info WHERE user_id = ?", (authorid,))
    uinfo=cur.fetchone()
    cur.execute("UPDATE global_user_info SET correct = ?, score = ?, last_active = ? WHERE user_id = ?", (uinfo[0]+1, uinfo[1]+1, message.created_at, authorid))
    cur.execute("UPDATE server_info SET last_user = ?, current_letter = ?, last_message = ? WHERE server_id = ?", (authorid, letters[1], message.id, guildid))
    cache[guildid]["last_user"] = authorid
    cache[guildid]["current_letter"] = letters[1]
    cache[guildid]["last_message"] = message.id
    
    if res[0] in set(citieslist):
        cur.execute("UPDATE server_info SET leaderboard_eligible = ? WHERE server_id = ?", (False,guildid))
        cache[guildid]["leaderboard_eligible"] = False
    cur.execute("INSERT INTO chain_info(server_id, user_id, round_number, count, city_id, name, admin2, admin1, country, country_code, alt_country, time_placed, valid, message_id, leaderboard_eligible) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (guildid, authorid, cache[guildid]["round_number"], len(citieslist)+1, res[0], n[0], n[3], n[2], n[1][0], n[1][1], n[4], message.created_at, True, message.id, (cache[guildid]["leaderboard_eligible"])))
    
    cur.execute("SELECT count FROM count_info WHERE server_id = ? AND city_id = ?", (guildid, res[0]))
    new_city = False
    if cur.rowcount==0:
        new_city = True
        cur.execute("INSERT INTO count_info(server_id, city_id, name, admin2, admin1, country, country_code, alt_country, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (guildid, res[0], name, n[3], n[2], n[1][0], n[1][1], n[4], 1))
    else:
        citycount = cur.fetchone()[0]
        cur.execute("UPDATE count_info SET count = ? WHERE server_id = ? AND city_id = ?", (citycount+1, guildid, res[0]))

    length = len(citieslist)+1
    new_high = False
    if cache[guildid]["max_chain"]<(length):
        current_time = message.created_at
        cur.execute("UPDATE server_info SET max_chain = ?, last_best = ? WHERE server_id = ?",(length, current_time, guildid))
        cache[guildid]["max_chain"] = length
        cache[guildid]["last_best"] = current_time
        new_high = True
    conn.commit()
    
    try:
        if new_high:
            await message.add_reaction('\N{BALLOT BOX WITH CHECK}')
        else:
            await message.add_reaction('\N{WHITE HEAVY CHECK MARK}')
        
        if not ((res[2].replace(' ','').isalpha() and res[2].isascii() and original_content[len(cache[guildid]["prefix"]):].find(',')<0)):
            await message.add_reaction(regionalindicators[letters[1]])
        
        if cache[guildid]["emoji"]:
            await message.add_reaction(regionalindicators[country[0].lower()]+regionalindicators[country[1].lower()])

            if (country=="GB"):
                if adm1=="Scotland":
                    await message.add_reaction("🏴󠁧󠁢󠁳󠁣󠁴󠁿")
                elif adm1=="Wales":
                    await message.add_reaction("🏴󠁧󠁢󠁷󠁬󠁳󠁿")

            if altcountry:
                await message.add_reaction(regionalindicators[altcountry[0].lower()]+regionalindicators[altcountry[1].lower()])
            cur.execute("SELECT reaction FROM react_info WHERE server_id = ? AND city_id = ?", (guildid,res[0]))
            if cur.rowcount>0:
                await message.add_reaction(cur.fetchone()[0])

            if new_city:
                await message.add_reaction('\N{FIRST PLACE MEDAL}')
    except:
        pass
    # remove this from the queue of messages to process
    processes[guildid].pop(0)
    # if queue of other cities to process empty, set to none again. otherwise, process next city
    if len(processes[guildid])>0:
        # check if next one is ready; if not, wait
        await asyncio.create_task(process_chain(*processes[guildid][0]))

async def fail(message:discord.Message,reason,citieslist,res,n,cityfound,msgref):
    guildid=message.guild.id
    authorid=message.author.id

    cur.execute("SELECT incorrect, score FROM server_user_info WHERE server_id = ? AND user_id = ?", (guildid, authorid))
    uinfo=cur.fetchone()
    cur.execute("UPDATE server_user_info SET incorrect = ?, score = ?, last_active = ? WHERE server_id = ? AND user_id = ?",(uinfo[0]+1,uinfo[1]-1,message.created_at,guildid,authorid))
    cur.execute("SELECT incorrect, score FROM global_user_info WHERE user_id = ?",(authorid,))
    uinfo=cur.fetchone()
    cur.execute("UPDATE global_user_info SET incorrect = ?, score = ?, last_active = ? WHERE user_id = ?", (uinfo[0]+1, uinfo[1]-1, message.created_at, authorid))
    
    if cityfound:
        cur.execute("INSERT INTO chain_info(server_id, user_id, round_number, count, city_id, name, admin2, admin1, country, country_code, alt_country, time_placed, valid, message_id, leaderboard_eligible) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (guildid, authorid, cache[guildid]["round_number"], len(citieslist)+1, res[0], n[0], n[3], n[2], n[1][0], n[1][1], n[4], message.created_at, False, message.id, False))
    else:
        cur.execute("INSERT INTO chain_info(server_id, user_id, round_number, count, name, time_placed, valid, message_id, leaderboard_eligible) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (guildid, authorid, cache[guildid]["round_number"], len(citieslist)+1, message.content[len(cache[guildid]["prefix"]):], message.created_at, False, message.id, False))
    cur.execute("UPDATE server_info SET chain_end = ?, current_letter = ?, last_user = ?, leaderboard_eligible = ?, last_message = ? WHERE server_id = ?",(True, '-', None, True, 0, guildid))
    cache[guildid]["chain_end"] = True
    cache[guildid]["current_letter"] = "-"
    cache[guildid]["last_user"] = None
    cache[guildid]["leaderboard_eligible"] = True
    cache[guildid]["last_message"] = 0

    # choose city
    if cache[guildid]["choose_city"]:
        # satisfies min population
        poss=city_default.filter((pl.col('population') >= cache[guildid]["min_pop"]) & (~pl.col('deleted').cast(pl.Boolean)))
        # not blacklisted/are whitelisted
        if cache[guildid]["list_mode"]:
            countrieslist={i for i in cache[guildid]["list"].split(',') if i}
            if len(countrieslist):
                if cache[guildid]["list_mode"]==1:
                    poss=poss.filter(~(pl.col('country').is_in(countrieslist)|pl.col('alt-country').is_in(countrieslist)))
                else:
                    poss=poss.filter(pl.col('country').is_in(countrieslist)|pl.col('alt-country').is_in(countrieslist))
        entr=poss.sample(1).row(0, named=True)

        nname=entr['name']
        newid=entr['geonameid']
        n=(nname,iso2[entr['country']],entr['country'],admin1name(entr['country'],entr['admin1']),admin2name(entr['country'],entr['admin1'],entr['admin2']),entr['alt-country'])

        try:
            await message.add_reaction('\N{CROSS MARK}')
        except:
            pass
        new_message = await message.channel.send('<@%s> RUINED IT AT **%s**!! Start again from `%s` (next letter `%s`). %s'%(authorid,f"{len(citieslist):,}",entr['name'],entr['last-letter'],reason), reference = msgref)
        cur.execute("UPDATE server_info SET current_letter = ?, last_message = ? WHERE server_id = ?", (entr['last-letter'], new_message.id, guildid))
        cache[guildid]["current_letter"] = entr['last-letter']
        cache[guildid]["last_message"] = new_message.id

        cur.execute("INSERT INTO chain_info(server_id, city_id, round_number, count, name, admin2, admin1, country, country_code, alt_country, time_placed, valid, leaderboard_eligible, message_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (guildid, int(newid), cache[guildid]["round_number"]+1, 1, n[0], n[4], n[3], n[1], n[2], n[5], message.created_at, True, True, new_message.id))
        conn.commit()
    else:
        conn.commit()
        try:
            await message.add_reaction('\N{CROSS MARK}')
        except:
            pass
        await message.channel.send('<@%s> RUINED IT AT **%s**!! %s'%(authorid,f"{len(citieslist):,}",reason), reference = msgref)
    # remove this from the queue of messages to process
    processes[guildid].pop(0)
    # if queue of other cities to process empty, set to none again. otherwise, process next city
    if len(processes[guildid])>0:
        await asyncio.create_task(process_chain(*processes[guildid][0]))

stats = app_commands.Group(name='stats',description="description", guild_only=True)
@app_commands.rename(se='show-everyone')
@stats.command(description="Displays server statistics.")
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def server(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    embed=discord.Embed(title="Server Stats", color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    cur.execute("SELECT * FROM chain_info WHERE server_id = ? AND round_number = ?", (guildid, cache[guildid]["round_number"]))
    embed.description='Round: **%s**\nCurrent letter: **%s**\nCurrent length: **%s**\nLast user: **%s**\nLongest chain: **%s** %s\nMinimum population: **%s**\nChoose city: **%s**\nRepeats: **%s**\nPrefix: %s\nList mode: **%s**\nUpdates:  **%s**\nNice: **%s**'%(f'{cache[guildid]["round_number"]:,}',cache[guildid]["current_letter"],f'{cur.rowcount:,}','<@'+str(cache[guildid]["last_user"])+'>' if cache[guildid]["last_user"] else '-',f'{cache[guildid]["max_chain"]:,}','<t:'+str(int(cache[guildid]["last_best"].timestamp()))+':R>' if cache[guildid]["last_best"] else '',f'{cache[guildid]["min_pop"]:,}','enabled' if cache[guildid]["choose_city"] else 'disabled', 'only after %s cities'%f'{cache[guildid]["min_repeat"]:,}' if cache[guildid]["repeats"] else 'disallowed','**'+cache[guildid]["prefix"]+'**' if cache[guildid]["prefix"]!='' else None,['disabled','blacklist','whitelist'][cache[guildid]["list_mode"]],'enabled' if cache[guildid]["updates"] else 'disabled','enabled' if cache[guildid]["nice"] else 'disabled')
    await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(description="Displays user statistics.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(member="The user to get statistics for.",se='Yes to show everyone stats, no otherwise')
async def user(interaction: discord.Interaction, member:Optional[discord.User|discord.Member]=None,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if not member:
        member=interaction.user
    cur.execute("SELECT correct, incorrect, score, last_active, blocked FROM server_user_info WHERE user_id = ? and server_id = ?", (member.id, interaction.guild_id))
    server_uinfo=cur.fetchone()
    if not (server_uinfo or isinstance(member,discord.Member)):
        await interaction.followup.send(embed=discord.Embed(color=RED,description="You do not have permission to acces this user's stats. "),ephemeral=(se=='no'))
        return
    cur.execute("SELECT correct, incorrect, score, last_active, blocked FROM global_user_info WHERE user_id = ?",(member.id,))
    if cur.rowcount==0:
        if member.id==interaction.user.id:
            await interaction.followup.send(embed=discord.Embed(color=RED,description='You must join the chain to use that command. '),ephemeral=(se=='no'))
        else:
            await interaction.followup.send(embed=discord.Embed(color=RED,description=f'<@{member.id}> has no Cities Chain stats. '),ephemeral=(se=='no'))
    else:
        embedslist=[]
        global_uinfo=cur.fetchone()
        embed=discord.Embed(title="User Stats", color=GREEN)
        if member.avatar:
            embed.set_author(name=member.name, icon_url=member.avatar.url)
        else:
            embed.set_author(name=member.name)

        embedslist.append(embed)
        if global_uinfo and global_uinfo[0]+global_uinfo[1]:
            embed.add_field(name=f'Global Stats {":no_pedestrians:" if global_uinfo[4] else ""}',value=f"Correct: **{f'{global_uinfo[0]:,}'}**\nIncorrect: **{f'{global_uinfo[1]:,}'}**\nCorrect Rate: **{round(global_uinfo[0]/(global_uinfo[0]+global_uinfo[1])*10000)/100 if global_uinfo[0]+global_uinfo[1]>0 else 0.0}%**\nScore: **{f'{global_uinfo[2]:,}'}**\nLast Active: <t:{int(pytz.utc.localize(global_uinfo[3]).timestamp())}:R>",inline=True)
        if server_uinfo and server_uinfo[0]+server_uinfo[1]:
            cur.execute("SELECT COUNT(*) as first_city_count FROM (SELECT user_id, city_id FROM chain_info WHERE server_id = ? AND user_id IS NOT NULL AND valid = ? GROUP BY city_id ORDER BY time_placed ASC) AS x GROUP BY user_id HAVING user_id = ?",(True, interaction.guild_id, member.id))
            first_cities=0
            if cur.rowcount:
                first_cities=cur.fetchone()[0]
            embed.add_field(name=f'Stats for ```%s``` {":no_pedestrians:" if server_uinfo[4] else ""}'%interaction.guild.name,value=f"Correct: **{f'{server_uinfo[0]:,}'}**\nIncorrect: **{f'{server_uinfo[1]:,}'}**\nCorrect Rate: **{round(server_uinfo[0]/(server_uinfo[0]+server_uinfo[1])*10000)/100 if server_uinfo[0]+server_uinfo[1]>0 else 0.0}%**\nScore: **{f'{server_uinfo[2]:,}'}**\nLast Active: <t:{int(pytz.utc.localize(server_uinfo[3]).timestamp())}:R>\nCities Placed First: **{f'{first_cities:,}'}**",inline=True)
        
            
            favcities = discord.Embed(title=f"Favorite Cities/Countries", color=GREEN)
            favc = []
            # (THANKS MARENS FOR SQL CODE)
            cur.execute("SELECT city_id, COUNT(*) AS use_count FROM chain_info WHERE server_id = ? AND user_id = ? AND valid = ? GROUP BY city_id ORDER BY use_count DESC", (interaction.guild_id, member.id, True))
            for i in cur.fetchall():
                if len(favc)==10:
                    break
                if i[0] in city_default['geonameid']:
                    cityrow = city_default.row(by_predicate=pl.col('geonameid') == i[0], named=True)
                    favc.append(city_string(cityrow['name'],admin1name(cityrow['country'],cityrow['admin1']),admin2name(cityrow['country'],cityrow['admin1'],cityrow['admin2']),cityrow['country'],cityrow['alt-country'])+f' - **{i[1]:,}**')
            favcities.add_field(name='Cities',value='\n'.join([f"{n+1}. "+i for n,i in enumerate(favc)]))
            
            cur.execute("SELECT country_code, COUNT(*) AS use_count FROM chain_info WHERE server_id = ? AND user_id = ? AND valid = ? GROUP BY country_code ORDER BY use_count DESC", (interaction.guild_id, member.id, True))
            countryuses = {i[0]:i[1] for i in cur.fetchall()}
            cur.execute("SELECT alt_country, COUNT(*) AS use_count FROM chain_info WHERE server_id = ? AND user_id = ? AND valid = ? AND alt_country IS NOT NULL GROUP BY alt_country ORDER BY use_count DESC", (interaction.guild_id, member.id, True))
            for i in cur.fetchall():
                if i[0] in countryuses:
                    countryuses[i[0]]+=i[1]
                else:
                    countryuses[i[0]]=i[1]
            fav_countries = [f"{j[1]} {j[2]} - **{j[0]:,}**" for j in sorted([(countryuses[i],iso2[i],flags[i]) for i in countryuses],key = lambda x:(-x[0],x[1],x[2]))[:10]]
            favcities.add_field(name='Countries',value='\n'.join([f"{n+1}. "+i for n,i in enumerate(fav_countries)]))
           
            if member.avatar:
                favcities.set_author(name=member.name, icon_url=member.avatar.url)
            else:
                favcities.set_author(name=member.name)
            embedslist.append(favcities)
        await interaction.followup.send(embeds=embedslist,ephemeral=(se=='no'))

@stats.command(description="Displays list of cities.")
@app_commands.rename(se='show-everyone',showmap='map')
@app_commands.describe(order='The order in which the cities are presented, sequential or alphabetical',cities='Whether to show all cities or only the ones that cannot be repeated',showmap='Whether to show a map of cities',se='Yes to show everyone stats, no otherwise')
async def cities(interaction: discord.Interaction,order:Literal['sequential','alphabetical'],cities:Literal['all','non-repeatable'],showmap:Optional[Literal['yes','no']]='no',se:Optional[Literal['yes','no']]='no'):
    # if is_blocked(interaction.user.id,interaction.guild_id):
    #     await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
    #     return
    cit=cities.capitalize()+" Cities"
    title=f"{cit} - {order.capitalize()}"
    await interaction.response.defer(ephemeral=(se=='no'))
    guildid=interaction.guild_id

    if not cache[guildid]["chain_end"]:
        cur.execute("SELECT city_id FROM repeat_info WHERE server_id = ?", (interaction.guild_id,))
        repeated={i[0] for i in cur.fetchall()}
        if cache[guildid]["repeats"] and cit.startswith("N"):
            cur.execute("SELECT name, admin1, admin2, country_code, alt_country, city_id, valid FROM chain_info WHERE server_id = ? AND round_number = ? ORDER BY count DESC",(guildid,cache[guildid]["round_number"]))
        else:
            cur.execute("SELECT name, admin1, admin2, country_code, alt_country, city_id, valid FROM chain_info WHERE server_id = ? AND round_number = ? ORDER BY count DESC LIMIT ?",(guildid,cache[guildid]["round_number"], cache[guildid]["min_repeat"]))
        cutoff=[]
        
        cityids = []
        countries = set()
        
        for i in cur.fetchall():
            r = city_default.row(by_predicate=pl.col('geonameid') == i[5], named=True)
            if i[6]:
                cityids.append(i[5])
            if i[4]:
                countries.add(i[4])
            countries.add(i[3])
            if i[5]!=-1:
                dname = r['name']
            cutoff.append(i[0] if i[5]==-1 else (city_string(i[0] + (f" ({dname})" if i[0]!=dname else ""),i[1],i[2],i[3],i[4])+(f"{':no_entry:' if (i[5]!=-1 and r['deleted']) else ''}{':repeat:' if i[5] in repeated else ''}"),i[6]))
        
        embed=discord.Embed(title=title, color=GREEN)
        if order.startswith('s'):
            fmt=[':x: %s'%i[0] for i in cutoff if not i[1]]+['%s. %s'%(n+1,i[0]) for n,i in enumerate([j for j in cutoff if j[1]])]
        else:
            fmt=['- '+i[0] for i in sorted(cutoff,key=lambda x:x[0].lower()) if i[1]]
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,title,math.ceil(len(fmt)/25),interaction.user.id,embed, f" | {len(set(cityids))} unique cities across {len(set(countries))} countries")
        
        await interaction.followup.send(embed=embed,view=view,ephemeral=(se=='no'),files=[discord.File(io.BytesIO(generate_map(tuple(cityids))), filename='map.png')] if showmap=='yes' else [])
        view.message=await interaction.original_response()
    else:
        embed=discord.Embed(title=title, color=GREEN,description='```There are no statistics.```')
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(name='round',description="Displays all cities said for one round.")
@app_commands.rename(se='show-everyone',showmap='map')
@app_commands.describe(round_num='Round to retrieve information from (0 = current round, negative numbers for older rounds)',showmap='Whether to show a map of cities',se='Yes to show everyone stats, no otherwise')
async def roundinfo(interaction: discord.Interaction,round_num:int,showmap:Optional[Literal['yes','no']]='no',se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    guildid=interaction.guild_id
    if round_num<=0:
        round_num=cache[guildid]["round_number"]+round_num
    cur.execute("SELECT name, admin1, admin2, country_code, alt_country, city_id, valid, count, user_id, time_placed FROM chain_info WHERE server_id = ? AND round_number = ? ORDER BY count ASC", (guildid,round_num))
    if 1<=round_num<=cache[guildid]["round_number"]:
        cutoff=[]

        cityids=[]
        countries = set()
        all_entries = list(cur.fetchall())
        participants = set()
        start = all_entries[0][9]
        end = all_entries[-1][9]
        if all_entries[-1][6]:
            end=None
        for i in all_entries:
            if i[6]:
                cityids.append(i[5])
                if i[8]:
                    participants.add(i[8])
                if i[4]:
                    countries.add(i[4])
                countries.add(i[3])
            if i[5]>=0:
                dname = city_default.row(by_predicate=pl.col('geonameid') == i[5])[city_default.get_column_index('name')]
            cutoff.append(('%s. '%i[7]if i[6] else ':x: ') + (i[0] if i[5]==-1 else (city_string(i[0] + (f" ({dname})" if i[0]!=dname else ""),i[1],i[2],i[3],i[4]))))
        embed=discord.Embed(title="Round %s (%s - %s, %s Participants)"%(f'{round_num:,}', f'<t:{int(pytz.utc.localize(start).timestamp())}:f>', f'<t:{int(pytz.utc.localize(end).timestamp())}:f>' if end else "Ongoing", len(participants)), color=GREEN,description='\n'.join(cutoff[:25]))
        if interaction.guild.icon:
            embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
        else:
            embed.set_author(name=interaction.guild.name)
        view=Paginator(1,cutoff,"Round %s (%s - %s, %s Participants)"%(f'{round_num:,}', f'<t:{int(pytz.utc.localize(start).timestamp())}:f>', f'<t:{int(pytz.utc.localize(end).timestamp())}:f>' if end else "Ongoing", len(participants)),math.ceil(len(cutoff)/25),interaction.user.id,embed,f" | {len(set(cityids))} unique cities across {len(set(countries))} countries")
        await interaction.followup.send(embed=embed,view=view,ephemeral=(se=='no'),files=[discord.File(io.BytesIO(generate_map(tuple(cityids))), filename='map.png')] if showmap=='yes' else [])
        view.message=await interaction.original_response()
    else:
        if cache[guildid]["round_number"]:
            await interaction.followup.send("Round_num must be a number between **-%s** and **%s**."%(cache[guildid]["round_number"]-1,cache[guildid]["round_number"]),ephemeral=(se=='no'))
        else:
            await interaction.followup.send("No rounds played yet.",ephemeral=(se=='no'))

def max_age_to_timestamp(interaction:discord.Interaction, max_age, is_global):
    # get all time
    if interaction.guild_id not in max_ages:
        cur.execute("SELECT MIN(time_placed) FROM chain_info WHERE server_id = ?", (interaction.guild_id,))
        since_time = cur.fetchone()[0]
        if since_time:
            max_ages[interaction.guild_id]=since_time
            alltime = max_ages[interaction.guild_id]
        else: 
            alltime = datetime.datetime.fromtimestamp(0)
    else:
        alltime = max_ages[interaction.guild_id]

    if max_age == "All Time":
        if is_global:
            return pytz.utc.localize(earliest_time)
        else:
            return pytz.utc.localize(alltime)  
    else:
        return max(pytz.utc.localize(alltime), (interaction.created_at - stats_time_offset[max_age]))

@stats.command(description="Displays serverwide user leaderboard.")
@app_commands.rename(se='show-everyone', max_age = 'max-age')
@app_commands.describe(se='Yes to show everyone stats, no otherwise', max_age = 'Range of statistics to show - default is 1 Month (30 days)')
async def slb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no', max_age:Optional[Literal["24 Hours", "48 Hours", "7 Days", "14 Days", "1 Month", "3 Months", "All Time"]]="1 Month"):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return

    since = max_age_to_timestamp(interaction, max_age, 0)
    embed=discord.Embed(title=f"```{interaction.guild.name}``` User Leaderboard - Since <t:{int(since.timestamp())}:d>",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    
    if since == max_ages[interaction.guild_id]:
        cur.execute("SELECT user_id, score FROM server_user_info WHERE server_id = ? ORDER BY score DESC",(interaction.guild_id,))
    else:
        cur.execute("SELECT user_id, SUM(CASE WHEN valid = ? THEN ? ELSE ? END) AS score FROM chain_info WHERE server_id = ? AND time_placed >= ? AND user_id IS NOT NULL GROUP BY user_id ORDER BY score DESC", (True, 1, -1, interaction.guild_id, since))

    if cur.rowcount>0:
        fmt=[f'{n+1}. <@{i[0]}>{":no_pedestrians:" if is_blocked(i[0],interaction.guild_id) else ""} - **{f"{i[1]:,}"}**' for n,i in enumerate(cur.fetchall())]
        embed.description='\n'.join(fmt[:25])
        await interaction.followup.send(embed=embed,view=Paginator(1,fmt,embed.title,math.ceil(len(fmt)/25),interaction.user.id,embed),ephemeral=(se=='no'))    
    else:
        embed.description='```There are no statistics.```'
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))    

@stats.command(description="Displays global leaderboard of maximum scores for servers.")
@app_commands.rename(se='show-everyone', max_age = 'max-age')
@app_commands.describe(se='Yes to show everyone stats, no otherwise', max_age = 'Range of statistics to show - default is 1 Month (30 days)')
async def lb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no', max_age:Optional[Literal["24 Hours", "48 Hours", "7 Days", "14 Days", "1 Month", "3 Months", "All Time"]]="1 Month"):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return

    since = max_age_to_timestamp(interaction, max_age, 1)
    embed=discord.Embed(title=f"Server High Scores - Since <t:{int(since.timestamp())}:d>",color=GREEN)
    # create round_info table to do this
    cur.execute("SELECT server_id, mc FROM (SELECT server_id, round_number,MAX(count) AS mc, MIN(count) AS began, MAX(time_placed) AS time_finished FROM chain_info WHERE valid = ? AND leaderboard_eligible = ? AND time_placed >= ? GROUP BY server_id, round_number ORDER BY mc DESC) x WHERE began = ? GROUP BY server_id ORDER BY mc DESC, time_finished ASC, server_id DESC;", (True, True, since, 1))
    if cur.rowcount>0:
        top=[]
        counter=0
        for i in cur.fetchall():
            server_from_id = client.get_guild(i[0])
            if server_from_id:
                counter+=1
                top.append(f'{counter}. {server_from_id.name} - **{f"{i[1]:,}"}**')
        if len(top):
            embed.description='\n'.join(top[:25])
            await interaction.followup.send(embed=embed,view=Paginator(1,top,embed.title,math.ceil(len(top)/25),interaction.user.id,embed," | The stats on this leaderboard indicate the maximum number of cities before a repeat."),ephemeral=(se=='no'))
        else:
            embed.description='```There are no statistics.```'
            embed.set_footer(text="The stats on this leaderboard indicate the maximum number of cities before a repeat.")
            await interaction.followup.send(embed=embed,ephemeral=(se=='no'))
    else:
        embed.description='```There are no statistics.```'
        embed.set_footer(text="The stats on this leaderboard indicate the maximum number of cities before a repeat.")
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(description="Displays global user leaderboard.")
@app_commands.rename(se='show-everyone', max_age = 'max-age')
@app_commands.describe(se='Yes to show everyone stats, no otherwise', max_age = 'Range of statistics to show - default is 1 Month (30 days)')
async def ulb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no', max_age:Optional[Literal["24 Hours", "48 Hours", "7 Days", "14 Days", "1 Month", "3 Months", "All Time"]]="1 Month"):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    
    since = max_age_to_timestamp(interaction, max_age, 1)
    embed=discord.Embed(title=f"Global User Leaderboard - Since <t:{int(since.timestamp())}:d>",color=GREEN)

    since_is_max = (since == earliest_time)
    if since_is_max:
        cur.execute("SELECT user_id, score, blocked FROM global_user_info ORDER BY score DESC",(interaction.guild_id,))
    else:
        cur.execute("SELECT user_id, SUM(CASE WHEN valid = ? THEN ? ELSE ? END) AS score FROM chain_info WHERE time_placed >= ? AND user_id IS NOT NULL GROUP BY user_id ORDER BY score DESC", (True, 1, -1, since,))
        cur.execute("SELECT user_id FROM global_user_info WHERE blocked = ?", (True, ))
        global_blocks = {i for (i,) in cur.fetchall()}

    if cur.rowcount>0:
        if since_is_max:
            fmt = [f'{n+1}. <@{i[0]}>{":no_pedestrians:" if i[2] else ""} - **{f"{i[1]:,}"}**' for n,i in enumerate(cur.fetchall())]
        else:
            fmt = [f'{n+1}. <@{i[0]}>{":no_pedestrians:" if i[0] in global_blocks else ""} - **{f"{i[1]:,}"}**' for n,i in enumerate(cur.fetchall())]
        
        embed.description='\n'.join(fmt[:25])
        await interaction.followup.send(embed=embed,view=Paginator(1,fmt,embed.title,math.ceil(len(fmt)/25),interaction.user.id,embed),ephemeral=(se=='no'))
    else:
        embed.description='```There are no statistics.```'
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(name="first-cities",description="Displays users who have been first to place the most cities.")
@app_commands.rename(se='show-everyone', max_age = 'max-age')
@app_commands.describe(se='Yes to show everyone stats, no otherwise', max_age = 'Range of statistics to show - default is 1 Month (30 days)')
async def firstcity(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no', max_age:Optional[Literal["24 Hours", "48 Hours", "7 Days", "14 Days", "1 Month", "3 Months", "All Time"]]="1 Month"):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    since = max_age_to_timestamp(interaction, max_age, 0)
    embed=discord.Embed(title=f"```{interaction.guild.name}``` First-City Placers Leaderboard - Since <t:{int(since.timestamp())}:d>",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    cur.execute("SELECT user_id, COUNT(*) as first_city_count FROM (SELECT user_id, city_id, MIN(time_placed) as first_time FROM chain_info WHERE server_id = ? AND user_id IS NOT NULL AND valid = ? GROUP BY city_id) x WHERE first_time >= ? GROUP BY user_id ORDER BY first_city_count DESC",(interaction.guild_id, True, since))
    if cur.rowcount>0:
        fmt=[f'{n+1}. <@{i[0]}>{":no_pedestrians:" if is_blocked(i[0],interaction.guild_id) else ""} - **{f"{i[1]:,}"}**' for n,i in enumerate(cur.fetchall())]
        embed.description='\n'.join(fmt[:25])
        await interaction.followup.send(embed=embed,view=Paginator(1,fmt,embed.title,math.ceil(len(fmt)/25),interaction.user.id,embed),ephemeral=(se=='no'))    
    else:
        embed.description='```There are no statistics.```'
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))    

@stats.command(description="Displays all cities and their reactions.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def react(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    embed=discord.Embed(title='Cities With Reactions',color=GREEN)
    cur.execute("SELECT city_id, reaction FROM react_info WHERE server_id = ?", (interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[]
        for (i,r) in cur.fetchall():
            j=city_default.row(by_predicate=pl.col('geonameid') == i, named=True)
            fmt.append(f"- {city_string(j['name'],admin1name(j['country'],j['admin1']),admin2name(j['country'],j['admin1'],j['admin2']),j['country'],j['alt-country'])} - {r}")
        fmt=sorted(fmt)
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Cities With Reactions",math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=(se=='no'))
        view.message=await interaction.original_response()
    else:
        embed.description="```There are no reactions associated with any cities.```"
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(description="Displays all cities that can be repeated.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def repeat(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    embed=discord.Embed(title='Repeats Rule Exceptions',color=GREEN)
    cur.execute("SELECT city_id FROM repeat_info WHERE server_id = ?", (interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[]
        for (i,) in cur.fetchall():
            j=city_default.row(by_predicate=pl.col('geonameid') == i, named=True)
            fmt.append(f"- {city_string(j['name'],admin1name(j['country'],j['admin1']),admin2name(j['country'],j['admin1'],j['admin2']),j['country'],j['alt-country'])}")
        fmt=sorted(fmt)
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Repeats Rule Exceptions",math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=(se=='no'))
        view.message=await interaction.original_response()
    else:
        embed.description="```There are no repeatable cities.```"
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(name='popular-cities',description="Displays most popular cities and countries added to chain.")
@app_commands.rename(se='show-everyone', max_age = 'max-age')
@app_commands.describe(se='Yes to show everyone stats, no otherwise', max_age = 'Range of statistics to show - default is 1 Month (30 days)')
async def popular(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no', max_age:Optional[Literal["24 Hours", "48 Hours", "7 Days", "14 Days", "1 Month", "3 Months", "All Time"]]="1 Month"):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    since = max_age_to_timestamp(interaction, max_age, 0)
    # check if all time equals since
    if since == max_ages[interaction.guild_id]:
        cur.execute("SELECT city_id, count FROM count_info WHERE server_id = ? ORDER BY count DESC, city_id ASC", (interaction.guild_id,))
    else:
        cur.execute("SELECT city_id, COUNT(*) as city_counts FROM chain_info WHERE time_placed >= ? AND valid = ? AND server_id = ? AND user_id IS NOT NULL GROUP BY city_id ORDER BY city_counts DESC, city_id ASC",(since, True, interaction.guild_id))
    cities=[i for i in cur.fetchall()]
    cur.execute("SELECT city_id FROM repeat_info WHERE server_id = ?", (interaction.guild_id,))
    repeated={i[0] for i in cur.fetchall()}
    embed=discord.Embed(title=f"Popular Cities/Countries - Since <t:{int(since.timestamp())}:d>",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    if len(cities)>0:
        fmt=[]
        countries={ccode:0 for ccode in iso2}
        df = pl.DataFrame(cities, schema={'geonameid':pl.Int32, 'count':pl.UInt32}, orient='row')
        counts = city_default.join(df, ['geonameid'], 'inner').sort(['count','geonameid'],descending=[True,False])

        for n,j in enumerate(counts.iter_rows(named=True)):
            countries[j['country']]+=j['count']
            if j['alt-country']:
                countries[j['alt-country']]+=j['count']
            if n < 10:
                fmt.append((j['count'],city_string(j['name'],admin1name(j['country'],j['admin1']),admin2name(j['country'],j['admin1'],j['admin2']),j['country'],j['alt-country'])+(f'{":no_entry:" if j["deleted"] else ""}{":repeat:" if j["geonameid"] in repeated else ""}')))
        fmt=sorted(fmt,key = lambda x:(-x[0],x[1]))
        embed.add_field(name='Cities',value='\n'.join(['%s. %s - **%s**' %(n+1,i[1],f"{i[0]:,}") for (n,i) in enumerate(fmt)]))
        fmt=[]
        for i in countries:
            if countries[i]:
                fmt.append((int(countries[i]),iso2[i],flags[i]))
        fmt=sorted(fmt,key = lambda x:(-x[0],x[1]))[:10]
        embed.add_field(name='Countries',value='\n'.join(['%s. %s %s - **%s**' %(n+1,i[1],i[2],f"{i[0]:,}") for (n,i) in enumerate(fmt)]))
    else:
        embed.add_field(name='Cities',value='```There are no statistics.```')
        embed.add_field(name='Countries',value='```There are no statistics.```')
    await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(name='best-rounds',description="Displays longest chains in server.")
@app_commands.rename(se='show-everyone', max_age = 'max-age')
@app_commands.describe(se='Yes to show everyone stats, no otherwise', max_age = 'Range of statistics to show - default is 1 Month (30 days)')
async def bestrds(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no', max_age:Optional[Literal["24 Hours", "48 Hours", "7 Days", "14 Days", "1 Month", "3 Months", "All Time"]]="1 Month"):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return

    since = max_age_to_timestamp(interaction, max_age, 0)
    # create round_info table to do this
    cur.execute("SELECT * FROM (SELECT server_id, round_number, MIN(count) as started_after, MAX(CASE valid WHEN ? THEN count ELSE ? END) as round_length, MIN(time_placed) as start_time, MAX(time_placed) as end_time, COUNT(DISTINCT(CASE WHEN valid = ? AND user_id IS NOT NULL THEN user_id END)) as participants FROM chain_info WHERE server_id = ? AND time_placed >= ? GROUP BY round_number ORDER BY round_length DESC, round_number ASC LIMIT ?) X WHERE started_after = ?  ORDER BY round_length DESC, round_number ASC LIMIT ?", (True, 0, True, interaction.guild_id, since, 11, 1, 10))
    embed=discord.Embed(title=f"Best Rounds - Since <t:{int(since.timestamp())}:d>",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    # if len(rounds)>0:
    if cur.rowcount>0:
        # fmt=[]
        # maxrounds=[]
        # if not bb[1]:
        #     cur.execute("select count(*) from chain_info where server_id = ? and round_number = ?",(interaction.guild_id,bb[0]))
        #     maxrounds.append((cur.fetchone()[0],bb[0],"**Ongoing**"))
        # cur.execute("select distinct count,round_number,time_placed from chain_info where server_id = ? and valid = ?",(interaction.guild_id,False))
        # maxrounds.extend([(i-1,j,k) for (i,j,k) in cur.fetchall()])
        # maxrounds=sorted(maxrounds,reverse=1)[:10]
        # for i in maxrounds:
        for i in cur.fetchall():
            _, round_num, _, length, start_time, end_time, part = i
            start_time = pytz.utc.localize(start_time)
            end_time = pytz.utc.localize(end_time)
            if length:
                # get first city
                cur.execute("SELECT city_id, name FROM chain_info WHERE server_id = ? AND round_number = ? AND (count = ? OR count = ?) AND valid = ? ORDER BY count ASC", (interaction.guild_id, round_num, 1, length, True))
                first_id, first_name = cur.fetchone()
                f_c = city_default.row(by_predicate=pl.col('geonameid') == first_id, named=True)
                name_str = city_string(first_name if first_name == f_c['name'] else f"{first_name} ({f_c['name']})", 
                                        admin1_default.row(by_predicate=(pl.col('country') == f_c['country']) & (pl.col('admin1') == f_c['admin1']))[admin1_default.get_column_index('name')] if f_c['admin1'] else None,
                                        admin2_default.row(by_predicate=(pl.col('country') == f_c['country']) & (pl.col('admin1') == f_c['admin1']) & (pl.col('admin2') == f_c['admin2']))[admin2_default.get_column_index('name')] if f_c['admin2'] else None,
                                        f_c['country'], f_c['alt-country'])
                if length-1:
                    # get last city
                    # cur.execute("SELECT city_id,name FROM chain_info WHERE server_id = ? AND round_number = ? AND count = ? AND valid = 1", (interaction.guild_id, round_num, length))
                    last_id, last_name = cur.fetchone()
                    l_c = city_default.row(by_predicate=pl.col('geonameid') == last_id, named=True)
                    name_str += ' - ' +  city_string(last_name if last_name == l_c['name'] else f"{last_name} ({l_c['name']})", 
                                        admin1_default.row(by_predicate=(pl.col('country') == l_c['country']) & (pl.col('admin1') == l_c['admin1']))[admin1_default.get_column_index('name')] if l_c['admin1'] else None,
                                        admin2_default.row(by_predicate=(pl.col('country') == l_c['country']) & (pl.col('admin1') == l_c['admin1']) & (pl.col('admin2') == l_c['admin2']))[admin2_default.get_column_index('name')] if l_c['admin2'] else None,
                                        l_c['country'], l_c['alt-country'])
            else:
                name_str = '-'
            
            embed.add_field(name=name_str, value=f'Length: {length:,}\nRound: {round_num:,}\nParticipants: {part:,}\nStarted: <t:{int(start_time.timestamp())}:f>\nEnded: {"**Ongoing**" if (round_num==cache[interaction.guild_id]["round_number"] and not cache[interaction.guild_id]["chain_end"]) else f"<t:{int(end_time.timestamp())}:f>"}')
            
            # if maxc>1:
            #     cur.execute("select city_id,name,admin2,admin1,country_code,alt_country,time_placed from chain_info where server_id = ? and round_number = ? and count = ?",(interaction.guild_id,i[1],1))
            #     b1=cur.fetchone()
            #     cur.execute("select city_id,name,admin2,admin1,country_code,alt_country from chain_info where server_id = ? and round_number = ? and count = ?",(interaction.guild_id,i[1],maxc))
            #     b2=cur.fetchone()
            #     b=[]
            #     for j in (b1,b2):
            #         o=city_default.loc[(j[0])]
            #         b.append(city_string(j[1]+(f" ({o['name']})" if o['name']!=j[1] else ""),j[3],j[2],j[4],j[5]))
            #     fmt.append((maxc,i[1],part,tuple(b),("<t:%s:f>"%b1[6],i[2] if type(i[2])==str else "<t:%s:f>"%i[2])))
            # elif maxc==1:
            #     cur.execute("select city_id,name,admin2,admin1,country_code,alt_country,time_placed from chain_info where server_id = ? and round_number = ? and count = ?",(interaction.guild_id,i[1],1))
            #     j=cur.fetchone()
            #     o=city_default.loc[(j[0])]
            #     fmt.append((maxc,i[1],part,(city_string(j[1]+(f" ({o['name']})" if o['name']!=j[1] else ""),j[3],j[2],j[4],j[5]),),("<t:%s:f>"%j[6],i[2] if type(i[2])==str else "<t:%s:f>"%i[2])))
            # else:
            #     fmt.append((0,i[1],1,("None","None"),(i[2] if type(i[2])==str else "<t:%s:f>"%i[2],i[2] if type(i[2])==str else "<t:%s:f>"%i[2])))
        # for i in fmt:
        #     if i[0]>1:
        #         embed.add_field(name='%s - %s'%i[3],value='Length: %s\nRound: %s\nParticipants: %s\nStarted: %s\nEnded: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}',i[4][0],i[4][1]))
        #     elif i[0]==1:
        #         embed.add_field(name='%s'%i[3][0],value='Length: %s\nRound: %s\nParticipants: %s\nStarted: %s\nEnded: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}',i[4][0],i[4][1]))
        #     else:
        #         embed.add_field(name='None',value='Length: %s\nRound: %s\nParticipants: %s\nStarted: %s\nEnded: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}',i[4][0],i[4][1]))
    else:
        embed.add_field(name='',value='```There are no statistics.```')
    await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(name='blocked-users',description="Point and laugh.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def blocked(interaction:discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    cur.execute("SELECT user_id, block_reason, block_expiry FROM server_user_info WHERE blocked = ? AND server_id = ?", (True, interaction.guild_id))
    blocks={i[0]:(i[1],i[2]) for i in cur.fetchall()}
    # just filter by guild membership
    cur.execute("SELECT global_user_info.user_id, global_user_info.block_reason, global_user_info.block_expiry FROM global_user_info INNER JOIN (SELECT server_user_info.user_id FROM server_user_info WHERE server_id = ?) AS b ON global_user_info.user_id = b.user_id WHERE blocked = ? ",(interaction.guild_id, True))
    for i in cur.fetchall():
        blocks[i[0]]=(i[1],i[2])
    embed=discord.Embed(title='Blocked Users',color=GREEN)
    if len(blocks)>0:
        fmt=[f"- <@{i}> - {blocks[i][0]} - Expires {f'<t:{int(blocks[i][1].timestamp())}:R>' if blocks[i][1]!=NEVER_TIMESTAMP else '**Never**'}" for i in blocks]
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Blocked Users",math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=(se=='no'))
        view.message=await interaction.original_response()
    else:
        embed.description="```There are no blocked users.```"
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@stats.command(name='country-list',description="Shows blacklisted/whitelisted countries.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def countrylist(interaction:discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    countrylist,mode=cache[interaction.guild_id]["list"],cache[interaction.guild_id]["list_mode"]
    countrylist=[i for i in countrylist.split(',') if len(i)==2]
    embed=discord.Embed(title='%s Countries%s'%(['List of','Blacklisted','Whitelisted'][mode],' (blacklist/whitelist must be enabled to use)' if not mode else ''),color=GREEN if mode else RED)
    if len(countrylist)>0:
        fmt=[f"- {flags[i]} {iso2[i]} ({i})" for i in countrylist]
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,embed.title,math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=(se=='no'))
        view.message=await interaction.original_response()
    else:
        embed.description="```There are no countries in the list.```"
        await interaction.followup.send(embed=embed,ephemeral=(se=='no'))

@tree.command(name='city-info',description='Gets information for a given city.')
@app_commands.describe(query="The name of the city",include_deletes='Whether to search for cities that have been removed from the database or not',se='Yes to show everyone stats, no otherwise')
@app_commands.rename(include_deletes='include-deletes',se='show-everyone')
@app_commands.guild_only()
async def cityinfo(interaction: discord.Interaction, query:str,include_deletes:Optional[Literal['yes','no']]='no',se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    cur.execute("SELECT min_pop, list_mode, list FROM server_info WHERE server_id = ?", (interaction.guild_id,))
    minimum_population,country_list_mode,country_list = cache[interaction.guild_id]["min_pop"],cache[interaction.guild_id]["list_mode"],cache[interaction.guild_id]["list"]
    sanitized = sanitize_query(query)
    if len(sanitized):
        res=search_cities(sanitized[0],sanitized[1:],minimum_population,(include_deletes=='yes'),country_list_mode,country_list)
        # res=chain_pool.submit(search_cities,sanitized[0],sanitized[1:],minimum_population,(include_deletes=='yes'),country_list_mode,country_list)
        # res=res.result()
        if res:
            cur.execute("SELECT count FROM count_info WHERE server_id = ? and city_id = ?", (interaction.guild_id, res[0]))
            if cur.rowcount:
                count = cur.fetchone()[0]
                cur.execute("SELECT user_id, SUM(CASE user_id WHEN ? THEN ? ELSE ? END), MIN(time_placed) FROM chain_info WHERE server_id = ? AND city_id = ? AND valid = ? AND user_id IS NOT NULL ORDER BY time_placed DESC", (interaction.user.id, 1, 0, interaction.guild_id, res[0], True))
                first_user, user_count, _=cur.fetchone()
            else:
                count=0
            cur.execute("SELECT * FROM repeat_info WHERE server_id = ? AND city_id = ?", (interaction.guild_id,res[0]))
            repeatable=cur.rowcount
            aname=citydata.filter(pl.col('geonameid')==res[0])
            default=city_default.row(by_predicate=pl.col('geonameid') == res[0], named=True)
            dname=default['name']
            embed=discord.Embed(title='Information - %s'%dname,color = GREEN if not default['deleted'] else RED)
            embed.add_field(name='Geonames ID',value=res[0],inline=True)
            embed.add_field(name='Name',value=dname,inline=True)
            embed.add_field(name='Count',value="%s%s"%(f"{count:,} {':repeat:' if repeatable else ''}",'\n(%s uses by <@%s>)\nFirst used by <@%s>'%(f"{user_count:,}",interaction.user.id,first_user) if count else ''),inline=True)
            if default['deleted']:
                embed.set_footer(text='This city has been removed from Geonames.')
            alts=aname.filter(pl.col('default')==0)['name']
            tosend=[embed]
            if alts.shape[0]!=0:
                joinednames='`'+'`,`'.join(alts)+'`'
                if len(joinednames)<=1024:
                    embed.add_field(name='Alternate Names',value=joinednames,inline=False)
                else:
                    embed2=discord.Embed(title='Alternate Names - %s'%dname,
                                        color = GREEN if not default['deleted'] else RED)
                    tosend.append(embed2)
                    if len(joinednames)>2048:
                        commaindex=joinednames[:2048].rfind(',')+1
                        embed2.description=joinednames[:commaindex]
                        embed3=discord.Embed(color=GREEN)
                        embed3.description=joinednames[commaindex:]
                        tosend.append(embed3)
                    else:
                        embed2.description=joinednames
            else:
                embed.add_field(name='',value='',inline=False)
            if default['admin1']:
                if default['admin2']:
                    embed.add_field(name='Administrative Divisions',value=f"1. {admin1name(default['country'],default['admin1'])}\n2. {admin2name(default['country'],default['admin1'],default['admin2'])}",inline=True)
                else:
                    embed.add_field(name='Administrative Division',value=admin1name(default['country'],default['admin1']),inline=True)

            if default['alt-country']:
                embed.add_field(name='Countries',value=flags[default['country']]+' '+iso2[default['country']]+' ('+default['country']+')\n'+flags[default['alt-country']]+' '+iso2[default['alt-country']]+' ('+default['alt-country']+')',inline=True)
            else:
                embed.add_field(name='Country',value=flags[default['country']]+' '+iso2[default['country']]+' ('+default['country']+')',inline=True)
            embed.add_field(name='Population',value=f"{default['population']:,}",inline=True)
            
            # first, last letters
            f_l_letters=discord.Embed(title=f"For the spelling `{res[2]}`",color=GREEN if not default['deleted'] else RED)
            f_l_letters.add_field(name="Name",value=f"`{res[1]['name']}`")
            f_l_letters.add_field(name="As ASCII spelling",value=f"`{anyascii(res[2])}`")
            f_l_letters.add_field(name="First & Last Letters",value=f"`{res[1]['first-letter']}`,`{res[1]['last-letter']}`")
            tosend.append(f_l_letters)

            embed_sizes = [sum([len(j) for j in (i.title if i.title else '', 
                                                i.description if i.description else '')]) +
                            sum(sum([len(k) for k in (j.name if j.name else '',
                                                    j.value if j.value else '')]) for j in i.fields) for i in tosend]
                # while over limit send embeds individually
            while sum(embed_sizes)>6000:
                await interaction.followup.send(embed=tosend[0],ephemeral=(se=='no'))
                embed_sizes=embed_sizes[1:]      
                tosend=tosend[1:] 
            await interaction.followup.send(embeds=tosend,ephemeral=(se=='no'))    
        else:
            await interaction.followup.send('City not recognized. Please try again. ',ephemeral=(se=='no'))
    else:
        await interaction.followup.send('Please type in a name. ',ephemeral=(se=='no'))

@tree.command(name='subdivision-info',description='Gets information for a given administrative division.')
# subdivision field, specify if admin1 or admin2, admin1
@app_commands.describe(sd_name="The subdivision to get information is", admin1='1st-level administrative division the subdivision is located in, if the subdivision is 2nd-level', country="The country in which the subdivision is located in",se='Yes to show everyone stats, no otherwise')
@app_commands.rename(sd_name="subdivision",se='show-everyone')
@app_commands.autocomplete(country=countrycomplete)
@app_commands.guild_only()
async def subdivisioninfo(interaction: discord.Interaction, sd_name:str, admin1:Optional[str]='', country:Optional[str]='' ,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    
    admsearch=sd_name.lower().strip()
    if admin1:
        # case: admin2
        res=admin2data.filter(pl.col('name').str.to_lowercase()==admsearch)
        a1search = admin1.lower().strip()
        a1choice=admin1data.filter(pl.col('name').str.to_lowercase()==a1search)
        res = res.join(a1choice['country','admin1'], ['country','admin1'], 'inner')
    else:
        # case: admin1
        res=admin1data.filter(pl.col('name').str.to_lowercase()==admsearch)
    
    if country:
        countrysearch=country.lower().strip()
        cchoice = countriesdata.filter((pl.col('name').str.to_lowercase()==countrysearch)|(pl.col('country').str.to_lowercase()==countrysearch))
        res = res.join(cchoice, ['country'], 'inner')
    res = res.sort(['default','country','admin1','geonameid'],descending=[True,False,False,False])

    if res.shape[0]:
        res = res.row(0, named=True)
        # query was admin2
        if admin1:
            cities_in_admin = city_default.filter((pl.col('country')==res['country'])&(pl.col('admin1')==res['admin1'])&(pl.col('admin2')==res['admin2']))
        # query was admin1
        else:
            cities_in_admin = city_default.filter((pl.col('country')==res['country'])&(pl.col('admin1')==res['admin1']))

        # cur.execute("select city_id, count from count_info WHERE server_id = ? AND country_code = ?",(interaction.guild_id,res['country']))
        cur.execute("SELECT city_id, COUNT(*), SUM(CASE user_id WHEN ? THEN ? ELSE ? END) FROM chain_info WHERE server_id = ? AND country_code = ? AND valid = ? AND user_id IS NOT NULL GROUP BY city_id", (interaction.user.id, 1, 0, interaction.guild_id, res["country"], True))
        if cur.rowcount:
            cities_fetched = [i for i in cur.fetchall()]
            filtered_cities = pl.DataFrame(cities_fetched,schema={'geonameid':pl.Int32,'count':pl.Int16,'user_counts':pl.Int16}, orient='row').join(cities_in_admin, ['geonameid'], 'inner').sort(['count','geonameid'],descending=[True,False])
        else:
            filtered_cities = pl.DataFrame(schema={'geonameid':pl.Int32,'count':pl.Int16,'user_counts':pl.Int16}, orient='row')
        if admin1:
            aname=admin2data.filter(pl.col('geonameid')==res['geonameid'])
            default=admin2_default.row(by_predicate=(pl.col('country')==res['country'])&(pl.col('admin1')==res['admin1'])&(pl.col('admin2')==res['admin2']), named=True)
        else:
            aname=admin1data.filter(pl.col('geonameid')==res['geonameid'])
            default=admin1_default.row(by_predicate=(pl.col('country')==res['country'])&(pl.col('admin1')==res['admin1']), named=True)
        dname=default['name']

        a1d = admin1name(res['country'], res['admin1']) if admin1 else ''

        embed=discord.Embed(title='Information - %s, %s %s (%s) - Count: %s'%(f"{dname}, {a1d}" if admin1 else dname, flags[res['country']],iso2[res['country']],res['country'],f"{filtered_cities['count'].sum():,} ({filtered_cities['user_counts'].sum():,} uses by @{interaction.user.name})" if filtered_cities['count'].sum() else 0),color=GREEN)
        alts=aname.filter(pl.col('default')==0)['name']
        if alts.shape[0]:
            joinednames='`'+'`,`'.join(alts)+'`'
            if (len(joinednames)<4096):
                embed.description=joinednames
                tosend=[embed]
            else:
                commaindex=joinednames[:4096].rfind(',')+1
                embed.description=joinednames[:commaindex]
                embed2=discord.Embed(color=GREEN)
                embed2.description=joinednames[commaindex:]
                tosend=[embed,embed2]
            topcities=discord.Embed(title='Popular Cities -  %s, %s %s (%s)'%(f"{dname}, {admin1name(res['country'], res['admin1'])}" if admin1 else dname, flags[res['country']],iso2[res['country']],res['country']),color=GREEN)
            # cur.execute("select name,admin2,admin1,country_code,alt_country,count,city_id from count_info where server_id = ? and (country_code = ? or alt_country = ?) order by count desc limit 10",(interaction.guild_id,res['country'],res['country']))
            # if cur.rowcount>0:
            if filtered_cities.shape[0]:
                citylist=[]
                for n in range(min(10,filtered_cities.shape[0])):
                    i = filtered_cities.row(n, named=True)
                    citylist.append(f"{n+1}. {city_string(i['name'], a1d if admin1 else dname, dname if admin1 else admin2name(i['country'], i['admin1'], i['admin2']), i['country'], i['alt-country'])} - **{i['count']:,}**")
                    
            #     citylist=[]
            #     for n,i in enumerate(cur.fetchall()):
            #         deleted = city_default.loc[i[6],'deleted']
            #         citylist.append(f"{n+1}. {city_string(i[0],i[2],i[1],i[3],i[4])}{':no_entry:' if deleted else ''} - **{i[5]:,}**")
                topcities.description='\n'.join(citylist)
                tosend.append(topcities)
            embed_sizes = [sum([len(j) for j in (i.title if i.title else '',i.description)]) for i in tosend]
            # while over limit send embeds individually
            while sum(embed_sizes)>6000:
                await interaction.followup.send(embed=tosend[0],ephemeral=(se=='no'))
                embed_sizes=embed_sizes[1:]      
                tosend=tosend[1:] 
            await interaction.followup.send(embeds=tosend,ephemeral=(se=='no'))      
        else:
            embed.description='There are no alternate names for this subdivision.'
            await interaction.followup.send(embed=embed,ephemeral=(se=='no'))
    else:
        await interaction.followup.send('Subdivision not recognized. Please try again. ',ephemeral=(se=='no'))

@tree.command(name='country-info',description='Gets information for a given country.')
@app_commands.describe(country="The country to get information for",se='Yes to show everyone stats, no otherwise')
@app_commands.rename(se='show-everyone')
@app_commands.autocomplete(country=countrycomplete)
@app_commands.guild_only()
async def countryinfo(interaction: discord.Interaction, country:str,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    countrysearch=country.lower().strip()
    res=countriesdata.filter((pl.col('name').str.to_lowercase()==countrysearch)|(pl.col('country').str.to_lowercase()==countrysearch))
    if res.shape[0]!=0:
        res = res.row(0, named=True)

        # TEMPORARILY FOR ANTARCTICA - WORK ON FIX
        if res['country'] == "AQ":
            await interaction.followup.send("Antarctica has been temporarily disabled from the bot - please be patient as we are working on a fix",ephemeral=(se=='no'))      
            return

        # cur.execute("select sum(count) from count_info where server_id = ? and (country_code = ? or alt_country = ?)",(interaction.guild_id,res['country'],res['country']))
        cur.execute("SELECT COUNT(*), SUM(CASE user_id WHEN ? THEN ? ELSE ? END) FROM chain_info WHERE server_id = ? AND valid = ? AND user_id IS NOT NULL AND (country_code = ? OR alt_country = ?)", (interaction.user.id, 1, 0, interaction.guild_id, True, res['country'], res['country']))
        count, user_count=cur.fetchone()
        aname=countriesdata.filter(pl.col('geonameid')==res['geonameid'])
        default=countrydefaults.row(by_predicate=pl.col('country') == res['country'], named=True)
        dname=default['name']
        embed=discord.Embed(title='Information - %s %s (%s) - Count: %s'%(flags[res['country']],dname,res['country'],f"{count:,} ({user_count:,} uses by @{interaction.user.name})" if count else 0, ),color=GREEN)
        alts=aname.filter(pl.col('default')==0)['name']
        if alts.shape[0]:
            joinednames='`'+'`,`'.join(alts)+'`'
            if (len(joinednames)<4096):
                embed.description=joinednames
                tosend=[embed]
            else:
                commaindex=joinednames[:4096].rfind(',')+1
                embed.description=joinednames[:commaindex]
                embed2=discord.Embed(color=GREEN)
                embed2.description=joinednames[commaindex:]
                tosend=[embed,embed2]
            topcities=discord.Embed(title=f"Popular Cities - {flags[res['country']]} {dname} ({res['country']})",color=GREEN)
            # cur.execute("select city_id from count_info where server_id = ? and (country_code = ? or alt_country = ?) order by count desc limit 10",(interaction.guild_id,res['country'],res['country']))
            cur.execute("SELECT city_id, COUNT(*) AS c_counts FROM chain_info WHERE server_id = ? AND valid = ? AND user_id IS NOT NULL AND (country_code = ? or alt_country = ?) GROUP BY city_id ORDER BY c_counts DESC, city_id ASC LIMIT ?", (interaction.guild_id, True, res['country'], res["country"], 10))
            if cur.rowcount>0:
                citylist=[]
                for n,i in enumerate(cur.fetchall()):
                    default = city_default.row(by_predicate=pl.col('geonameid') == i[0], named=True)
                    citylist.append(f"{n+1}. {city_string(default['name'],admin1name(default['country'],default['admin1']),admin2name(default['country'],default['admin1'],default['admin2']),default['country'],default['alt-country'])}{':no_entry:' if default['deleted'] else ''} - **{i[1]:,}**")
                topcities.description='\n'.join(citylist)
                tosend.append(topcities)
            embed_sizes = [sum([len(j) for j in (i.title if i.title else '',i.description)]) for i in tosend]
            # while over limit send embeds individually
            while sum(embed_sizes)>6000:
                await interaction.followup.send(embed=tosend[0],ephemeral=(se=='no'))
                embed_sizes=embed_sizes[1:]      
                tosend=tosend[1:] 
            await interaction.followup.send(embeds=tosend,ephemeral=(se=='no'))      
        else:
            embed.description='There are no alternate names for this country.'
            await interaction.followup.send(embed=embed,ephemeral=(se=='no'))
    else:
        await interaction.followup.send('Country not recognized. Please try again. ',ephemeral=(se=='no'))

@tree.command(name='delete-stats',description='Deletes server stats.')
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
async def deletestats(interaction: discord.Interaction):
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
        return
    embed=discord.Embed(color=RED,title='Are you sure?',description='This action is irreversible. Game settings for the server will be preserved, but all records of chains placed will be removed, and all people who have been server-blocked will be unblocked.')
    view=Confirmation(interaction.guild_id,interaction.user.id)
    await interaction.response.send_message(embed=embed,view=view)
    view.message=await interaction.original_response()

@tree.command(description="Tests the client's latency. ")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message('Pong! `%s ms`'%(client.latency*1000))

@tree.command(name="block-server",description="Blocks a user from using the bot in the server. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
async def serverblock(interaction: discord.Interaction,member: discord.Member, reason: app_commands.Range[str,0,128], duration: Literal['1 Hour', '6 Hours', '24 Hours', '48 Hours', '7 Days', '1 Month', 'Permanent']):
    if member!=owner and not member.bot:
        if is_blocked(interaction.user.id,interaction.guild_id):
            await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
            return
        cur.execute("SELECT user_id FROM server_user_info WHERE user_id = ? AND server_id = ?", (member.id, interaction.guild_id))
        block_expiry = (datetime.datetime.now()+block_time_offset[duration]) if duration!='Permanent' else NEVER_TIMESTAMP
        if cur.rowcount:
            cur.execute("UPDATE server_user_info SET blocked = ?, block_reason = ?, block_expiry = ? WHERE user_id = ? AND server_id = ?",(True, reason, block_expiry, member.id, interaction.guild_id))
        else:
            cur.execute("INSERT INTO server_user_info(server_id, user_id, blocked, block_reason, block_expiry) VALUES (?, ?, ?, ?, ?)", (interaction.guild_id, member.id, True, reason, block_expiry))
        conn.commit()
        await interaction.response.send_message(f"<@{member.id}> has been blocked from using this bot in the server. Reason: `{reason}`, Expires: {f'<t:{int(block_expiry.timestamp())}:R>' if block_expiry!=NEVER_TIMESTAMP else '**Never**'}")
        if duration!='Permanent':
            await timed_unblock(interaction.guild_id,member.id,block_expiry,False)
    else:
        await interaction.response.send_message(f"Nice try, bozo")

def unblock(server_id, user, is_global:bool):
    if is_global:
        cur.execute("UPDATE global_user_info SET blocked = ?, block_reason = ?, block_expiry = ? WHERE user_id = ?", (False, None, None, user))
    else:
        cur.execute("UPDATE server_user_info SET blocked = ?, block_reason = ?, block_expiry = ? WHERE user_id = ? and server_id = ?", (False, None, None, user, server_id))
    conn.commit()

@tree.command(name="unblock-server",description="Unblocks a user from using the bot in the server. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guild_only()
async def serverunblock(interaction: discord.Interaction,member: discord.Member):
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute("SELECT blocked FROM global_user_info WHERE user_id = ?", (member.id,))
    if cur.rowcount:
        if cur.fetchone()[0]:
            await interaction.response.send_message(f":no_entry: <@{member.id}> cannot be unblocked. ")
            return
    unblock(interaction.guild_id,member.id,False)
    await interaction.response.send_message(f"<@{member.id}> has been unblocked from using this bot in the server. ")

@tree.command(name="block-global",description="Blocks a user from using the bot. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guilds(1126556064150736999)
@app_commands.guild_only()
async def globalblock(interaction: discord.Interaction,user: discord.User,reason: app_commands.Range[str,0,128], duration: Literal['1 Hour', '6 Hours', '24 Hours', '48 Hours', '7 Days', '1 Month', 'Permanent']):
    cur.execute("SELECT user_id FROM global_user_info WHERE user_id = ?", (user.id,))
    block_expiry = (datetime.datetime.now()+block_time_offset[duration]) if duration!='Permanent' else NEVER_TIMESTAMP
    if cur.rowcount:
        cur.execute("UPDATE global_user_info SET blocked = ?, block_reason = ?, block_expiry = ? WHERE user_id = ?", (True, reason, block_expiry, user.id))
    else:
        cur.execute("INSERT INTO global_user_info(user_id, blocked, block_reason, block_expiry) VALUES (?, ?, ?, ?)", (user.id, True, reason, block_expiry))
    conn.commit()
    await interaction.response.send_message(f"<@{user.id}> has been blocked from using this bot. Reason: `{reason}`, Expires: {f'<t:{int(block_expiry.timestamp())}:R>' if block_expiry!=NEVER_TIMESTAMP else '**Never**'}")
    if duration!='Permanent':
        await timed_unblock(interaction.guild_id,user.id,block_expiry,True)


@tree.command(name="unblock-global",description="Unblocks a user from using the bot. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guilds(1126556064150736999)
@app_commands.guild_only()
async def globalunblock(interaction: discord.Interaction,user: discord.User):
    unblock(interaction.guild_id, user.id, True)
    await interaction.response.send_message(f"<@{user.id}> has been unblocked from using this bot. ")

@tree.command(name="clear-processes",description="Clears list of cities for a given guild. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guilds(1126556064150736999)
@app_commands.guild_only()
async def clearprocesses(interaction: discord.Interaction, guild: str):
    if guild.isnumeric():
        processes[int(guild)] = []
        await interaction.response.send_message(f"Server `{guild}` has had its processes list cleared. ")
    else:
        await interaction.response.send_message(f"Please put in valid server ID. ")

@tree.command(description="Clears list of cities for a given guild. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guilds(1126556064150736999)
@app_commands.guild_only()
async def quit(interaction: discord.Interaction):
    await interaction.response.send_message(f"Disconnecting.")
    await client.close()


@tree.command(name='execute-sql',description="Executes SQL query. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guilds(1126556064150736999)
@app_commands.guild_only()
async def executesql(interaction: discord.Interaction, query: str, data: Optional[str]):
    await interaction.response.defer()
    try:
        data_tuple = []
        if data:
            for i in data.split(','):
                try:
                    data_tuple.append(ast.literal_eval(i))
                except:
                    data_tuple.append(i)
        cur.execute(query, data_tuple)
        conn.commit()
        await interaction.followup.send(f"Query executed:\n```sql\n{query}```\nData:\n```python\n{tuple(data_tuple)}```")
    except:
        await interaction.followup.send(f"Query could not be executed.")

@tree.command(name='send-logs',description="Sends last 500 lines of logs. ")
@app_commands.default_permissions(moderate_members=True)
@app_commands.guilds(1126556064150736999)
@app_commands.guild_only()
async def sendlogs(interaction: discord.Interaction):
    num_processes = [(len(processes[i]),i) for i in processes]
    num_processes = [i for i in num_processes if i[0]]
    num_processes.sort(reverse=True)

    lines = open(LOGGING_FILE+'.log').readlines()[-500:]
    await interaction.response.send_message("Number of cities being processed: ```%s```\n\nLast 500 lines of logfile:"%("\n".join(["%s\t%s"%(str(i[0]),str(i[1])) for i in num_processes]) if len(num_processes) else None),file=discord.File(io.StringIO(''.join(lines)),LOGGING_FILE+f'{datetime.datetime.now().strftime(r"-%Y%m%d-%H%M")}-500.log'))
    
@tree.command(description="Gets information about the bot and the game. ")
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
@app_commands.rename(se='show-everyone')
async def help(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    await interaction.response.defer(ephemeral=(se=='no'))
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    embed=discord.Embed(color=GREEN)
    command_messages=[[],[],[],[]]
    commands = tree.get_commands()
    for i in commands:
        if isinstance(i,discord.app_commands.Group):
            for j in i.commands:
                optional_params = [k.display_name for k in j.parameters if not k.required]
                if i.name=='set':
                    index=0
                elif i.name=='add' or i.name=='remove':
                    index=1
                elif i.name=='stats':
                    index=2
                command_messages[index].append(f"`/{i.name} {j.name} {''.join([f'[{k.display_name}]' for k in j.parameters if k.required])}{('(' if len(optional_params) else '')+''.join([f'[{k}]' for k in optional_params])+(')' if len(optional_params) else '')}`: {j.description}")
        else:
            optional_params = [j.display_name for j in i.parameters if not j.required]
            command_messages[3].append(f"`/{i.name} {''.join([f'[{j.display_name}]' for j in i.parameters if j.required])}{('(' if len(optional_params) else '')+''.join([f'[{j}]' for j in optional_params])+(')' if len(optional_params) else '')}`: {i.description}")
    headers = ['**Set Commands:**','**Features Commands:**','**Stats Commands:**','**Other Commands:**']
    for n in range(len(command_messages)):
        command_messages[n]=headers[n]+'\n\n'+'\n'.join(command_messages[n])
    embed.description="Choose a topic."
    await interaction.followup.send(embed=embed,view=Help([open("guides/setup.md", 'r', encoding="utf-8").read(),open("guides/rules.md", 'r', encoding="utf-8").read(),None,open("guides/emojis.md", 'r', encoding="utf-8").read(),open("guides/tips.md", 'r', encoding="utf-8").read(),open("guides/faq.md", 'r', encoding="utf-8").read()],command_messages,interaction.user.id),ephemeral=(se=='no'))
@tree.command(description="Information about the bot.")
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
@app_commands.rename(se='show-everyone')
async def about(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=(se=='no'))
        return
    embed = discord.Embed(color=GREEN,title="About Cities Chain Bot")
    embed.add_field(name="Ping",value=f"`{round(client.latency*1000, 3)}` ms")
    embed.add_field(name="Support Server",value="[Join the Discord](https://discord.gg/xTERJGpx5d)")
    embed.add_field(name="Suggestions Channel",value="<#1221861912657006612> (in the support server)")
    embed.add_field(name="Data Source",value="[Geonames](https://geonames.org) - [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)\n")
    embed.add_field(name="GitHub Repository",value="[GitHub](https://github.com/GlutenFreeGrapes/cities-chain)")
    embed.add_field(name="Legal",value="[Terms of Service](https://github.com/GlutenFreeGrapes/cities-chain/blob/main/legal/Terms_of_Service.md)\n[Privacy Policy](https://github.com/GlutenFreeGrapes/cities-chain/blob/main/legal/Privacy_Policy.md)")
    embed.set_footer(text = "Geonames data from (YYYY-MM-DD):  %s "%(metadata["GeonamesDataDate"]))
    await interaction.response.send_message(embed=embed,ephemeral=(se=='no'))

tree.add_command(assign)
tree.add_command(add)
tree.add_command(remove)
tree.add_command(stats)

# error handling
async def on_command_error(interaction:discord.Interaction, error:discord.app_commands.errors.CommandInvokeError, *args, **kwargs):
    # suppress 404 Not Found errors w/ code 10062
    if not (isinstance(error.original, discord.errors.NotFound) and (error.original.code == 10062)):
        embed = discord.Embed(title=f":x: Command Error", colour=BLUE)
        if 'options' in interaction.data['options'][0]:
            embed.add_field(name='Command', value=f"{interaction.data['name']} {interaction.data['options'][0]['name']}")
            embed.add_field(name='Parameters', value='\n'.join([f"**{i['name']}**: `{i['value']}`" for i in interaction.data['options'][0]['options']]))
        else:
            embed.add_field(name='Command', value=interaction.data['name'])
            embed.add_field(name='Parameters', value='\n'.join([f"**{i['name']}**: `{i['value']}`" for i in interaction.data['options']]))
        if interaction.guild_id:
            embed.add_field(name='Guild ID', value = str(interaction.guild_id))
        if interaction.user:
            embed.add_field(name='User ID', value = str(interaction.user.id))
        embed.add_field(name='Timestamp created',value=interaction.created_at.timestamp())
        embed.description = '```\n%s\n```' % traceback.format_exc()
        embed.timestamp = datetime.datetime.now()
        app_info = await client.application_info()
        owner = await client.fetch_user(app_info.team.owner_id)
        await owner.send(embed=embed)
        await send_log(owner)
tree.on_error=on_command_error

@client.event
# message error handling
async def on_error(event, *args, **kwargs):
    embed = discord.Embed(title=':x: Event Error', colour=RED)
    embed.add_field(name='Event', value=event)
    if args:
        embed.add_field(name='Message',value=args[0].content)
        if args[0].guild and isinstance(args[0], discord.Message):
            embed.add_field(name='Guild ID', value = str(args[0].guild.id))
            # empty process queue
            processes[args[0].guild] = []
            await args[0].reply(f'Ran into error processing city. Try running `/stats server` to see what has been registered.', mention_author=False)
        if args[0].author:
            embed.add_field(name='User ID', value = str(args[0].author.id))
        embed.add_field(name='Timestamp created',value=args[0].created_at.timestamp())
    embed.description = '```\n%s\n```' % traceback.format_exc()
    embed.timestamp = datetime.datetime.now()
    app_info = await client.application_info()
    owner = await client.fetch_user(app_info.team.owner_id)
    await owner.send(embed=embed)
    await send_log(owner)

async def send_log(owner):
    num_processes = [(len(processes[i]),i) for i in processes]
    num_processes = [i for i in num_processes if i[0]]
    num_processes.sort(reverse=True)

    lines = open(LOGGING_FILE+'.log').readlines()[-500:]
    await owner.send("Number of cities being processed: ```%s```\n\nLast 500 lines of logfile:"%("\n".join(["%s\t%s"%(str(i[0]),str(i[1])) for i in num_processes]) if len(num_processes) else None),file=discord.File(io.StringIO(''.join(lines)),LOGGING_FILE+f'{datetime.datetime.now().strftime(r"-%Y%m%d-%H%M")}-500.log'))

dt_fmt = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(filename=LOGGING_FILE+'.log', filemode='w', level = logging.INFO, format = '{asctime} [{levelname:<8}] {name}: {message}', style = '{', datefmt=dt_fmt)
formatter = logging.Formatter('{asctime} [{levelname:<8}] {name}: {message}', dt_fmt, style='{')
handler = logging.FileHandler(filename=LOGGING_FILE+'.log', encoding='utf-8', mode='w')
if __name__ == '__main__':
    client.run(env["DISCORD_TOKEN"], reconnect=True, log_handler=handler, log_level=logging.INFO, log_formatter=formatter)