import discord, re, pandas as pd, random, math, mariadb, numpy as np, tarfile, requests, unidecode, json, asyncio, io
from discord import app_commands
from discord.ext import commands
from typing import Optional,Literal
from os import environ as env
from dotenv import load_dotenv
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

citydata,countriesdata,admin1data,admin2data=pd.read_csv('cities.txt',sep='\t',keep_default_na=False,na_values='',dtype={'admin1':str,'admin2':str,'alt-country':str}),pd.read_csv('countries.txt',sep='\t',keep_default_na=False,na_values=''),pd.read_csv('admin1.txt',sep='\t',keep_default_na=False,na_values='',dtype={'admin1':str}),pd.read_csv('admin2.txt',sep='\t',keep_default_na=False,na_values='',dtype={'admin1':str,'admin2':str,})
citydata=citydata.fillna(np.nan).replace([np.nan], [None])
countriesdata=countriesdata.fillna(np.nan).replace([np.nan], [None])
admin1data=admin1data.fillna(np.nan).replace([np.nan], [None])
admin2data=admin2data.fillna(np.nan).replace([np.nan], [None])

GREEN = discord.Colour.from_rgb(0,255,0)
RED = discord.Colour.from_rgb(255,0,0)

env.setdefault("DB_NAME", "cities_chain")
conn = mariadb.connect(
    user=env["DB_USER"],
    password=env["DB_PASSWORD"],
    host=env["DB_HOST"],
    database=None)
cur = conn.cursor() 

cur.execute('create database if not exists ' + env["DB_NAME"])
cur.execute('use ' + env["DB_NAME"])
cur.execute("SET @@session.wait_timeout = 3600") # max 1hr timeout

cur.execute('''create table if not exists server_info(
            server_id bigint, 
            round_number int default 0, 
            min_repeat int default 50, 
            min_pop int default 1000, 
            choose_city bool default false, 
            repeats bool default true, 
            chain_end bool default true,
            channel_id bigint default -1, 
            current_letter char(1) default '-', 
            last_user bigint, 
            max_chain int default 0, 
            last_best int,
            prefix varchar(10) default '!',
            primary key(server_id))''')

cur.execute('''create table if not exists react_info(
            server_id bigint, 
            city_id int default -1,
            reaction varchar(50), 
            primary key(server_id,city_id),
            foreign key(server_id)
                references server_info(server_id))''')

cur.execute('''create table if not exists repeat_info(
            server_id bigint, 
            city_id int default -1,
            primary key(server_id,city_id),
            foreign key(server_id)
                references server_info(server_id))''')

cur.execute('''create table if not exists global_user_info(
            user_id bigint,
            correct int default 0, 
            incorrect int default 0, 
            score int default 0,
            last_active int,
            primary key(user_id))''')

cur.execute('''create table if not exists server_user_info(
            user_id bigint,
            server_id bigint, 
            correct int default 0, 
            incorrect int default 0, 
            score int default 0,
            last_active int,
            primary key(user_id,server_id),
            foreign key(server_id)
                references server_info(server_id))''')

cur.execute('''create table if not exists chain_info(
            server_id bigint, 
            user_id bigint,
            round_number int, 
            count int, 
            city_id int default -1,
            name varchar(3000),
            admin1 varchar(200),
            country varchar(100),
            country_code char(2),
            alt_country char(2),
            time_placed int,
            valid bool,
            primary key(server_id,city_id,round_number,count,time_placed),
            foreign key(server_id)
                references server_info(server_id))''')

cur.execute('''drop table if exists bans''')

cur.execute('''alter table global_user_info
               add column if not exists blocked bool default 0''')
cur.execute('''alter table server_user_info
               add column if not exists blocked bool default 0''')
cur.execute('''alter table chain_info
               add column if not exists leaderboard_eligible bool default 1''')

cur.execute('alter table chain_info add column if not exists admin2 varchar(100) default null after name')

cur.execute('''create table if not exists count_info(
            server_id bigint,
            city_id int,
            name varchar(200),
            admin1 varchar(200),
            country varchar(100),
            country_code char(2),
            alt_country varchar(100),
            count int,
            primary key(server_id,city_id))
            ''')

cur.execute('alter table count_info add column if not exists admin2 varchar(100) default null after name')

client = discord.Client(intents=intents)
tree=app_commands.tree.CommandTree(client)

allnames=citydata[citydata['default']==1]
allnames=allnames.set_index('geonameid')

countrydefaults=countriesdata[countriesdata['default']==1]
allcountries=list(countrydefaults['name'])
iso2={i:allcountries[n] for n,i in enumerate(countrydefaults['country'])}
iso3={i:allcountries[n] for n,i in enumerate(countrydefaults['iso3'])}
allcountries=sorted(allcountries)
regionalindicators={chr(97+i):chr(127462+i) for i in range(26)}
flags = {i:regionalindicators[i[0].lower()]+regionalindicators[i[1].lower()] for i in iso2}

def is_blocked(user_id,guild_id):
    cur.execute("select user_id,blocked from global_user_info where user_id=?",(user_id,))
    g = cur.fetchone()
    if g:
        if g[1]:
            return 1
    else:
        return 0
    cur.execute("select server_id,user_id,blocked from server_user_info where user_id=? and server_id=?",(user_id,guild_id))
    g = cur.fetchone()
    if g:
        return g[2]
    else:
        return 0

def admin1name(country,admin1):
    return admin1data[(admin1data['country']==country)&(admin1data['admin1']==admin1)&(admin1data['default']==1)].iloc[0]['name'] if admin1 else None

def admin2name(country,admin1,admin2):
    return admin2data[(admin2data['country']==country)&(admin2data['admin1']==admin1)&(admin2data['admin2']==admin2)&(admin2data['default']==1)].iloc[0]['name'] if admin2 else None

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

def search_cities(city,province,country,min_pop,include_deleted):
    city=re.sub(',$','',city.lower().strip())
    if city[-1]==',':
        city=city[:-1]
    if province:
        city+=","+province
    if country:
        city+=","+country
    return search_cities_chain(city,0,min_pop,include_deleted)

def search_cities_chain(query, checkApostrophe,min_pop,include_deleted):
    s=('name','decoded','punct-space','punct-empty')
    q=re.sub(',$','',query.lower().strip())
    if q[-1]==',':
        q=q[:-1]
    p=re.sub('\s*,\s*',',',q).split(',')
    city=p[0]
    res1=citydata[(citydata['name'].str.lower()==city)]
    res2=citydata[(citydata['decoded'].str.lower()==city)]
    res3=citydata[(citydata['punct-space'].str.lower()==city)]
    res4=citydata[(citydata['punct-empty'].str.lower()==city)]

    res1=res1.assign(match=0)
    res2=res2.assign(match=1)
    res3=res3.assign(match=2)
    res4=res4.assign(match=3)

    results=pd.concat([res1,res2,res3,res4])
    results=results.drop_duplicates(subset=('geonameid','name'))
    if len(p)==2:
        otherdivision=p[1]
        cchoice=countriesdata[(countriesdata['name'].str.lower()==otherdivision)|(countriesdata['country'].str.lower()==otherdivision)]
        a1choice=admin1data[(admin1data['name'].str.lower()==otherdivision)|(admin1data['admin1'].str.lower()==otherdivision)]
        a2choice=admin2data[(admin2data['name'].str.lower()==otherdivision)|(admin2data['admin2'].str.lower()==otherdivision)]
        cchoice=set(cchoice['country'])
        a1choice=set(zip(a1choice['country'],a1choice['admin1']))
        a2choice=set(zip(a2choice['country'],a2choice['admin1'],a2choice['admin2']))
        cresults=results[results['country'].isin(cchoice)|results['alt-country'].isin(cchoice)]
        rcol=results.columns
        a1results=pd.DataFrame(columns=rcol)
        for i in a1choice:
            a1results=pd.concat([a1results,results[(results['country']==i[0])&(results['admin1']==i[1])]])
        a2results=pd.DataFrame(columns=rcol)
        for i in a2choice:
            a2results=pd.concat([a2results,results[(results['country']==i[0])&(results['admin1']==i[1])&(results['admin2']==i[2])]])
        results=pd.concat([cresults,a1results,a2results])
        results=results.drop_duplicates()
    elif len(p)==3:
        otherdivision=p[1]
        country=p[2]
        cchoice=countriesdata[((countriesdata['name'].str.lower()==country)|(countriesdata['country'].str.lower()==country))]
        c=set(cchoice['country'])
        a1choice=admin1data[((admin1data['name'].str.lower()==otherdivision)|(admin1data['admin1'].str.lower()==otherdivision))&(admin1data['country'].isin(c))]
        a2choice=admin2data[((admin2data['name'].str.lower()==otherdivision)|(admin2data['admin2'].str.lower()==otherdivision))&(admin2data['country'].isin(c))]
        a1choice=set(zip(a1choice['country'],a1choice['admin1']))
        a2choice=set(zip(a2choice['country'],a2choice['admin1'],a2choice['admin2']))
        rcol=results.columns
        a1results=pd.DataFrame(columns=rcol)
        for i in a1choice:
            a1results=pd.concat([a1results,results[((results['country']==i[0]))&(results['admin1']==i[1])]])
        a2results=pd.DataFrame(columns=rcol)
        for i in a2choice:
            a2results=pd.concat([a2results,results[((results['country']==i[0]))&(results['admin1']==i[1])&(results['admin2']==i[2])]])
        results=pd.concat([a1results,a2results])
        results=results.drop_duplicates()
    elif len(p)>3:
        admin2=p[1]
        admin1=p[2]
        country=p[3]
        cchoice=countriesdata[(countriesdata['name'].str.lower()==country)|(countriesdata['country'].str.lower()==country)]
        c=set(cchoice['country'])
        a1choice=admin1data[(admin1data['name'].str.lower()==admin1)|(admin1data['admin1'].str.lower()==admin1)&(admin1data['country'].isin(c))]
        a1=set(a1choice['admin1'])
        a2choice=admin2data[(admin2data['name'].str.lower()==admin2)|(admin2data['admin2'].str.lower()==admin2)&(admin2data['country'].isin(c))&(admin2data['admin1'].isin(a1))]
        a2choice=set(zip(a2choice['country'],a2choice['admin1'],a2choice['admin2']))
        rcol=results.columns
        a2results=pd.DataFrame(columns=rcol)
        for i in a2choice:
            a2results=pd.concat([a2results,results[(results['country']==i[0])&(results['admin1']==i[1])&(results['admin2']==i[2])]])
        results=a2results.drop_duplicates()
    if not include_deleted:
        results = results[results['deleted']==0]
    if results.shape[0]==0:
        if checkApostrophe:
            return None
        else:
            return search_cities_chain(query.replace("`","'").replace("’","'").replace("ʻ","'").replace("ʼ","'"),1,min_pop,include_deleted)
    else:

        r=results.sort_values(['default','population','match'],ascending=[0,0,1]).head(1).iloc[0]
        
        # if population too small, look for larger options. if none, return original result
        if r['population']<min_pop:
            alternateresult=results.sort_values(['population','default','match'],ascending=[0,0,1]).head(1).iloc[0]
            if alternateresult['population']>=min_pop:
                r=alternateresult

        return (int(r['geonameid']),r,r[s[r['match']]])

def generate_map(city_id_list):
    coords = [allnames.loc[city_id][['latitude','longitude']] for city_id in city_id_list]
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
    if len(coords):
        if len(coords)>1:
            m = Basemap(llcrnrlon=max(-180,mlon-londif*(SCALING_FACTOR/2)),llcrnrlat=max(-90,mlat-latdif*(SCALING_FACTOR/2)),urcrnrlon=min(180,Mlon+londif*(SCALING_FACTOR/2)),urcrnrlat=min(90,Mlat+latdif*(SCALING_FACTOR/2)),resolution='l',projection='cyl')
        else:
            MARGIN_DEGREES = 5 # on each side
            m = Basemap(llcrnrlon=lons[0]-MARGIN_DEGREES,llcrnrlat=lats[0]-MARGIN_DEGREES,urcrnrlon=lons[0]+MARGIN_DEGREES,urcrnrlat=lats[0]+MARGIN_DEGREES,resolution='l',projection='cyl')
        x,y = m(lons,lats)

        m.fillcontinents()
        m.drawcountries(color='w')
        m.plot(x,y,marker='.',color='tab:blue',linewidth=1,markersize=2.5)
        img_buf = io.BytesIO()
        plt.savefig(img_buf,format='png',bbox_inches='tight')
        img_buf.seek(0)
        plt.clf()
        return discord.File(img_buf, filename='map.png')

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
        self.add_item(Selector(command_messages,[discord.SelectOption(label="Settings",description="/set commands",value="0"),discord.SelectOption(label="Reactions/Repeats",description="/add and /remove commands",value="1"),discord.SelectOption(label="Stats",description="/stats commands",value="2"),discord.SelectOption(label="Other Commands",description="Other commands",value="3")],"Choose a command group"))
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
    def __init__(self,page,blist,title,lens,user,embed):
        super().__init__(timeout=None)
        self.page=page
        self.blist=blist
        self.title=title
        self.lens=lens
        self.author=user
        self.embed=embed
        self.update_buttons()

    async def interaction_check(self,interaction):
        return interaction.user.id==self.author
    
    def update_buttons(self):
        if self.lens==1:
            for i in self.children:
                i.disabled=True
            self.embed.set_footer(text="Page %s/%s"%(1,1))
        elif self.page==1:
            self.children[0].disabled=True
            self.children[1].disabled=True
            self.embed.set_footer(text="Page %s/%s"%(1,self.lens))
            self.children[3].disabled=False
            self.children[4].disabled=False
        elif self.page==self.lens:
            self.children[0].disabled=False
            self.children[1].disabled=False
            self.embed.set_footer(text="Page %s/%s"%(self.lens,self.lens))
            self.children[3].disabled=True
            self.children[4].disabled=True
        else:
            for i in self.children:
                i.disabled=False
            self.embed.set_footer(text="Page %s/%s"%(self.page,self.lens))

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
        
    @discord.ui.button(label='⏮', style=discord.ButtonStyle.primary)
    async def front(self, interaction, button):
        self.page=1
        await self.updateembed(interaction)
    @discord.ui.button(label='⏴', style=discord.ButtonStyle.primary)
    async def prev(self, interaction, button):
        self.page=self.page-1
        await self.updateembed(interaction)
    @discord.ui.button(label='🛑', style=discord.ButtonStyle.danger)
    async def stop(self, interaction, button):
        for i in self.children:
            i.disabled=True
        await interaction.response.edit_message(embeds=self.message.embeds,view=self, attachments=self.message.attachments)
    @discord.ui.button(label='⏵', style=discord.ButtonStyle.primary)
    async def next(self, interaction, button):
        self.page=self.page+1
        await self.updateembed(interaction)
    @discord.ui.button(label='⏭', style=discord.ButtonStyle.primary)
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
            cur.execute('''delete from chain_info where server_id=?''',data=(interaction.guild_id,))
            cur.execute('''delete from count_info where server_id=?''',data=(interaction.guild_id,))
            cur.execute('''select user_id,correct,incorrect,score from server_user_info where server_id=?''',data=(interaction.guild_id,))
            for i in cur.fetchall():
                cur.execute('''select correct,incorrect,score from global_user_info where user_id=?''',data=(i[0],))
                j=cur.fetchone()
                if (i[1]-j[0]==0) and (i[2]-j[1]==0) and (i[3]-j[2]==0):
                    cur.execute('''delete from global_user_info where user_id=?''',data=(i[0],))
                else:
                    cur.execute('''select last_active from server_user_info where user_id=? and server_id!=? order by last_active desc''',data=(i[0],interaction.guild_id))
                    la=cur.fetchone()[0]
                    cur.execute('''update global_user_info 
                                    set correct = ?,incorrect = ?,score = ?,last_active = ? where user_id = ?''', data=(j[0]-i[1],j[1]-i[2],j[2]-i[3],la,i[0]))
            cur.execute('''delete from server_user_info where server_id=?''',data=(interaction.guild_id,))
            cur.execute('''delete from react_info where server_id=?''',data=(interaction.guild_id,))
            cur.execute('''delete from repeat_info where server_id=?''',data=(interaction.guild_id,))
            cur.execute('''update server_info set round_number=?,current_letter=?,last_user=?,max_chain=?,last_best=?,chain_end=? where server_id=?''',data=(0,'-',None,0,None,True,interaction.guild_id))
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
cur.execute('select server_id from server_info')
processes = {i:None for (i,) in cur}
@client.event
async def on_ready():
    global owner
    await tree.sync()
    app_info = await client.application_info()
    owner = await client.fetch_user(app_info.team.owner_id)
    cur.execute('select server_id from server_info')
    alr={i for (i,) in cur}
    empty={i.id for i in client.guilds}-alr
    for i in empty:
        cur.execute('''insert into server_info(server_id) VALUES (?)''',data=(i,))
    conn.commit()
    print(f'Logged in as {client.user} (ID: {client.user.id})\n------')

@client.event
async def on_guild_join(guild:discord.Guild):
    global processes
    processes[guild.id]=None
    cur.execute('''select * from server_info where server_id = ?''',(guild.id,))
    if cur.rowcount==0:
        cur.execute('''insert into server_info(server_id) VALUES (?)''',data=(guild.id,))
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            await channel.send('Hi! use /help to get more information on how to use this bot. ')
        break

async def owner_modcheck(interaction: discord.Interaction):
    return interaction.permissions.moderate_members or interaction.user==owner

assign = app_commands.Group(name="set", description="Set different things for the chain.",)
@app_commands.check(owner_modcheck)
@assign.command(description="Sets the channel for the bot to monitor for cities chain.")
@app_commands.describe(channel="The channel where the cities chain will happen")
async def channel(interaction: discord.Interaction, channel: discord.TextChannel|discord.Thread):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute('''update server_info
        set channel_id = ?
        where server_id = ?''', data=(channel.id,interaction.guild_id))
    conn.commit()
    await interaction.followup.send('Channel set to <#%s>.'%channel.id)

@app_commands.check(owner_modcheck)
@assign.command(description="Sets the gap between when a city can be repeated in the chain.")
@app_commands.describe(num="The minimum number of cities before they can repeat again, set to -1 to disallow any repeats")
async def repeat(interaction: discord.Interaction, num: app_commands.Range[int,-1,None]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute('''select chain_end from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()[0]  
    if c:
        if num==-1:
            cur.execute('''update server_info
                        set repeats = ?
                        where server_id = ?''', data=(False,interaction.guild_id))
            conn.commit()
            await interaction.followup.send('Repeats set to **OFF**. ')
        else:
            cur.execute('''select repeats from server_info
                        where server_id = ?''', data=(interaction.guild_id,))
            c=cur.fetchone()[0] 
            if c:
                await interaction.followup.send('Minimum number of cities before repeating set to **%s**.'%f'{num:,}')
            else:
                await interaction.followup.send('Repeats set to **ON**. ')
                await interaction.channel.send('Minimum number of cities before repeating set to **%s**.'%f'{num:,}')
            cur.execute('''update server_info
                        set repeats = ?,
                        min_repeat = ?
                        where server_id = ?''', data=(True,num,interaction.guild_id))
            conn.commit()
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@app_commands.check(owner_modcheck)
@assign.command(description="Sets the minimum population of cities in the chain.")
@app_commands.describe(population="The minimum population of cities in the chain")
async def population(interaction: discord.Interaction, population: app_commands.Range[int,1,None]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute('''select chain_end from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()[0] 
    if c:
        cur.execute('''update server_info
                    set min_pop = ?
                    where server_id = ?''', data=(population,interaction.guild_id))
        conn.commit()
        await interaction.followup.send('Minimum city population set to **%s**.'%f'{population:,}')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@app_commands.check(owner_modcheck)
@assign.command(description="Sets the prefix to listen to.")
@app_commands.describe(prefix="Prefix that all cities to be chained must begin with")
async def prefix(interaction: discord.Interaction, prefix: Optional[app_commands.Range[str,0,10]]=''):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute('''select chain_end from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()[0] 
    if c:
        cur.execute('''update server_info
                    set prefix = ?
                    where server_id = ?''', data=(prefix,interaction.guild_id))
        conn.commit()
        if prefix!='':
            await interaction.followup.send('Prefix set to **%s**.'%prefix)
        else:
            await interaction.followup.send('Prefix removed.')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@app_commands.check(owner_modcheck)
@assign.command(name='choose-city',description="Toggles if bot can choose starting city for the next chain.")
@app_commands.describe(option="on to let the bot choose the next city, off otherwise")
async def choosecity(interaction: discord.Interaction, option:Literal["on","off"]):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    guildid=interaction.guild_id
    cur.execute('''select chain_end,min_pop,round_number,choose_city from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()
    if c[0]:
        if option=='on':
            if c[3]:
                await interaction.followup.send('Choose_city is already **on**.')
            else:
                poss=allnames[allnames['population']>=c[1]]
                newid=int(random.choice(poss.index))
                entr=allnames.loc[(newid)]
                nname=poss.at[newid,'name']
                n=(nname,iso2[entr['country']],entr['country'],admin1name(entr['country'],entr['admin1']),admin2name(entr['country'],entr['admin1'],entr['admin2']),entr['alt-country'])
                cur.execute('''update server_info
                            set choose_city = ?,
                                current_letter = ?
                            where server_id = ?''', data=(True,entr['last-letter'],guildid))
                cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin2,admin1,country,country_code,alt_country,time_placed,valid)
                            values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,newid,c[2]+1,1,n[0],n[4],n[3],n[1],n[2],n[5],int(interaction.created_at.timestamp()),True))
                await interaction.followup.send('Choose_city set to **ON**. Next city is `%s`.'%nname)
        else:
            cur.execute('''update server_info
                        set choose_city = ?,
                            current_letter = ?
                        where server_id = ?''',data=(False,'-',guildid))
            cur.execute('''delete from chain_info where server_id = ? and round_number = ?''',data=(guildid,c[2]+1))
            await interaction.followup.send('Choose_city set to **OFF**. Choose the next city to start the chain. ')
        conn.commit()
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

async def countrycomplete(interaction: discord.Interaction, search: str):
    if search=='':
        return []
    s=search.lower()
    results=[i for i in allcountries if i.lower().startswith(s)]
    results.extend([iso2[i] for i in iso2 if i.lower().startswith(s) and iso2[i] not in results])
    results.extend([iso3[i] for i in iso3 if i.lower().startswith(s) and iso3[i] not in results])
    return [app_commands.Choice(name=i,value=i) for i in results[:10]]

add = app_commands.Group(name='add', description="Adds reactions/repeats for the chain.")
@app_commands.check(owner_modcheck)
@add.command(description="Adds reaction for a city. When cityed, react to client's message with emoji to react to city with.")
@app_commands.describe(city="The city that the client will react to",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def react(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    
    cur.execute('select min_pop from server_info where server_id=?',data=(interaction.guild_id,))
    minimum_population = cur.fetchone()[0]

    res=search_cities(city,province,country,minimum_population,0)
    if res:
        await interaction.followup.send('What reaction do you want for **%s**? React to this message with the emoji. '%res[2])
        msg = await interaction.original_response()
        def check(reaction, user):
            return reaction.message.id == msg.id and user == interaction.user
        cur.execute('''select city_id from react_info where server_id = ?''', data=(interaction.guild_id,))
        if cur.rowcount>0:
            reactset=set(cur.fetchall())
        else:
            reactset=set()
        try:
            reaction,user=await client.wait_for('reaction_add', check=check,timeout=60)
            if (res[0],) in reactset:
                cur.execute('''update react_info 
                        set reaction = ?
                        where server_id = ? and city_id = ?''', data=(str(reaction.emoji),interaction.guild_id,res[0]))
            else:
                cur.execute('''insert react_info(server_id,city_id,reaction)
                        values (?,?,?)''', data=(interaction.guild_id,res[0],str(reaction.emoji)))
            conn.commit()
            await interaction.edit_original_response(content='%s successfully added as reaction for **%s**. '%(reaction.emoji,res[2]))
        except asyncio.TimeoutError:
            if city in reactset:
                await interaction.edit_original_response(content='Interaction timed out. Previous reaction kept. ')
            else:
                await interaction.edit_original_response(content='Interaction timed out. No reaction added. ')
    else:
        await interaction.followup.send('City not recognized. Please try again. ')

@app_commands.check(owner_modcheck)
@add.command(description="Adds repeating exception for a city.")
@app_commands.describe(city="The city that the client will allow repeats for",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def repeat(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return

    cur.execute('''select chain_end,min_pop from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()  
    if c[0]:
        res=search_cities(city,province,country,c[1],0)
        if res:
            cur.execute('''select city_id from repeat_info where server_id = ?''', data=(interaction.guild_id,))
            if cur.rowcount>0:
                repeatset=set(cur.fetchall())
            else:
                repeatset=set()
            if (res[0],) not in repeatset:
                cur.execute('insert into repeat_info(server_id, city_id) values (?,?)',data=(interaction.guild_id,res[0]))
                conn.commit()
            await interaction.followup.send('**%s** can now be repeated. '%res[2])
        else:
            await interaction.followup.send('City not recognized. Please try again. ')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

remove = app_commands.Group(name='remove', description="Removes reactions/repeats for the chain.")
@app_commands.check(owner_modcheck)
@remove.command(description="Removes reaction for a city.")
@app_commands.describe(city="The city that the client will not react to",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def react(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return
    
    cur.execute('select min_pop from server_info where server_id=?',data=(interaction.guild_id,))
    minimum_population = cur.fetchone()[0]

    res=search_cities(city,province,country,minimum_population,1)
    if res:
        try:
            cur.execute('delete from react_info where server_id = ? and city_id = ?', data=(interaction.guild_id,res[0]))
            conn.commit()
            await interaction.followup.send('Reaction for %s removed.'%res[2])
        except:
            await interaction.followup.send('%s had no reactions.'%res[2])
    else:
        await interaction.followup.send('City not recognized. Please try again. ')

@app_commands.check(owner_modcheck)
@remove.command(description="Removes repeating exception for a city.")
@app_commands.describe(city="The city that the client will disallow repeats for",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def repeat(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.followup.send(":no_pedestrians: You are blocked from using this bot. ")
        return

    cur.execute('''select chain_end,min_pop from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone() 
    if c[0]:
        res=search_cities(city,province,country,c[1],1)
        if res:
            try:
                cur.execute('delete from repeat_info where server_id = ? and city_id = ?', data=(interaction.guild_id,res[0]))
                conn.commit()
                await interaction.followup.send('Repeating for %s removed.'%res[2])
            except:
                await interaction.followup.send('%s had no repeats.'%res[2])
        else:
            await interaction.followup.send('City not recognized. Please try again. ')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@client.event
async def on_message_delete(message:discord.Message):
    if message.guild:
        guildid = message.guild.id
        cur.execute('''select last_user,channel_id,current_letter,chain_end from server_info where server_id=?''',data=(guildid,))
        minfo=cur.fetchone()
        if ((message.author.id,message.channel.id)==minfo[:2]):
            cur.execute('''select last_active from server_user_info where user_id=? and server_id=?''',data=(minfo[0],guildid))
            t = cur.fetchone()[0]
            if int(message.created_at.timestamp())==t and not minfo[3]:
                cur.execute('''select name,valid from chain_info where message_id=?''',data=(message.id,))
                if cur.rowcount:
                    (name,valid)=cur.fetchone()
                    if valid:
                        await message.channel.send("<@%s> has deleted their city of `%s`. The next letter is `%s`."%(minfo[0],name,minfo[2]))

@client.event
async def on_message_edit(message:discord.Message, after:discord.Message):
    if message.guild:
        guildid = message.guild.id
        cur.execute('''select last_user,channel_id,current_letter from server_info where server_id=?''',data=(guildid,))
        minfo=cur.fetchone()
        if ((message.author.id,message.channel.id)==minfo[:2]):
            cur.execute('''select name from chain_info where message_id=? and server_id=?''',(message.id,guildid))
            if cur.rowcount and not message.edited_at:
                await message.channel.send("<@%s> has edited their city of `%s`. The next letter is `%s`."%(minfo[0],cur.fetchone()[0],minfo[2]))

@client.event
async def on_message(message:discord.Message):
    content = message.content
    global processes
    authorid=message.author.id
    if message.guild and authorid!=client.user.id:
        guildid=message.guild.id
        cur.execute('''select
                        channel_id,
                        prefix
                    from server_info
                    where server_id = ?''',data=(guildid,))
        channel_id, prefix=cur.fetchone()
        if message.channel.id==channel_id and not message.author.bot:
            if content.strip().startswith(prefix) and message.content[len(prefix):].strip()!='':
                # IF THERE IS A CITY BEING PROCESSED, ADD IT TO THE QUEUE AND EVENTUALLY IT WILL BE REACHED. OTHERWISE PROCESS IMMEDIATELY WHILE KEEPING IN MIND THAT IT IS CURRENTLY BEING PROCESSED
                msgref = discord.MessageReference.from_message(message,fail_if_not_exists=0)
                if processes[guildid]: 
                    processes[guildid].append((message,guildid,authorid,content,msgref))
                else:
                    processes[guildid]=[(message,guildid,authorid,message.content,msgref)]
                    await asyncio.create_task(chain(message,guildid,authorid,content,msgref))
            elif re.search(r"(?<!not )\b((mb)|(my bad)|(oop(s?))|(s(o?)(r?)ry))\b",message.content,re.I):
                await message.reply("it's ok")

async def chain(message:discord.Message,guildid,authorid,original_content,ref):
    if is_blocked(authorid,guildid):
        await message.add_reaction('\N{NO PEDESTRIANS}')
    else:
        cur.execute('''select round_number,
                    chain_end
                from server_info
                where server_id = ?''',data=(guildid,))
        round_num,chain_ended = cur.fetchone()
        cur.execute('''select * from server_user_info where user_id = ? and server_id = ?''',data=(authorid,guildid))
        if cur.rowcount==0:
            cur.execute('''select * from global_user_info where user_id = ?''',data=(authorid,))
            if cur.rowcount==0:
                cur.execute('''insert into global_user_info(user_id) values (?)''',data=(authorid,))
            cur.execute('''insert into server_user_info(user_id,server_id) values (?,?)''',data=(authorid,guildid))
        if chain_ended:
            cur.execute('''update server_info set chain_end = ?, round_number = ? where server_id = ?''',data=(False,round_num+1,guildid))
        cur.execute('''select
                    round_number,
                    min_repeat,
                    min_pop,
                    choose_city,
                    repeats,
                    chain_end,
                    channel_id,
                    current_letter,
                    last_user,
                    max_chain,
                    prefix
                from server_info
                where server_id = ?''',data=(guildid,))
        sinfo=cur.fetchone()
        cur.execute('''select city_id from chain_info where server_id = ? and round_number = ? order by count desc''',data=(guildid,sinfo[0]))
        citieslist=[i for (i,) in cur]
        if len(citieslist):
            cur.execute('''select leaderboard_eligible from chain_info where server_id = ? and round_number = ? order by count desc''',data=(guildid,sinfo[0]))
            l_eligible=cur.fetchone()[0]
        else:
            l_eligible=1
        res=search_cities_chain(original_content[len(sinfo[10]):],0,sinfo[2],0)
        if res:
            cur.execute('''select
                    round_number,
                    min_repeat,
                    min_pop,
                    choose_city,
                    repeats,
                    chain_end,
                    channel_id,
                    current_letter,
                    last_user,
                    max_chain,
                    prefix
                from server_info
                where server_id = ?''',data=(guildid,))
            sinfo=cur.fetchone()
            j=allnames.loc[(res[0])]
            name,adm2,adm1,country,altcountry=j['name'],j['admin2'],j['admin1'],j['country'],j['alt-country']
            if adm1:
                adm1=admin1name(country,adm1)
                if adm2:
                    adm2=admin2name(country,j['admin1'],adm2)
            n=(res[2]+(' ('+name+')'if not ((res[2].replace(' ','').isalpha() and res[2].isascii()) or res[1]['default']==1) else ""),(iso2[country],country),adm1,adm2,altcountry)
            letters=(res[1]['first-letter'],res[1]['last-letter'])
            if (sinfo[7]=='-' or sinfo[7]==letters[0]):
                if sinfo[2]<=res[1]['population']:
                    cur.execute('''select city_id from repeat_info where server_id = ?''', data=(guildid,))
                    if cur.rowcount>0:
                        repeatset={b[0] for b in cur.fetchall()}
                    else:
                        repeatset=set()
                    if ((sinfo[4] and res[0] not in set(citieslist[:sinfo[1]])) or (not sinfo[4] and res[0] not in set(citieslist)) or (res[0] in repeatset)):
                        if sinfo[8]!=message.author.id:
                            cur.execute('''select correct,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                            uinfo=cur.fetchone()
                            cur.execute('''update server_user_info set correct = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]+1,int(message.created_at.timestamp()),guildid,authorid))
                            cur.execute('''select correct,score from global_user_info where user_id=?''',data=(authorid,))
                            uinfo=cur.fetchone()
                            cur.execute('''update global_user_info set correct = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]+1,int(message.created_at.timestamp()),authorid))
                            cur.execute('''update server_info set last_user = ?, current_letter = ? where server_id = ?''',data=(authorid,letters[1],guildid))
                            cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin2,admin1,country,country_code,alt_country,time_placed,valid,message_id,leaderboard_eligible) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[3],n[2],n[1][0],n[1][1],n[4],int(message.created_at.timestamp()),True,message.id,l_eligible))
                            
                            cur.execute('''select count from count_info where server_id = ? and city_id = ?''',data=(guildid,res[0]))
                            if cur.rowcount==0:
                                
                                cur.execute('''insert into count_info(server_id,city_id,name,admin2,admin1,country,country_code,alt_country,count) values (?,?,?,?,?,?,?,?,?)''',data=(guildid,res[0],allnames.loc[res[0]]['name'],n[3],n[2],n[1][0],n[1][1],n[4],1))
                            else:
                                citycount = cur.fetchone()[0]
                                cur.execute('''update count_info set count=? where server_id=? and city_id=?''',data=(citycount+1,guildid,res[0]))
                            
                            # IF CITY APPEARS MORE THAN ONCE IN 50 CITIES, THE ROUND IS NOT ELIGIBLE FOR ALL LEADERBOARDS
                            if res[0] in set(citieslist[:50]):
                                cur.execute('''update chain_info set leaderboard_eligible=? where server_id=? and round_number=?''',(False,guildid,sinfo[0]))

                            try:
                                if sinfo[9]<(len(citieslist)+1):
                                    cur.execute('''update server_info set max_chain = ?,last_best = ? where server_id = ?''',data=(len(citieslist)+1,int(message.created_at.timestamp()),guildid))
                                    await message.add_reaction('\N{BALLOT BOX WITH CHECK}')
                                else:
                                    await message.add_reaction('\N{WHITE HEAVY CHECK MARK}')
                                await message.add_reaction(regionalindicators[country[0].lower()]+regionalindicators[country[1].lower()])
                                
                                
                                if (country=="GB"):
                                    if adm1=="Scotland":
                                        await message.add_reaction("🏴󠁧󠁢󠁳󠁣󠁴󠁿")
                                    elif adm1=="Wales":
                                        await message.add_reaction("🏴󠁧󠁢󠁷󠁬󠁳󠁿")

                                if altcountry:
                                    await message.add_reaction(regionalindicators[altcountry[0].lower()]+regionalindicators[altcountry[1].lower()])
                                cur.execute('''select reaction from react_info where server_id = ? and city_id = ?''', data=(guildid,res[0]))
                                if cur.rowcount>0:
                                    await message.add_reaction(cur.fetchone()[0])
                                if not ((res[2].replace(' ','').isalpha() and res[2].isascii() and original_content[len(sinfo[10]):].find(',')<0)):
                                    await message.add_reaction(regionalindicators[letters[1]])
                            except:
                                pass
                        else:
                            await fail(message,"**No going twice.**",sinfo,citieslist,res,n,True,ref)
                    else:
                        if sinfo[4]:
                            await fail(message,"**No repeats within `%s` cities.**"%f"{sinfo[1]:,}",sinfo,citieslist,res,n,True,ref)
                        else:
                            await fail(message,"**No repeats.**",sinfo,citieslist,res,n,True,ref)
                else:
                    await fail(message,"**City must have a population of at least `%s`.**"%f"{sinfo[2]:,}",sinfo,citieslist,res,n,True,ref)
            else:
                await fail(message,"**Wrong letter.**",sinfo,citieslist,res,n,True,ref)
        else:
            await fail(message,"**City not recognized.**",sinfo,citieslist,None,None,False,ref)
        conn.commit()

    # remove this from the queue of messages to process
    processes[guildid].pop(0)
    # if queue of other cities to process empty, set to none again. otherwise, process next city
    if len(processes[guildid])==0:
        processes[guildid]=None
    else:
        await asyncio.create_task(chain(*processes[guildid][0]))

@tree.command(name="fix-leaderboard",description="Fixes leaderboard stats, determines leaderboard eligible runs")
@commands.is_owner()
async def fixle(interaction:discord.Interaction):
    await interaction.response.send_message("starting")
    # select all rounds with <=50, count distinct ones for each one
    cur.execute('select * from (select server_id,round_number,max(count) as max_count from chain_info group by server_id,round_number) as x where max_count>50 order by max_count desc')
    rounds = [i for i in cur]
    # update rounds less than 50
    cur.execute('update chain_info inner join (select server_id,round_number,distinct_cities,max_count,case when distinct_cities=max_count then 1 else 0 end as le from (select server_id,round_number,count(distinct(city_id)) as distinct_cities,max(count) as max_count from chain_info where valid=1 group by server_id,round_number) as x where max_count<=50) as y on chain_info.server_id=y.server_id and chain_info.round_number=y.round_number set leaderboard_eligible = y.le ')
    for n_,(server,rnd,_) in enumerate(rounds):
        cur.execute('select city_id from chain_info where valid = 1 and server_id = ? and round_number = ? order by count asc',(server,rnd))
        citieslist = cur.fetchall()
        cll=len(citieslist)
        if len(set(citieslist))!=cll:
            # go through list and check
            for n,i in enumerate(citieslist):
                if (n+50)<=cll:
                    if len(set(citieslist[n:n+50]))!=50:
                        cur.execute('update chain_info set leaderboard_eligible=? where server_id=? and round_number=?',(False,server,rnd))
                        break
        if (n_+1)%50==0:
            conn.commit()
            await interaction.channel.send(content=f"{n_+1}/{len(rounds)} ({(n_+1)/len(rounds)*100}%)")
    conn.commit()
    await interaction.channel.send(content=f"{len(rounds)}/{len(rounds)} (100%)")

@tree.command(name="fix-names",description="Fixes admin1/admin2 names in chain and count info")
@commands.is_owner()
async def fixnames(interaction:discord.Interaction):
    await interaction.response.defer()
    await interaction.followup.send("starting")
    cur.execute('select distinct city_id from chain_info where city_id!=-1 order by country_code,city_id asc')
    cities = [i for (i,) in cur]
    a1s={}
    a2s={}
    cits=[]
    for n,i in enumerate(cities):
        c_info = allnames.loc[i]
        c,a1,a2,ac=c_info['country'],c_info['admin1'],c_info['admin2'],c_info['alt-country']
        if a1:
            if f'''{c}.{a1}''' in a1s:
                a1n=a1s[f'''{c}.{a1}''']
            else:
                a1n=admin1name(c,a1)
                a1s[f'''{c}.{a1}''']=a1n
            if a2:
                if f'''{c}.{a1}.{a2}''' in a2s:
                    a2n=a2s[f'''{c}.{a1}.{a2}''']
                else:
                    a2n=admin2name(c,a1,a2)
                    a2s[f'''{c}.{a1}.{a2}''']=a2n
            else:
                a2n=None
        else:
            a1n,a2n=None,None
            # cur.exe
        cits.append((i,c_info['name'],c,a1n,a2n,c_info['alt-country']))
        # cur.execute('update count_info set name=?,admin1=?,admin2=?,country=?,country_code=?,alt_country=? where city_id=?',(c_info['name'],a1n,a2n,iso2[c],c,c_info['alt-country'],i))
        # cur.execute('update chain_info set admin1=?,admin2=?,country=?,country_code=?,alt_country=? where city_id=?',(a1n,a2n,iso2[c],c,c_info['alt-country'],i))
        if (n+1)%500==0:
            await interaction.channel.send(content=f"{n+1}/{len(cities)} ({(n+1)/len(cities)*100}%)")
    await interaction.channel.send(content=f"{len(cities)}/{len(cities)} (100%)")
    cur.executemany('update count_info set name=?,admin1=?,admin2=?,country=?,country_code=?,alt_country=? where city_id=?',[(name,admin1,admin2,iso2[country],country,alt_country,c_id) for c_id,name,country,admin1,admin2,alt_country in cits])
    await interaction.channel.send(content='updated count_info')
    cur.executemany('update chain_info set admin1=?,admin2=?,country=?,country_code=?,alt_country=? where city_id=?',[(admin1,admin2,iso2[country],country,alt_country,c_id) for c_id,name,country,admin1,admin2,alt_country in cits])
    await interaction.channel.send(content='updated chain_info')
    conn.commit()
async def fail(message:discord.Message,reason,sinfo,citieslist,res,n,cityfound,msgref):
    guildid=message.guild.id
    authorid=message.author.id
    try:
        await message.add_reaction('\N{CROSS MARK}')
    except:
        pass


    cur.execute('''select incorrect,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
    uinfo=cur.fetchone()
    cur.execute('''update server_user_info set incorrect = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),guildid,authorid))
    cur.execute('''select incorrect,score from global_user_info where user_id=?''',data=(authorid,))
    uinfo=cur.fetchone()
    cur.execute('''update global_user_info set incorrect = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),authorid))
    if sinfo[3]:
        poss=allnames[allnames['population']>=sinfo[2]]
        newid=int(random.choice(poss.index))
        await message.channel.send('<@%s> RUINED IT AT **%s**!! Start again from `%s` (next letter `%s`). %s'%(authorid,f"{len(citieslist):,}",poss.at[newid,'name'],poss.at[newid,'last-letter'],reason), reference = msgref)
    else:
        await message.channel.send('<@%s> RUINED IT AT **%s**!! %s'%(authorid,f"{len(citieslist):,}",reason), reference = msgref)
    cur.execute('''select leaderboard_eligible from chain_info where server_id = ? and round_number = ? order by count desc''',data=(guildid,sinfo[0]))
    if cur.rowcount:
        l_eligible=cur.fetchone()[0]
    else:
        l_eligible=1
    if cityfound:
        cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin2,admin1,country,country_code,alt_country,time_placed,valid,message_id,leaderboard_eligible) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[3],n[2],n[1][0],n[1][1],n[4],int(message.created_at.timestamp()),False,message.id,l_eligible))
    else:
        cur.execute('''insert into chain_info(server_id,user_id,round_number,count,name,time_placed,valid,message_id,leaderboard_eligible) values (?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,message.content[len(sinfo[10]):],int(message.created_at.timestamp()),False,message.id,l_eligible))
    cur.execute('''update server_info set chain_end = ?, current_letter = ?, last_user = ? where server_id = ?''',data=(True,'-',None,guildid))
    if sinfo[3]:
        entr=allnames.loc[(newid)]
        nname=poss.at[newid,'name']
        n=(nname,iso2[entr['country']],entr['country'],admin1name(entr['country'],entr['admin1']),admin2name(entr['country'],entr['admin1'],entr['admin2']),entr['alt-country'])
        cur.execute('''update server_info
                    set choose_city = ?,
                        current_letter = ?
                    where server_id = ?''', data=(True,entr['last-letter'],guildid))
        cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin2,admin1,country,country_code,alt_country,time_placed,valid)
                    values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,int(newid),sinfo[0]+1,1,n[0],n[4],n[3],n[1],n[2],n[5],int(message.created_at.timestamp()),True))
    conn.commit()

stats = app_commands.Group(name='stats',description="description")
@app_commands.rename(se='show-everyone')
@stats.command(description="Displays server statistics.")
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def server(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    embed=discord.Embed(title="Server Stats", color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    cur.execute('select round_number,min_repeat,min_pop,choose_city,repeats,current_letter,last_user,max_chain,last_best,prefix from server_info where server_id = ?',data=(guildid,))
    sinfo=cur.fetchone()
    cur.execute('select * from chain_info where server_id = ? and round_number = ?',data=(guildid,sinfo[0]))
    embed.description='Round: **%s**\nCurrent letter: **%s**\nCurrent length: **%s**\nLast user: **%s**\nLongest chain: **%s** %s\nMinimum population: **%s**\nChoose city: **%s**\nRepeats: **%s**\nPrefix: %s'%(f'{sinfo[0]:,}',sinfo[5],f'{cur.rowcount:,}','<@'+str(sinfo[6])+'>' if sinfo[6] else '-',f'{sinfo[7]:,}','<t:'+str(sinfo[8])+':R>' if sinfo[8] else '',f'{sinfo[2]:,}','enabled' if sinfo[3] else 'disabled', 'only after %s cities'%f'{sinfo[1]:,}' if sinfo[4] else 'disallowed','**'+sinfo[9]+'**' if sinfo[9]!='' else None)
    await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays user statistics.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(member="The user to get statistics for.",se='Yes to show everyone stats, no otherwise')
async def user(interaction: discord.Interaction, member:Optional[discord.Member]=None,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    if not member:
        member=interaction.user
    cur.execute('select correct,incorrect,score,last_active from global_user_info where user_id = ?',data=(member.id,))
    if cur.rowcount==0:
        if member.id==interaction.user.id:
            await interaction.followup.send(embed=discord.Embed(color=RED,description='You must join the chain to use that command. '),ephemeral=eph)
        else:
            await interaction.followup.send(embed=discord.Embed(color=RED,description=f'<@{member.id}> has no Cities Chain stats. '),ephemeral=eph)
    else:
        uinfo=cur.fetchone()
        embed=discord.Embed(title="User Stats", color=GREEN)
        
        if (uinfo[0]+uinfo[1])>0:
            embed.add_field(name='Global Stats',value=f"Correct: **{f'{uinfo[0]:,}'}**\nIncorrect: **{f'{uinfo[1]:,}'}**\nCorrect Rate: **{round(uinfo[0]/(uinfo[0]+uinfo[1])*10000)/100}%**\nScore: **{f'{uinfo[2]:,}'}**\nLast Active: <t:{uinfo[3]}:R>",inline=True)
        cur.execute('select correct,incorrect,score,last_active from server_user_info where user_id = ? and server_id = ?',data=(member.id,interaction.guild_id))
        if cur.rowcount>0:
            uinfo=cur.fetchone()
            embed.add_field(name='Stats for ```%s```'%interaction.guild.name,value=f"Correct: **{f'{uinfo[0]:,}'}**\nIncorrect: **{f'{uinfo[1]:,}'}**\nCorrect Rate: **{round(uinfo[0]/(uinfo[0]+uinfo[1])*10000)/100}%**\nScore: **{f'{uinfo[2]:,}'}**\nLast Active: <t:{uinfo[3]}:R>",inline=True)
        
    
        favcities = discord.Embed(title=f"Favorite Cities/Countries", color=GREEN)
        favc = []
        # (THANKS MARENS FOR SQL CODE)
        cur.execute('SELECT city_id, COUNT(*) AS use_count FROM chain_info WHERE server_id = ? AND user_id = ? AND valid = 1 GROUP BY city_id ORDER BY use_count DESC',data=(interaction.guild_id,member.id))
        for i in cur:
            if len(favc)==10:
                break
            if i[0] in allnames.index:
                cityrow = allnames.loc[i[0]]
                favc.append(city_string(cityrow['name'],admin1name(cityrow['country'],cityrow['admin1']),admin2name(cityrow['country'],cityrow['admin1'],cityrow['admin2']),cityrow['country'],cityrow['alt-country'])+f' - **{i[1]:,}**')
        favcities.add_field(name='Cities',value='\n'.join([f"{n+1}. "+i for n,i in enumerate(favc)]))
        
        cur.execute('SELECT country_code, COUNT(*) AS use_count FROM chain_info WHERE server_id = ? AND user_id = ? AND valid = 1 GROUP BY country_code ORDER BY use_count DESC',data=(interaction.guild_id,member.id))
        countryuses = {i[0]:i[1] for i in cur}
        cur.execute('SELECT alt_country, COUNT(*) AS use_count FROM chain_info WHERE server_id = ? AND user_id = ? AND valid = 1 AND alt_country IS NOT NULL GROUP BY alt_country ORDER BY use_count DESC',data=(interaction.guild_id,member.id))
        for i in cur:
            if i[0] in cur:
                countryuses[i[0]]+=i[1]
            else:
                countryuses[i[0]]=i[1]
        fav_countries = [f"{j[1]} {j[2]} - **{j[0]:,}**" for j in sorted([(countryuses[i],iso2[i],flags[i]) for i in countryuses],reverse=1)[:10]]
        favcities.add_field(name='Countries',value='\n'.join([f"{n+1}. "+i for n,i in enumerate(fav_countries)]))
        if member.avatar:
            embed.set_author(name=member.name, icon_url=member.avatar.url)
            favcities.set_author(name=member.name, icon_url=member.avatar.url)
        else:
            embed.set_author(name=member.name)
            favcities.set_author(name=member.name)
        await interaction.followup.send(embeds=[embed,favcities],ephemeral=eph)

@stats.command(description="Displays list of cities.")
@app_commands.rename(se='show-everyone',showmap='map')
@app_commands.describe(order='The order in which the cities are presented, sequential or alphabetical',cities='Whether to show all cities or only the ones that cannot be repeated',showmap='Whether to show a map of cities',se='Yes to show everyone stats, no otherwise')
async def cities(interaction: discord.Interaction,order:Literal['sequential','alphabetical'],cities:Literal['all','non-repeatable'],showmap:Optional[Literal['yes','no']]='no',se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    cit=cities.capitalize()+" Cities"
    title=f"{cit} - {order.capitalize()}"
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    cur.execute('''select round_number,repeats,min_repeat,chain_end from server_info where server_id = ?''',data=(guildid,))
    s=cur.fetchone()
    cur.execute('''select city_id from repeat_info where server_id = ?''', data=(interaction.guild_id,))
    repeated={i[0] for i in cur}
    cur.execute('''select name,admin1,admin2,country_code,alt_country,city_id,valid from chain_info where server_id = ? and round_number = ? order by count desc''',data=(guildid,s[0]))
    if cur.rowcount>0:
        cutoff=[]
        
        cityids = []
        
        for i in cur:
            if i[6]:
                cityids.append(i[5])
            cutoff.append((city_string(i[0],i[1],i[2],i[3],i[4])+(f"{':no_entry:' if (i[5]!=-1 and allnames.loc[i[5],'deleted']) else ''}{':repeat:' if i[5] in repeated else ''}"),i[6]))
        if s[1] and cit.startswith("N"):
            cutoff=cutoff[:s[2]]
            cityids=cityids[:s[2]]
        seq=[':x: %s'%i[0] for i in cutoff if not i[1]]+['%s. %s'%(n+1,i[0]) for n,i in enumerate([j for j in cutoff if j[1]])]
        alph=['- '+i[0] for i in sorted(cutoff,key=lambda x:x[0].lower()) if i[1]]
        
        embed=discord.Embed(title=title, color=GREEN)
        if order.startswith('s'):
            embed.description='\n'.join(seq[:25])
            view=Paginator(1,seq,title,math.ceil(len(seq)/25),interaction.user.id,embed)
        else:
            embed.description='\n'.join(alph[:25])
            view=Paginator(1,alph,title,math.ceil(len(alph)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph,files=[generate_map(cityids)] if showmap=='yes' else [])
        view.message=await interaction.original_response()
    else:
        embed=discord.Embed(title=title, color=GREEN,description='```null```')
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='round',description="Displays all cities said for one round.")
@app_commands.rename(se='show-everyone',showmap='map')
@app_commands.describe(round_num='Round to retrieve information from (0 = current round, -1 = previous round)',showmap='Whether to show a map of cities',se='Yes to show everyone stats, no otherwise')
async def roundinfo(interaction: discord.Interaction,round_num:app_commands.Range[int,-1,None],showmap:Optional[Literal['yes','no']]='no',se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    cur.execute('''select round_number from server_info where server_id = ?''',data=(guildid,))
    s=cur.fetchone()

    if round_num<=0:
        round_num=s[0]+round_num

    cur.execute('''select name,admin1,admin2,country_code,alt_country,city_id,valid,count from chain_info where server_id = ? and round_number = ? order by count asc''',data=(guildid,round_num))
    if 1<=round_num<=s[0]:
        cutoff=[]

        cityids=[]

        for i in cur:
            if i[6]:
                cityids.append(i[5])
            cutoff.append(('%s. '%i[7]if i[6] else ':x: ') + city_string(i[0],i[1],i[2],i[3],i[4]))
        embed=discord.Embed(title="Round %s"%(f'{round_num:,}'), color=GREEN,description='\n'.join(cutoff[:25]))
        if interaction.guild.icon:
            embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
        else:
            embed.set_author(name=interaction.guild.name)
        view=Paginator(1,cutoff,"Round %s"%(f'{round_num:,}'),math.ceil(len(cutoff)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph,files=[generate_map(cityids)] if showmap=='yes' else [])
        view.message=await interaction.original_response()
    else:
        if s[0]:
            await interaction.followup.send("Round_num must be a number between **1** and **%s**."%s[0],ephemeral=eph)
        else:
            await interaction.followup.send("No rounds played yet.",ephemeral=eph)

@stats.command(description="Displays serverwide user leaderboard.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def slb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title=f"```{interaction.guild.name}``` LEADERBOARD",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    cur.execute('''select user_id,score from server_user_info where server_id = ? order by score desc''',data=(interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[f'{n+1}. <@{i[0]}> - **{f"{i[1]:,}"}**' for n,i in enumerate(cur)]
        embed.description='\n'.join(fmt[:25])
        await interaction.followup.send(embed=embed,view=Paginator(1,fmt,embed.title,math.ceil(len(fmt)/25),interaction.user.id,embed),ephemeral=eph)    
    else:
        embed.description='```null```'
        await interaction.followup.send(embed=embed,ephemeral=eph)    

@stats.command(description="Displays global leaderboard of maximum scores for servers.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def lb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title=f"SERVER HIGH SCORES",color=GREEN)
    cur.execute('SELECT server_id,MAX(count) AS mc FROM chain_info WHERE valid=1 AND leaderboard_eligible=1 GROUP BY `server_id` ORDER BY mc DESC')
    if cur.rowcount>0:
        top=[]
        counter=0
        for i in cur:
            server_from_id = client.get_guild(i[0])
            if server_from_id:
                counter+=1
                top.append(f'{counter}. {server_from_id.name} - **{f"{i[1]:,}"}**') 
        embed.description='\n'.join(top[:25])+"\nIn order for a server's run to be eligible for the leaderboard, no city (including repeat exceptions) can be said within 50 cities of itself."
        await interaction.followup.send(embed=embed,view=Paginator(1,top,embed.title,math.ceil(len(top)/25),interaction.user.id,embed),ephemeral=eph)
    else:
        embed.description='```null```'+"\nIn order for a server's run to be eligible for the leaderboard, no city (including repeat exceptions) can be said within 50 cities of itself."
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays global user leaderboard.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def ulb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title=f"GLOBAL USER LEADERBOARD",color=GREEN)
    cur.execute('''select user_id,score from global_user_info order by score desc''',data=(interaction.guild_id,))
    if cur.rowcount>0:
        fmt = [f'{n+1}. <@{i[0]}> - **{f"{i[1]:,}"}**' for n,i in enumerate(cur)]
        embed.description='\n'.join(fmt[:25])
        await interaction.followup.send(embed=embed,view=Paginator(1,fmt,embed.title,math.ceil(len(fmt)/25),interaction.user.id,embed),ephemeral=eph)
    else:
        embed.description='```null```'
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays all cities and their reactions.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def react(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title='Cities With Reactions',color=GREEN)
    cur.execute('''select city_id,reaction from react_info where server_id = ?''', data=(interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[]
        for (i,r) in cur:
            j=allnames.loc[(i)]
            fmt.append(f"- {city_string(j['name'],admin1name(j['country'],j['admin1']),admin2name(j['country'],j['admin1'],j['admin2']),j['country'],j['alt-country'])} - {r}")
        fmt=sorted(fmt)
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Cities With Reactions",math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
        view.message=await interaction.original_response()
    else:
        embed.description="```null```"
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays all cities that can be repeated.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def repeat(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title='Repeats Rule Exceptions',color=GREEN)
    cur.execute('''select city_id from repeat_info where server_id = ?''', data=(interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[]
        for (i,) in cur:
            j=allnames.loc[(i)]
            fmt.append(f"- {city_string(j['name'],admin1name(j['country'],j['admin1']),admin2name(j['country'],j['admin1'],j['admin2']),j['country'],j['alt-country'])}")
        fmt=sorted(fmt)
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Repeats Rule Exceptions",math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
        view.message=await interaction.original_response()
    else:
        embed.description="```null```"
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='popular-cities',description="Displays most popular cities and countries added to chain.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def popular(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    cur.execute('''select distinct city_id,country_code,alt_country from count_info where server_id = ? order by count desc''',data=(interaction.guild_id,))
    cities=[i for i in cur]
    cur.execute('''select city_id from repeat_info where server_id = ?''', data=(interaction.guild_id,))
    repeated={i[0] for i in cur}
    embed=discord.Embed(title="Popular Cities/Countries",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    if len(cities)>0:
        fmt=[]
        for i in cities[:10]:
            cur.execute('''select count from count_info where server_id = ? and city_id = ?''',data=(interaction.guild_id,i[0]))
            c=cur.fetchone()[0]
            j=allnames.loc[(i[0])]
            fmt.append((c,city_string(j['name'],admin1name(j['country'],j['admin1']),admin2name(j['country'],j['admin1'],j['admin2']),i[1],i[2])+(f'{":no_entry:" if j["deleted"] else ""}{":repeat:" if i[0] in repeated else ""}')))
        fmt=sorted(fmt,key = lambda x:(-x[0],x[1]))
        embed.add_field(name='Cities',value='\n'.join(['%s. %s - **%s**' %(n+1,i[1],f"{i[0]:,}") for (n,i) in enumerate(fmt)]))
        fmt=[]
        cur.execute('''select distinct country_code from count_info where server_id = ?''',data=(interaction.guild_id,))
        countrylist = {i[0] for i in cur}
        cur.execute('''select distinct alt_country from count_info where server_id = ? and alt_country is not null''',data=(interaction.guild_id,))
        countrylist.update({i[0] for i in cur})
        countries={}
        for i in countrylist:
            cur.execute('''select sum(count) from count_info where server_id = ? and (country_code = ? or alt_country = ?)''',data= (interaction.guild_id, i,i))
            if cur.rowcount!=0:
                countries[i] = cur.fetchone()[0]
        for i in countries:
            fmt.append((int(countries[i]),iso2[i],flags[i]))
        fmt=sorted(fmt,key = lambda x:(-x[0],x[1]))[:10]
        embed.add_field(name='Countries',value='\n'.join(['%s. %s %s - **%s**' %(n+1,i[1],i[2],f"{i[0]:,}") for (n,i) in enumerate(fmt)]))
    else:
        embed.add_field(name='Cities',value='```null```')
        embed.add_field(name='Countries',value='```null```')
    await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='best-rounds',description="Displays longest chains in server.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def bestrds(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    cur.execute('''select round_number,chain_end from server_info where server_id = ?''',data=(interaction.guild_id,))
    bb=cur.fetchone()
    rounds=range(1,bb[0]+1)
    embed=discord.Embed(title="Best Rounds",color=GREEN)
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    if len(rounds)>0:
        fmt=[]
        maxrounds=[]
        if not bb[1]:
            cur.execute('''select count(*) from chain_info where server_id = ? and round_number = ?''',data=(interaction.guild_id,bb[0]))
            maxrounds.append((cur.fetchone()[0],bb[0],"**Ongoing**"))
        cur.execute('''select distinct count,round_number,time_placed from chain_info where server_id = ? and valid = ?''',data=(interaction.guild_id,False))
        maxrounds.extend([(i-1,j,k) for (i,j,k) in cur.fetchall()])
        maxrounds=sorted(maxrounds,reverse=1)[:5]
        for i in maxrounds:
            maxc=i[0]
            cur.execute('''select distinct user_id from chain_info where server_id = ? and round_number = ?  and valid = ? and user_id is not null''',data=(interaction.guild_id,i[1],True))
            part=cur.rowcount
            if maxc>1:
                cur.execute('''select city_id,name,admin2,admin1,country_code,alt_country,time_placed from chain_info where server_id = ? and round_number = ? and count = ?''',data=(interaction.guild_id,i[1],1))
                b1=cur.fetchone()
                cur.execute('''select city_id,name,admin2,admin1,country_code,alt_country from chain_info where server_id = ? and round_number = ? and count = ?''',data=(interaction.guild_id,i[1],maxc))
                b2=cur.fetchone()
                b=[]
                for j in (b1,b2):
                    o=allnames.loc[(j[0])]
                    b.append(city_string(j[1]+(f" ({o['name']})" if o['name']!=j[1] else ""),j[3],j[2],j[4],j[5]))
                fmt.append((maxc,i[1],part,tuple(b),("<t:%s:f>"%b1[6],i[2] if type(i[2])==str else "<t:%s:f>"%i[2])))
            elif maxc==1:
                cur.execute('''select city_id,name,admin2,admin1,country_code,alt_country,time_placed from chain_info where server_id = ? and round_number = ? and count = ?''',data=(interaction.guild_id,i[1],1))
                j=cur.fetchone()
                o=allnames.loc[(j[0])]
                fmt.append((maxc,i[1],part,(city_string(j[1]+(f" ({o['name']})" if o['name']!=j[1] else ""),j[3],j[2],j[4],j[5]),),("<t:%s:f>"%j[6],i[2] if type(i[2])==str else "<t:%s:f>"%i[2])))
            else:
                fmt.append((0,i[1],1,("None","None"),(i[2] if type(i[2])==str else "<t:%s:f>"%i[2],i[2] if type(i[2])==str else "<t:%s:f>"%i[2])))
        for i in fmt:
            if i[0]>1:
                embed.add_field(name='%s - %s'%i[3],value='Length: %s\nRound: %s\nParticipants: %s\nStarted: %s\nEnded: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}',i[4][0],i[4][1]))
            elif i[0]==1:
                embed.add_field(name='%s'%i[3][0],value='Length: %s\nRound: %s\nParticipants: %s\nStarted: %s\nEnded: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}',i[4][0],i[4][1]))
            else:
                embed.add_field(name='None',value='Length: %s\nRound: %s\nParticipants: %s\nStarted: %s\nEnded: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}',i[4][0],i[4][1]))
    else:
        embed.add_field(name='',value='```null```')
    await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='blocked-users',description="Point and laugh.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
async def blocked(interaction:discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    cur.execute('select user_id from server_user_info where blocked=? and server_id=?',data=(True,interaction.guild_id))
    blocks={i[0] for i in cur}
    cur.execute('select user_id from global_user_info where blocked=?',(True,))
    blocks.update({i for (i,) in cur})
    members={i.id for i in interaction.guild.members}
    blocks=blocks.intersection(members)
    embed=discord.Embed(title='Blocked Users',color=GREEN)
    cur.execute('''select city_id from repeat_info where server_id = ?''', data=(interaction.guild_id,))
    if len(blocks)>0:
        fmt=[f"- <@{i}>" for i in blocks]
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Blocked Users",math.ceil(len(fmt)/25),interaction.user.id,embed)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
        view.message=await interaction.original_response()
    else:
        embed.description="```null```"
        await interaction.followup.send(embed=embed,ephemeral=eph)

@tree.command(name='city-info',description='Gets information for a given city.')
@app_commands.describe(city="The city to get information for",province="State, province, etc that the city is located in",country="Country the city is located in",include_deletes='Whether to search for cities that have been removed from the database or not',se='Yes to show everyone stats, no otherwise')
@app_commands.rename(province='administrative-division',include_deletes='include-deletes',se='show-everyone')
@app_commands.autocomplete(country=countrycomplete)
async def cityinfo(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None,include_deletes:Optional[Literal['yes','no']]='no',se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    cur.execute('select min_pop from server_info where server_id=?',data=(interaction.guild_id,))
    minimum_population = cur.fetchone()[0]
    res=search_cities(city,province,country,minimum_population,(include_deletes=='yes'))
    if res:
        cur.execute("select count from count_info where server_id=? and city_id=?",data=(interaction.guild_id,res[0]))
        if cur.rowcount:
            count=cur.fetchone()[0]
        else:
            count=0
        cur.execute('''select * from repeat_info where server_id = ? and city_id=?''', data=(interaction.guild_id,res[0]))
        repeatable=cur.rowcount
        aname=citydata[(citydata['geonameid']==res[0])]
        default=aname[aname['default']==1].iloc[0]
        dname=default['name']
        embed=discord.Embed(title='Information - %s'%dname,color = GREEN if not default['deleted'] else RED)
        embed.add_field(name='Geonames ID',value=res[0],inline=True)
        embed.add_field(name='Count',value=f"{count:,} {':repeat:' if repeatable else ''}",inline=True)
        embed.add_field(name='Name',value=dname,inline=True)
        if default['deleted']:
            embed.set_footer(text='This city has been removed from Geonames.')
        alts=aname[(aname['default']==0)]['name']
        secondEmbed=False
        if alts.shape[0]!=0:
            joinednames='`'+'`,`'.join(alts)+'`'
            if len(joinednames)<=1024:
                embed.add_field(name='Alternate Names',value=joinednames,inline=False)
            else:
                secondEmbed=True
                embed2=discord.Embed(title='Alternate Names - %s'%dname,color = GREEN if not default['deleted'] else RED,description=joinednames)
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
        if secondEmbed:
            await interaction.followup.send(embeds=[embed,embed2],ephemeral=eph)
        else:
            await interaction.followup.send(embed=embed,ephemeral=eph)
    else:
        await interaction.followup.send('City not recognized. Please try again. ',ephemeral=eph)

@tree.command(name='country-info',description='Gets information for a given country.')
@app_commands.describe(country="The country to get information for",se='Yes to show everyone stats, no otherwise')
@app_commands.rename(se='show-everyone')
@app_commands.autocomplete(country=countrycomplete)
async def countryinfo(interaction: discord.Interaction, country:str,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
    countrysearch=country.lower().strip()
    res=countriesdata[((countriesdata['name'].str.lower()==countrysearch)|(countriesdata['country'].str.lower()==countrysearch))]
    if res.shape[0]!=0:
        res = res.iloc[0]
        cur.execute("select sum(count) from count_info where server_id=? and (country_code=? or alt_country=?)",data=(interaction.guild_id,res['country'],res['country']))
        count=cur.fetchone()[0]
        aname=countriesdata[(countriesdata['geonameid']==res['geonameid'])]
        default=aname[aname['default']==1].iloc[0]
        dname=default['name']
        embed=discord.Embed(title='Information - %s %s (%s) - Count: %s'%(flags[res['country']],dname,res['country'],f"{count:,}" if count else 0),color=GREEN)
        alts=aname[(aname['default']==0)]['name']
        if alts.shape[0]!=0:
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
            topcities=discord.Embed(title=f'''Popular Cities - {flags[res['country']]} {dname} ({res['country']})''',color=GREEN)
            cur.execute('''select name,admin2,admin1,country_code,alt_country,count,city_id from count_info where server_id=? and (country_code=? or alt_country=?) order by count desc limit 10''',data=(interaction.guild_id,res['country'],res['country']))
            if cur.rowcount>0:
                citylist=[]
                for n,i in enumerate(cur):
                    deleted = allnames.loc[i[6],'deleted']
                    citylist.append(f'''{n+1}. {city_string(i[0],i[2],i[1],i[3],i[4])}{':no_entry:' if deleted else ''} - **{i[5]:,}**''')
                topcities.description='\n'.join(citylist)
                tosend.append(topcities)
            embed_sizes = [sum([len(j) for j in (i.title if i.title else '',i.description)]) for i in tosend]
            # while over limit send embeds individually
            while sum(embed_sizes)>6000:
                await interaction.followup.send(embed=tosend[0],ephemeral=eph)
                embed_sizes=embed_sizes[1:]      
                tosend=tosend[1:] 
            await interaction.followup.send(embeds=tosend,ephemeral=eph)      
        else:
            embed.description='There are no alternate names for this country.'
            await interaction.followup.send(embed=embed,ephemeral=eph)
    else:
        await interaction.followup.send('Country not recognized. Please try again. ',ephemeral=eph)

@tree.command(name='delete-stats',description='Deletes server stats.')
@app_commands.check(owner_modcheck)
async def deletestats(interaction: discord.Interaction):
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
        return
    embed=discord.Embed(color=RED,title='Are you sure?',description='This action is irreversible.')
    view=Confirmation(interaction.guild_id,interaction.user.id)
    await interaction.response.send_message(embed=embed,view=view)
    view.message=await interaction.original_response()

@tree.command(description="Tests the client's latency. ")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message('Pong! `%s ms`'%(client.latency*1000))

@tree.command(name="block-server",description="Blocks a user from using the bot in the server. ")
@app_commands.check(owner_modcheck)
async def serverblock(interaction: discord.Interaction,member: discord.Member):
    if member!=owner and not member.bot:
        if is_blocked(interaction.user.id,interaction.guild_id):
            await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
            return
        cur.execute('''update server_user_info set blocked=? where user_id=? and server_id=?''',data=(True,member.id,interaction.guild_id))
        conn.commit()
        await interaction.response.send_message(f"<@{member.id}> has been blocked from using this bot in the server. ")
    else:
        await interaction.response.send_message(f"Nice try, bozo")

@tree.command(name="unblock-server",description="Unblocks a user from using the bot in the server. ")
@app_commands.check(owner_modcheck)
async def serverunblock(interaction: discord.Interaction,member: discord.Member):
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute('''select blocked from global_user_info where user_id=?''',(member.id,))
    if cur.fetchone()[0]:
        await interaction.response.send_message(f":no_entry: <@{member.id}> cannot be unblocked. ")
    else:
        cur.execute('''update server_user_info set blocked=? where user_id=? and server_id=?''',data=(False,member.id,interaction.guild_id))
        conn.commit()
        await interaction.response.send_message(f"<@{member.id}> has been unblocked from using this bot in the server. ")

from discord.ext import commands

@tree.command(name="block-global",description="Blocks a user from using the bot. ")
@commands.is_owner()
async def globalblock(interaction: discord.Interaction,user: discord.User):
    if user!=owner and not user.bot:
        if is_blocked(interaction.user.id,interaction.guild_id):
            await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
            return
        cur.execute('''update global_user_info set blocked=? where user_id=?''',data=(True,user.id))
        conn.commit()
        await interaction.response.send_message(f"<@{user.id}> has been blocked from using this bot in the server. ")
    else:
        await interaction.response.send_message(f"Nice try, bozo")

@tree.command(name="unblock-global",description="Unblocks a user from using the bot. ")
@commands.is_owner()
async def globalunblock(interaction: discord.Interaction,user: discord.User):
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ")
        return
    cur.execute('''update global_user_info set blocked=? where user_id=?''',data=(False,user.id))
    conn.commit()
    await interaction.response.send_message(f"<@{user.id}> has been unblocked from using this bot. ")

@tree.command(description="Gets information about the bot and the game. ")
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
@app_commands.rename(se='show-everyone')
async def help(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    await interaction.response.defer(ephemeral=eph)
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
                command_messages[index].append(f'''`/{i.name} {j.name} {''.join([f'[{k.display_name}]' for k in j.parameters if k.required])}{('(' if len(optional_params) else '')+''.join([f'[{k}]' for k in optional_params])+(')' if len(optional_params) else '')}`: {j.description}''')
        else:
            optional_params = [j.display_name for j in i.parameters if not j.required]
            command_messages[3].append(f'''`/{i.name} {''.join([f'[{j.display_name}]' for j in i.parameters if j.required])}{('(' if len(optional_params) else '')+''.join([f'[{j}]' for j in optional_params])+(')' if len(optional_params) else '')}`: {i.description}''')
    headers = ['**Set Commands:**','**Reaction/Repeat Commands:**','**Stats Commands:**','**Other Commands:**']
    for n in range(len(command_messages)):
        command_messages[n]=headers[n]+'\n\n'+'\n'.join(command_messages[n])
    
    embed.description="Choose a topic."
    await interaction.followup.send(embed=embed,view=Help(['''1. Find a channel that you want to use the bot in, and use `/set channel` to designate it as such.\n2. Using the `/set` commands listed in the Commands section of this help page, change around settings to your liking.\n3. Happy playing!''','''One person chooses a city, then the next person must choose a city beginning with the last letter of the previous city. \nExamples: \nLondo**n** --> **N**ew Yor**k** --> **K**arachi\nLondo**n** --> **N**anjin**g** --> **G**uadalajara\n\nThe chain can be broken in a few different ways, such as the next city not starting with the last letter, the next city not existing, the next city having too small of a population, the next user going twice, or putting down a city that has been said before. The settings for some of those can be changed, but here are some examples:\n**User A**: New Haven\n**User B**: London (city starts with wrong letter)\n\n**User A**: New Haven\n**User A**: Nagoya (user went twice in a row)\n\n**User A**: New Haven\n**User B**: New Kent (city's population is below 1000, if 1000 is the minimum population set)\n\n**User A**: New Haven\n**User B**: New Haven (same city repeated)\n\n**User A**: New Haven\n**User B**: Never Gonna Give You Up (city does not exist)\n\nCities must be prefixed by the server's given prefix in order to be counted towards the chain (by default it's **!**, but this can be changed).\n\nDo note that cities with alternate names will start and end with the first and last letters of those names, but will be counted as the same city. \nFor example, The Hague starts with `t` and ends with `e`, while Den Haag starts with `d` and ends with `g`, but both are considered the same city. \nRuining the chain deliberately is considered a bannable offense, as is creating alternate accounts to sidestep bans.''',None,''':white_check_mark: - valid addition to chain\n:ballot_box_with_check: - valid addition to chain, breaks server record\n:x: - invalid addition to chain\n:regional_indicator_a: - letter the city ends with\n\nIn addition, you can make the bot react to certain cities of your choosing using the `/add react` and `remove react` commands.''','''- When many people are playing, play cities that start and end with the same letter to avoid breaking the chain. \n- If you want to specify a different city than the one the bot interprets, you can use commas to specify provinces, states, or countries: \nExamples: \n:white_check_mark: Atlanta\n:white_check_mark: Atlanta, United States\n:white_check_mark: Atlanta, Georgia\n:white_check_mark: Atlanta, Fulton County\n:white_check_mark: Atlanta, Fulton County, Georgia, United States\nYou can specify a maximum of 2 administrative divisions, not including the country. \n- Googling cities is allowed. \n- Remember, at the end of the day, it is just a game, and the game is supposed to be lighthearted and fun.''','''**Q: Some cities aren't recognized by the bot. Why?**\nA: The bot takes its data from Geonames, and only cities with a listed population (that is, greater than 0 people listed) are considered by the bot.\n\n**Q: I added some cities to Geonames, but they still aren't recognized by the bot. Why?**\nA: The Geonames dump updates the cities list daily, but the bot's record of cities is not updated on a regular basis, so it might take until I decide to update it again for those cities to show up.\n\n**Q: Why are some of the romanizations for cities incorrect?**\nA: That's a thing to do with the Python library I use to romanize the characters (`unidecode`) - characters are romanized one-by-one instead of with context. For example, `unidecode` will turn `广州` into `Yan Zhou`. I still haven't found a good way to match every foreign name to a perfect translation. \n\n**Q: How do I suggest feedback to the bot?**\nA: There is a support server and support channels listed in the `/about` command for this bot.'''],command_messages,interaction.user.id),ephemeral=(se=='no'))

@tree.command(description="Information about the bot.")
@app_commands.describe(se='Yes to show everyone stats, no otherwise')
@app_commands.rename(se='show-everyone')
async def about(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=(se=='no')
    if is_blocked(interaction.user.id,interaction.guild_id):
        await interaction.response.send_message(":no_pedestrians: You are blocked from using this bot. ",ephemeral=eph)
        return
    embed = discord.Embed(color=GREEN,title="About Cities Chain Bot")
    embed.add_field(name="Ping",value=f"{int(client.latency*1000)} ms")
    embed.add_field(name="Support Server",value="[here](https://discord.gg/xTERJGpx5d)")
    embed.add_field(name="Suggestions Channel",value="<#1231870769454125129>\n<#1221861912657006612>")
    embed.add_field(name="Data Source",value="[Geonames](https://geonames.org) - [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)")
    embed.add_field(name="GitHub Repository",value="[here](https://github.com/GlutenFreeGrapes/cities-chain)")
    await interaction.response.send_message(embed=embed,ephemeral=eph)

import traceback,datetime
@client.event
async def on_error(event, *args, **kwargs):
    embed = discord.Embed(title=':x: Event Error', colour=0xe74c3c)
    embed.add_field(name='Event', value=event)
    embed.description = '```\n%s\n```' % traceback.format_exc()
    embed.timestamp = datetime.datetime.now()
    app_info = await client.application_info()
    owner = await client.fetch_user(app_info.team.owner_id)
    await owner.send(embed=embed)


tree.add_command(assign)
tree.add_command(add)
tree.add_command(remove)
tree.add_command(stats)

client.run(env["DISCORD_TOKEN"], reconnect=1)