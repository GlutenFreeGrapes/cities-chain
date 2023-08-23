import discord,re,pandas as pd,random,math,mariadb,numpy as np
from discord import app_commands
from typing import Optional,Literal
import asyncio
from os import environ as env
from dotenv import load_dotenv
load_dotenv()

intents = discord.Intents.default()
intents.message_content = True

citydata,countriesdata,admin1data,admin2data=pd.read_csv('cities.txt',sep='\t',keep_default_na=False,na_values='',dtype={'admin1':str,'admin2':str,'alt-country':str}),pd.read_csv('countries.txt',sep='\t',keep_default_na=False,na_values=''),pd.read_csv('admin1.txt',sep='\t',keep_default_na=False,na_values='',dtype={'admin1':str}),pd.read_csv('admin2.txt',sep='\t',keep_default_na=False,na_values='',dtype={'admin1':str,'admin2':str,})
citydata=citydata.fillna(np.nan).replace([np.nan], [None])
countriesdata=countriesdata.fillna(np.nan).replace([np.nan], [None])
admin1data=admin1data.fillna(np.nan).replace([np.nan], [None])
admin2data=admin2data.fillna(np.nan).replace([np.nan], [None])

env.setdefault("DB_NAME", "cities_chain")
conn = mariadb.connect(
    user=env["DB_USER"],
    password=env["DB_PASSWORD"],
    host=env["DB_HOST"],
    database=None)
cur = conn.cursor() 

# cur.execute('drop database ' + env["DB_NAME"])

cur.execute('create database if not exists ' + env["DB_NAME"])
cur.execute('use ' + env["DB_NAME"])

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
                references server_info(server_id),
            foreign key(user_id)
                references global_user_info(user_id))''')

cur.execute('''create table if not exists chain_info(
            server_id bigint, 
            user_id bigint,
            round_number int, 
            count int, 
            city_id int default -1,
            name varchar(200),
            admin1 varchar(200),
            country varchar(100),
            country_code char(2),
            alt_country varchar(100),
            time_placed int,
            valid bool,
            primary key(server_id,city_id,round_number,count),
            foreign key(server_id)
                references server_info(server_id),
            foreign key(user_id)
                references global_user_info(user_id))''')

client = discord.Client(intents=intents)
tree=app_commands.tree.CommandTree(client)

allnames=citydata[citydata['default']==1]
allnames=allnames.set_index('geonameid')[['name','population']]

allcountries=['Afghanistan', 'Aland Islands', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antarctica', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bermuda', 'Bhutan', 'Bolivia', 'Bonaire, Saint Eustatius and Saba ', 'Bosnia and Herzegovina', 'Botswana', 'Bouvet Island', 'Brazil', 'British Indian Ocean Territory', 'British Virgin Islands', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada', 'Cayman Islands', 'Central African Republic', 'Chad', 'Chile', 'China', 'Christmas Island', 'Cocos Islands', 'Colombia', 'Comoros', 'Cook Islands', 'Costa Rica', 'Croatia', 'Cuba', 'Curacao', 'Cyprus', 'Czechia', 'Democratic Republic of the Congo', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Falkland Islands', 'Faroe Islands', 'Fiji', 'Finland', 'France', 'French Guiana', 'French Polynesia', 'French Southern Territories', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Gibraltar', 'Greece', 'Greenland', 'Grenada', 'Guadeloupe', 'Guam', 'Guatemala', 'Guernsey', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Heard Island and McDonald Islands', 'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Isle of Man', 'Israel', 'Italy', 'Ivory Coast', 'Jamaica', 'Japan', 'Jersey', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', 'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macao', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Martinique', 'Mauritania', 'Mauritius', 'Mayotte', 'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia', 'Montenegro', 'Montserrat', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'Netherlands Antilles', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'Niue', 'Norfolk Island', 'North Korea', 'North Macedonia', 'Northern Mariana Islands', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Palestinian Territory', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Pitcairn', 'Poland', 'Portugal', 'Puerto Rico', 'Qatar', 'Republic of the Congo', 'Reunion', 'Romania', 'Russia', 'Rwanda', 'Saint Barthelemy', 'Saint Helena', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin', 'Saint Pierre and Miquelon', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Serbia and Montenegro', 'Seychelles', 'Sierra Leone', 'Singapore', 'Sint Maarten', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Georgia and the South Sandwich Islands', 'South Korea', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Svalbard and Jan Mayen', 'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Timor Leste', 'Togo', 'Tokelau', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Turks and Caicos Islands', 'Tuvalu', 'U.S. Virgin Islands', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'United States Minor Outlying Islands', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Vatican', 'Venezuela', 'Vietnam', 'Wallis and Futuna', 'Western Sahara', 'Yemen', 'Zambia', 'Zimbabwe']
iso2={'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan', 'AG': 'Antigua and Barbuda', 'AI': 'Anguilla', 'AL': 'Albania', 'AM': 'Armenia', 'AO': 'Angola', 'AQ': 'Antarctica', 'AR': 'Argentina', 'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba', 'AX': 'Aland Islands', 'AZ': 'Azerbaijan', 'BA': 'Bosnia and Herzegovina', 'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium', 'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi', 'BJ': 'Benin', 'BL': 'Saint Barthelemy', 'BM': 'Bermuda', 'BN': 'Brunei', 'BO': 'Bolivia', 'BQ': 'Bonaire, Saint Eustatius and Saba ', 'BR': 'Brazil', 'BS': 'Bahamas', 'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana', 'BY': 'Belarus', 'BZ': 'Belize', 'CA': 'Canada', 'CC': 'Cocos Islands', 'CD': 'Democratic Republic of the Congo', 'CF': 'Central African Republic', 'CG': 'Republic of the Congo', 'CH': 'Switzerland', 'CI': 'Ivory Coast', 'CK': 'Cook Islands', 'CL': 'Chile', 'CM': 'Cameroon', 'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CU': 'Cuba', 'CV': 'Cabo Verde', 'CW': 'Curacao', 'CX': 'Christmas Island', 'CY': 'Cyprus', 'CZ': 'Czechia', 'DE': 'Germany', 'DJ': 'Djibouti', 'DK': 'Denmark', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria', 'EC': 'Ecuador', 'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia', 'FI': 'Finland', 'FJ': 'Fiji', 'FK': 'Falkland Islands', 'FM': 'Micronesia', 'FO': 'Faroe Islands', 'FR': 'France', 'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia', 'GF': 'French Guiana', 'GG': 'Guernsey', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GL': 'Greenland', 'GM': 'Gambia', 'GN': 'Guinea', 'GP': 'Guadeloupe', 'GQ': 'Equatorial Guinea', 'GR': 'Greece', 'GS': 'South Georgia and the South Sandwich Islands', 'GT': 'Guatemala', 'GU': 'Guam', 'GW': 'Guinea-Bissau', 'GY': 'Guyana', 'HK': 'Hong Kong', 'HM': 'Heard Island and McDonald Islands', 'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary', 'ID': 'Indonesia', 'IE': 'Ireland', 'IL': 'Israel', 'IM': 'Isle of Man', 'IN': 'India', 'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy', 'JE': 'Jersey', 'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan', 'KE': 'Kenya', 'KG': 'Kyrgyzstan', 'KH': 'Cambodia', 'KI': 'Kiribati', 'KM': 'Comoros', 'KN': 'Saint Kitts and Nevis', 'KP': 'North Korea', 'KR': 'South Korea', 'XK': 'Kosovo', 'KW': 'Kuwait', 'KY': 'Cayman Islands', 'KZ': 'Kazakhstan', 'LA': 'Laos', 'LB': 'Lebanon', 'LC': 'Saint Lucia', 'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia', 'LY': 'Libya', 'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova', 'ME': 'Montenegro', 'MF': 'Saint Martin', 'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'North Macedonia', 'ML': 'Mali', 'MM': 'Myanmar', 'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands', 'MQ': 'Martinique', 'MR': 'Mauritania', 'MS': 'Montserrat', 'MT': 'Malta', 'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico', 'MY': 'Malaysia', 'MZ': 'Mozambique', 'NA': 'Namibia', 'NC': 'New Caledonia', 'NE': 'Niger', 'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua', 'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal', 'NR': 'Nauru', 'NU': 'Niue', 'NZ': 'New Zealand', 'OM': 'Oman', 'PA': 'Panama', 'PE': 'Peru', 'PF': 'French Polynesia', 'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan', 'PL': 'Poland', 'PM': 'Saint Pierre and Miquelon', 'PN': 'Pitcairn', 'PR': 'Puerto Rico', 'PS': 'Palestinian Territory', 'PT': 'Portugal', 'PW': 'Palau', 'PY': 'Paraguay', 'QA': 'Qatar', 'RE': 'Reunion', 'RO': 'Romania', 'RS': 'Serbia', 'RU': 'Russia', 'RW': 'Rwanda', 'SA': 'Saudi Arabia', 'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan', 'SS': 'South Sudan', 'SE': 'Sweden', 'SG': 'Singapore', 'SH': 'Saint Helena', 'SI': 'Slovenia', 'SJ': 'Svalbard and Jan Mayen', 'SK': 'Slovakia', 'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia', 'SR': 'Suriname', 'ST': 'Sao Tome and Principe', 'SV': 'El Salvador', 'SX': 'Sint Maarten', 'SY': 'Syria', 'SZ': 'Eswatini', 'TC': 'Turks and Caicos Islands', 'TD': 'Chad', 'TF': 'French Southern Territories', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TK': 'Tokelau', 'TL': 'Timor Leste', 'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TO': 'Tonga', 'TR': 'Turkey', 'TT': 'Trinidad and Tobago', 'TV': 'Tuvalu', 'TW': 'Taiwan', 'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'UM': 'United States Minor Outlying Islands', 'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VA': 'Vatican', 'VC': 'Saint Vincent and the Grenadines', 'VE': 'Venezuela', 'VG': 'British Virgin Islands', 'VI': 'U.S. Virgin Islands', 'VN': 'Vietnam', 'VU': 'Vanuatu', 'WF': 'Wallis and Futuna', 'WS': 'Samoa', 'YE': 'Yemen', 'YT': 'Mayotte', 'ZA': 'South Africa', 'ZM': 'Zambia', 'ZW': 'Zimbabwe', 'CS': 'Serbia and Montenegro', 'AN': 'Netherlands Antilles'}
iso3={'AND': 'Andorra', 'ARE': 'United Arab Emirates', 'AFG': 'Afghanistan', 'ATG': 'Antigua and Barbuda', 'AIA': 'Anguilla', 'ALB': 'Albania', 'ARM': 'Armenia', 'AGO': 'Angola', 'ATA': 'Antarctica', 'ARG': 'Argentina', 'ASM': 'American Samoa', 'AUT': 'Austria', 'AUS': 'Australia', 'ABW': 'Aruba', 'ALA': 'Aland Islands', 'AZE': 'Azerbaijan', 'BIH': 'Bosnia and Herzegovina', 'BRB': 'Barbados', 'BGD': 'Bangladesh', 'BEL': 'Belgium', 'BFA': 'Burkina Faso', 'BGR': 'Bulgaria', 'BHR': 'Bahrain', 'BDI': 'Burundi', 'BEN': 'Benin', 'BLM': 'Saint Barthelemy', 'BMU': 'Bermuda', 'BRN': 'Brunei', 'BOL': 'Bolivia', 'BES': 'Bonaire, Saint Eustatius and Saba ', 'BRA': 'Brazil', 'BHS': 'Bahamas', 'BTN': 'Bhutan', 'BVT': 'Bouvet Island', 'BWA': 'Botswana', 'BLR': 'Belarus', 'BLZ': 'Belize', 'CAN': 'Canada', 'CCK': 'Cocos Islands', 'COD': 'Democratic Republic of the Congo', 'CAF': 'Central African Republic', 'COG': 'Republic of the Congo', 'CHE': 'Switzerland', 'CIV': 'Ivory Coast', 'COK': 'Cook Islands', 'CHL': 'Chile', 'CMR': 'Cameroon', 'CHN': 'China', 'COL': 'Colombia', 'CRI': 'Costa Rica', 'CUB': 'Cuba', 'CPV': 'Cabo Verde', 'CUW': 'Curacao', 'CXR': 'Christmas Island', 'CYP': 'Cyprus', 'CZE': 'Czechia', 'DEU': 'Germany', 'DJI': 'Djibouti', 'DNK': 'Denmark', 'DMA': 'Dominica', 'DOM': 'Dominican Republic', 'DZA': 'Algeria', 'ECU': 'Ecuador', 'EST': 'Estonia', 'EGY': 'Egypt', 'ESH': 'Western Sahara', 'ERI': 'Eritrea', 'ESP': 'Spain', 'ETH': 'Ethiopia', 'FIN': 'Finland', 'FJI': 'Fiji', 'FLK': 'Falkland Islands', 'FSM': 'Micronesia', 'FRO': 'Faroe Islands', 'FRA': 'France', 'GAB': 'Gabon', 'GBR': 'United Kingdom', 'GRD': 'Grenada', 'GEO': 'Georgia', 'GUF': 'French Guiana', 'GGY': 'Guernsey', 'GHA': 'Ghana', 'GIB': 'Gibraltar', 'GRL': 'Greenland', 'GMB': 'Gambia', 'GIN': 'Guinea', 'GLP': 'Guadeloupe', 'GNQ': 'Equatorial Guinea', 'GRC': 'Greece', 'SGS': 'South Georgia and the South Sandwich Islands', 'GTM': 'Guatemala', 'GUM': 'Guam', 'GNB': 'Guinea-Bissau', 'GUY': 'Guyana', 'HKG': 'Hong Kong', 'HMD': 'Heard Island and McDonald Islands', 'HND': 'Honduras', 'HRV': 'Croatia', 'HTI': 'Haiti', 'HUN': 'Hungary', 'IDN': 'Indonesia', 'IRL': 'Ireland', 'ISR': 'Israel', 'IMN': 'Isle of Man', 'IND': 'India', 'IOT': 'British Indian Ocean Territory', 'IRQ': 'Iraq', 'IRN': 'Iran', 'ISL': 'Iceland', 'ITA': 'Italy', 'JEY': 'Jersey', 'JAM': 'Jamaica', 'JOR': 'Jordan', 'JPN': 'Japan', 'KEN': 'Kenya', 'KGZ': 'Kyrgyzstan', 'KHM': 'Cambodia', 'KIR': 'Kiribati', 'COM': 'Comoros', 'KNA': 'Saint Kitts and Nevis', 'PRK': 'North Korea', 'KOR': 'South Korea', 'XKX': 'Kosovo', 'KWT': 'Kuwait', 'CYM': 'Cayman Islands', 'KAZ': 'Kazakhstan', 'LAO': 'Laos', 'LBN': 'Lebanon', 'LCA': 'Saint Lucia', 'LIE': 'Liechtenstein', 'LKA': 'Sri Lanka', 'LBR': 'Liberia', 'LSO': 'Lesotho', 'LTU': 'Lithuania', 'LUX': 'Luxembourg', 'LVA': 'Latvia', 'LBY': 'Libya', 'MAR': 'Morocco', 'MCO': 'Monaco', 'MDA': 'Moldova', 'MNE': 'Montenegro', 'MAF': 'Saint Martin', 'MDG': 'Madagascar', 'MHL': 'Marshall Islands', 'MKD': 'North Macedonia', 'MLI': 'Mali', 'MMR': 'Myanmar', 'MNG': 'Mongolia', 'MAC': 'Macao', 'MNP': 'Northern Mariana Islands', 'MTQ': 'Martinique', 'MRT': 'Mauritania', 'MSR': 'Montserrat', 'MLT': 'Malta', 'MUS': 'Mauritius', 'MDV': 'Maldives', 'MWI': 'Malawi', 'MEX': 'Mexico', 'MYS': 'Malaysia', 'MOZ': 'Mozambique', 'NAM': 'Namibia', 'NCL': 'New Caledonia', 'NER': 'Niger', 'NFK': 'Norfolk Island', 'NGA': 'Nigeria', 'NIC': 'Nicaragua', 'NLD': 'Netherlands', 'NOR': 'Norway', 'NPL': 'Nepal', 'NRU': 'Nauru', 'NIU': 'Niue', 'NZL': 'New Zealand', 'OMN': 'Oman', 'PAN': 'Panama', 'PER': 'Peru', 'PYF': 'French Polynesia', 'PNG': 'Papua New Guinea', 'PHL': 'Philippines', 'PAK': 'Pakistan', 'POL': 'Poland', 'SPM': 'Saint Pierre and Miquelon', 'PCN': 'Pitcairn', 'PRI': 'Puerto Rico', 'PSE': 'Palestinian Territory', 'PRT': 'Portugal', 'PLW': 'Palau', 'PRY': 'Paraguay', 'QAT': 'Qatar', 'REU': 'Reunion', 'ROU': 'Romania', 'SRB': 'Serbia', 'RUS': 'Russia', 'RWA': 'Rwanda', 'SAU': 'Saudi Arabia', 'SLB': 'Solomon Islands', 'SYC': 'Seychelles', 'SDN': 'Sudan', 'SSD': 'South Sudan', 'SWE': 'Sweden', 'SGP': 'Singapore', 'SHN': 'Saint Helena', 'SVN': 'Slovenia', 'SJM': 'Svalbard and Jan Mayen', 'SVK': 'Slovakia', 'SLE': 'Sierra Leone', 'SMR': 'San Marino', 'SEN': 'Senegal', 'SOM': 'Somalia', 'SUR': 'Suriname', 'STP': 'Sao Tome and Principe', 'SLV': 'El Salvador', 'SXM': 'Sint Maarten', 'SYR': 'Syria', 'SWZ': 'Eswatini', 'TCA': 'Turks and Caicos Islands', 'TCD': 'Chad', 'ATF': 'French Southern Territories', 'TGO': 'Togo', 'THA': 'Thailand', 'TJK': 'Tajikistan', 'TKL': 'Tokelau', 'TLS': 'Timor Leste', 'TKM': 'Turkmenistan', 'TUN': 'Tunisia', 'TON': 'Tonga', 'TUR': 'Turkey', 'TTO': 'Trinidad and Tobago', 'TUV': 'Tuvalu', 'TWN': 'Taiwan', 'TZA': 'Tanzania', 'UKR': 'Ukraine', 'UGA': 'Uganda', 'UMI': 'United States Minor Outlying Islands', 'USA': 'United States', 'URY': 'Uruguay', 'UZB': 'Uzbekistan', 'VAT': 'Vatican', 'VCT': 'Saint Vincent and the Grenadines', 'VEN': 'Venezuela', 'VGB': 'British Virgin Islands', 'VIR': 'U.S. Virgin Islands', 'VNM': 'Vietnam', 'VUT': 'Vanuatu', 'WLF': 'Wallis and Futuna', 'WSM': 'Samoa', 'YEM': 'Yemen', 'MYT': 'Mayotte', 'ZAF': 'South Africa', 'ZMB': 'Zambia', 'ZWE': 'Zimbabwe', 'SCG': 'Serbia and Montenegro', 'ANT': 'Netherlands Antilles'}
regionalindicators={'a':'🇦','b':'🇧','c':'🇨','d':'🇩','e':'🇪','f':'🇫','g':'🇬','h':'🇭','i':'🇮','j':'🇯','k':'🇰','l':'🇱','m':'🇲','n':'🇳','o':'🇴','p':'🇵','q':'🇶','r':'🇷','s':'🇸','t':'🇹','u':'🇺','v':'🇻','w':'🇼','x':'🇽','y':'🇾','z':'🇿'}
acodes={'admin1 code','admin2 code'}
ccodes={'country code'}
anames={'admin1 names','admin2 names'}
cnames={'country names',"alternate countries",'alternate country names'}
def search_cities(city,province,country):
    city=re.sub(',$','',city.casefold().strip())
    if city[-1]==',':
        city=city[:-1]
    res1=citydata[(citydata['name'].str.casefold()==city)]
    res2=citydata[(citydata['decoded'].str.casefold()==city)]
    res3=citydata[(citydata['punct space'].str.casefold()==city)]
    res4=citydata[(citydata['punct empty'].str.casefold()==city)]
    results=res1[res1['default']==1]
    s='name'
    if results.shape[0]==0:
        results=res2[res2['default']==1]
        s='decoded'
        if results.shape[0]==0:
            results=res3[res3['default']==1]
            s='punct space'
            if results.shape[0]==0:
                results=res4[res4['default']==1]
                s='punct empty'
                if results.shape[0]==0:
                    results=res1[res1['default']==0]
                    s='name'
                    if results.shape[0]==0:
                        results=res2[res2['default']==0]
                        s='decoded'
                        if results.shape[0]==0:
                            results=res3[res3['default']==0]
                            s='punct space'
                            if results.shape[0]==0:
                                results=res4[res4['default']==0]
                                s='punct empty'
    if province:
        p=province.casefold().strip()
        a1choice=admin1data[(admin1data['name'].str.casefold()==p)|(admin1data['admin1'].str.casefold()==p)]
        a2choice=admin2data[(admin2data['name'].str.casefold()==p)|(admin2data['admin2'].str.casefold()==p)]
        a1choice=set(zip(a1choice['country'],a1choice['admin1']))
        a2choice=set(zip(a2choice['country'],a2choice['admin1'],a2choice['admin2']))
        rcol=results.columns
        a1results=pd.DataFrame(columns=rcol)
        for i in a1choice:
            a1results=pd.concat([a1results,results[(results['country']==i[0])&(results['admin1']==i[1])]])
        a2results=pd.DataFrame(columns=rcol)
        for i in a2choice:
            a2results=pd.concat([a2results,results[(results['country']==i[0])&(results['admin1']==i[1])&(results['admin2']==i[2])]])
        results=pd.concat([a1results,a2results]).drop_duplicates()
    if country:
        c=country.casefold().strip()
        cchoice=countriesdata[(countriesdata['name'].str.casefold()==c)|(countriesdata['country'].str.casefold()==c)]
        cchoice=set(cchoice['country'])
        results=results[results['country'].isin(cchoice)|results['alt-country'].isin(cchoice)]
    if len(results)==0:
        return None
    else:
        r=results.sort_values('population',ascending=0).head(1).iloc[0]
        return (int(r['geonameid']),r,r[s])

codes={'country code','admin1 code','admin2 code'}
names={'country names','admin1 names','admin2 names',"alternate countries",'alternate country names'}
def search_cities_chain(query):
    q=re.sub(',$','',query.casefold().strip())
    if q[-1]==',':
        q=q[:-1]
    p=re.sub('\s*,\s*',',',q).split(',')
    city=p[0]
    res1=citydata[(citydata['name'].str.casefold()==city)]
    res2=citydata[(citydata['decoded'].str.casefold()==city)]
    res3=citydata[(citydata['punct space'].str.casefold()==city)]
    res4=citydata[(citydata['punct empty'].str.casefold()==city)]
    results=res1[res1['default']==1]
    s='name'
    if results.shape[0]==0:
        results=res2[res2['default']==1]
        s='decoded'
        if results.shape[0]==0:
            results=res3[res3['default']==1]
            s='punct space'
            if results.shape[0]==0:
                results=res4[res4['default']==1]
                s='punct empty'
                if results.shape[0]==0:
                    results=res1[res1['default']==0]
                    s='name'
                    if results.shape[0]==0:
                        results=res2[res2['default']==0]
                        s='decoded'
                        if results.shape[0]==0:
                            results=res3[res3['default']==0]
                            s='punct space'
                            if results.shape[0]==0:
                                results=res4[res4['default']==0]
                                s='punct empty'
    if len(p)>1:
        otherdivision=p[-1]
        cchoice=countriesdata[(countriesdata['name'].str.casefold()==otherdivision)|(countriesdata['country'].str.casefold()==otherdivision)]
        a1choice=admin1data[(admin1data['name'].str.casefold()==otherdivision)|(admin1data['admin1'].str.casefold()==otherdivision)]
        a2choice=admin2data[(admin2data['name'].str.casefold()==otherdivision)|(admin2data['admin2'].str.casefold()==otherdivision)]
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
    if results.shape[0]==0:
        return None
    else:
        r=results.sort_values('population',ascending=0).head(1).iloc[0]
        return (int(r['geonameid']),r,r[s])

class Paginator(discord.ui.View):
    def __init__(self,page,blist,title,lens,user):
        super().__init__(timeout=180)
        self.page=page
        self.blist=blist
        self.title=title
        self.lens=lens
        self.author=user
        if lens==1:
            for i in self.children:
                i.disabled=True
            self.children[2].label="1/1"
        elif self.page==1:
            self.children[0].disabled=True
            self.children[1].disabled=True
            self.children[2].label="1/%s"%(self.lens)
            self.children[3].disabled=False
            self.children[4].disabled=False
        elif self.page==lens:
            self.children[0].disabled=False
            self.children[1].disabled=False
            self.children[2].label="%s/%s"%(self.lens,self.lens)
            self.children[3].disabled=True
            self.children[4].disabled=True
        else:
            for i in self.children:
                i.disabled=False
            self.children[2].label="%s/%s"%(self.page,self.lens)
    async def on_timeout(self):
        for i in self.children:
            i.disabled=True
        new=discord.Embed(title=self.title, color=discord.Colour.from_rgb(0,255,0),description='\n'.join(self.blist[self.page*25-25:self.page*25]))
        await self.message.edit(embed=new,view=None)
    async def updateembed(self,interaction:discord.Interaction):
        if self.lens==1:
            for i in self.children:
                i.disabled=True
            self.children[2].label="1/1"
        elif self.page==1:
            self.children[0].disabled=True
            self.children[1].disabled=True
            self.children[2].label="1/%s"%(self.lens)
            self.children[3].disabled=False
            self.children[4].disabled=False
        elif self.page==self.lens:
            self.children[0].disabled=False
            self.children[1].disabled=False
            self.children[2].label="%s/%s"%(self.lens,self.lens)
            self.children[3].disabled=True
            self.children[4].disabled=True
        else:
            for i in self.children:
                i.disabled=False
            self.children[2].label="%s/%s"%(self.page,self.lens)
        new=discord.Embed(title=self.title, color=discord.Colour.from_rgb(0,255,0),description='\n'.join(self.blist[self.page*25-25:self.page*25]))
        view=Paginator(self.page,self.blist,self.title,self.lens,self.author)
        await interaction.response.edit_message(embed=new,view=view)
        view.message=await interaction.original_response()
        
    @discord.ui.button(label='⏮', style=discord.ButtonStyle.primary)
    async def front(self, interaction, button):
        self.page=1
        await self.updateembed(interaction)
    @discord.ui.button(label='⏴', style=discord.ButtonStyle.primary)
    async def prev(self, interaction, button):
        self.page=self.page-1
        await self.updateembed(interaction)
    @discord.ui.button(label=f"0/0", disabled=True)
    async def dummy(self, interaction, button):
        1
    @discord.ui.button(label='⏵', style=discord.ButtonStyle.primary)
    async def next(self, interaction, button):
        self.page=self.page+1
        await self.updateembed(interaction)
    @discord.ui.button(label='⏭', style=discord.ButtonStyle.primary)
    async def back(self, interaction, button):
        self.page=self.lens
        await self.updateembed(interaction)
    async def interaction_check(self, interaction: discord.Interaction):
        return interaction.user.id == self.author

@client.event
async def on_ready():
    await tree.sync()
    cur.execute('select server_id from server_info')
    alr={i for (i,) in cur}
    empty={i.id for i in client.guilds}-alr
    for i in empty:
        cur.execute('''insert into server_info(server_id) VALUES (?)''',data=(i,))
    conn.commit()
    print(f'Logged in as {client.user} (ID: {client.user.id})')
    print('------')

@client.event
async def on_guild_join(guild):
    cur.execute('''insert into server_info(server_id) VALUES (?)''',data=(guild.id,))

modperms=discord.Permissions(moderate_members=True)

assign = app_commands.Group(name="set", description="Set different things for the chain.",default_permissions=modperms)

@assign.command(description="Sets the channel for the bot to monitor for cities chain.")
@app_commands.describe(channel="The channel where the cities chain will happen")
async def channel(interaction: discord.Interaction, channel: discord.TextChannel|discord.Thread):
    await interaction.response.defer()
    cur.execute('''update server_info
        set channel_id = ?
        where server_id = ?''', data=(channel.id,interaction.guild_id))
    conn.commit()
    await interaction.followup.send('Channel set to <#%s>.'%channel.id)

@assign.command(description="Sets the gap between when a city can be repeated in the chain.")
@app_commands.describe(num="The minimum number of cities before they can repeat again, set to -1 to disallow any repeats")
async def repeat(interaction: discord.Interaction, num: app_commands.Range[int,-1,None]):
    await interaction.response.defer()
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

@assign.command(description="Sets the minimum population of cities in the chain.")
@app_commands.describe(population="The minimum population of cities in the chain")
async def population(interaction: discord.Interaction, population: app_commands.Range[int,1,None]):
    await interaction.response.defer()
    cur.execute('''select chain_end from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()[0] 
    if c:
        cur.execute('''update server_info
                    set min_pop = ?
                    where server_id = ?''', data=(population,interaction.guild_id))
        conn.commit()
        await interaction.followup.send('Minimum number of cities before repeating set to **%s**.'%f'{population:,}')
    else:
        await interaction.followup.send('Command can only be used after the chain has ended.')

@assign.command(description="Sets the prefix to listen to.")
@app_commands.describe(prefix="Prefix that all cities to be chained must begin with")
async def prefix(interaction: discord.Interaction, prefix: Optional[app_commands.Range[str,0,10]]=''):
    await interaction.response.defer()
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

@assign.command(name='choose-city',description="Toggles if bot can choose starting city for the next chain.")
@app_commands.describe(option="on to let the bot choose the next city, off otherwise")
async def choosecity(interaction: discord.Interaction, option:Literal["on","off"]):
    await interaction.response.defer()
    guildid=interaction.guild_id
    cur.execute('''select chain_end,min_pop,round_number from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()
    if c[0]:
        if option=='on':
            poss=allnames[allnames['population']>=c[1]]
            newid=int(random.choice(poss.index))
            entr=citydata[(citydata['geonameid']==newid) & (citydata['default']==1)]
            nname=poss.at[newid,'name']
            n=(nname,iso2[entr['country'].iloc[0]],entr['country'].iloc[0],admin1data[(admin1data['country']==entr['country'].iloc[0])&(admin1data['admin1']==entr['admin1'].iloc[0])&(admin1data['default']==1)]['name'].iloc[0],(entr['alt-country'].iloc[0],))
            cur.execute('''update server_info
                        set choose_city = ?,
                            current_letter = ?
                        where server_id = ?''', data=(True,entr['last letter'].iloc[0],guildid))
            cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin1,country,country_code,alt_country,time_placed,valid)
                        values (?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,newid,c[2]+1,1,n[0],n[3],n[1],n[2],n[4][0] if n[4] else None,int(interaction.created_at.timestamp()),True))
            await interaction.followup.send('Choose_city set to **ON**. Next city is **%s.**'%nname)
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
    s=search.casefold()
    results=[i for i in allcountries if i.casefold().startswith(s)]
    results.extend([iso2[i] for i in iso2 if i.casefold().startswith(s) and iso2[i] not in results])
    results.extend([iso3[i] for i in iso3 if i.casefold().startswith(s) and iso3[i] not in results])
    return [app_commands.Choice(name=i,value=i) for i in results[:10]]

add = app_commands.Group(name='add', description="Adds reactions/repeats for the chain.",default_permissions=modperms)
@add.command(description="Adds reaction for a city. When cityed, react to client's message with emoji to react to city with.")
@app_commands.describe(city="The city that the client will react to",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def react(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    res=search_cities(city,province,country)
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

@add.command(description="Adds repeating exception for a city.")
@app_commands.describe(city="The city that the client will allow repeats for",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def repeat(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    cur.execute('''select chain_end,min_pop,round_number from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()[0]  
    if c:
        res=search_cities(city,province,country)
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

remove = app_commands.Group(name='remove', description="Removes reactions/repeats for the chain.",default_permissions=modperms)
@remove.command(description="Removes reaction for a city.")
@app_commands.describe(city="The city that the client will not react to",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def react(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    res=search_cities(city,province,country)
    if res:
        try:
            cur.execute('delete from react_info where server_id = ? and city_id = ?', data=(interaction.guild_id,res[0]))
            conn.commit()
            await interaction.followup.send('Reaction for %s removed.'%res[2])
        except:
            await interaction.followup.send('%s had no reactions.'%res[2])
    else:
        await interaction.followup.send('City not recognized. Please try again. ')

@remove.command(description="Removes repeating exception for a city.")
@app_commands.describe(city="The city that the client will disallow repeats for",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division')
@app_commands.autocomplete(country=countrycomplete)
async def repeat(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None):
    await interaction.response.defer()
    cur.execute('''select chain_end from server_info
                where server_id = ?''', data=(interaction.guild_id,))
    c=cur.fetchone()[0]  
    if c:
        res=search_cities(city,province,country)
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
async def on_message(message:discord.Message):
    authorid=message.author.id
    if authorid!=client.user.id:
        guildid=message.guild.id
        
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
        if message.content.startswith(sinfo[10]) and message.content[len(sinfo[10]):].strip()!='' and message.channel.id==sinfo[6] and not message.author.bot:
            cur.execute('''select * from server_user_info where user_id = ? and server_id = ?''',data=(authorid,message.guild.id))
            if cur.rowcount==0:
                cur.execute('''select * from global_user_info where user_id = ?''',data=(authorid,))
                if cur.rowcount==0:
                    cur.execute('''insert into global_user_info(user_id) values (?)''',data=(authorid,))
                cur.execute('''insert into server_user_info(user_id,server_id) values (?,?)''',data=(authorid,guildid))
            if sinfo[5]:
                cur.execute('''update server_info set chain_end = ?, round_number = ? where server_id = ?''',data=(False,sinfo[0]+1,guildid))
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
            cur.execute('''select city_id from chain_info where server_id = ? and round_number = ? order by count asc''',data=(guildid,sinfo[0]))
            citieslist=[i for (i,) in cur]
            res=search_cities_chain(message.content[len(sinfo[10]):])
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
                j=citydata[(citydata['geonameid']==res[0])&(citydata['default']==1)].iloc[0]
                name,adm1,country,altcountry=j['name'],admin1data[(admin1data['country']==j['country'])&(admin1data['admin1']==j['admin1'])&(admin1data['default']==1)]['name'].iloc[0],j['country'],j['alt-country']
                if not ((res[2].replace(' ','').isalpha() and res[2].isascii()) or res[1]['default']==1):
                    n=(res[2]+' ('+name+')',(iso2[country],country),adm1,(altcountry,))
                else:
                    n=(res[2],(iso2[country],country),adm1,(altcountry,))
                letters=(res[1]['first letter'],res[1]['last letter'])
                # if res[1]['default']==1:
                #     if adm1:
                #         if altcountry:
                #             loctuple=(res[2],adm1,country+'/'+altcountry)
                #         else:
                #             loctuple=(res[2],adm1,country)
                #     else:
                #         if altcountry:
                #             loctuple=(res[2],country+'/'+altcountry)
                #         else:
                #             loctuple=(res[2],country)
                # else:
                #     if adm1:
                #         if altcountry:
                #             loctuple=(res[2]+' (%s)'%name,adm1,country+'/'+altcountry)
                #         else:
                #             loctuple=(res[2]+' (%s)'%name,adm1,country)
                #     else:
                #         if altcountry:
                #             loctuple=(res[2]+' (%s)'%name,country+'/'+altcountry)
                #         else:
                #             loctuple=(res[2]+' (%s)'%name,country)
                # print(message.guild.name,message.author.name,loctuple,res[1]['population'])
                if (sinfo[7]=='-' or sinfo[7]==letters[0]):
                    if sinfo[2]<=res[1]['population']:
                        cur.execute('''select city_id from repeat_info where server_id = ?''', data=(guildid,))
                        if cur.rowcount>0:
                            repeatset=set(cur.fetchall())
                        else:
                            repeatset=set()
                        if ((sinfo[4] and res[0] not in citieslist[:sinfo[1]]) or (not sinfo[4] and res[0] not in citieslist) or (res[0] in repeatset)):
                            if sinfo[8]!=message.author.id:
                                

                                cur.execute('''select correct,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                                uinfo=cur.fetchone()
                                cur.execute('''update server_user_info set correct = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]+1,int(message.created_at.timestamp()),guildid,authorid))
                                cur.execute('''select correct,score from global_user_info where user_id=?''',data=(authorid,))
                                uinfo=cur.fetchone()
                                cur.execute('''update global_user_info set correct = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]+1,int(message.created_at.timestamp()),authorid))
                                
                                if sinfo[9]<(len(citieslist)+1):
                                    cur.execute('''update server_info set max_chain = ?,last_best = ? where server_id = ?''',data=(len(citieslist)+1,int(message.created_at.timestamp()),guildid))
                                    await message.add_reaction('\N{BALLOT BOX WITH CHECK}')
                                else:
                                    await message.add_reaction('\N{WHITE HEAVY CHECK MARK}')
                                
                                cur.execute('''update server_info set last_user = ?, current_letter = ? where server_id = ?''',data=(authorid,letters[1],guildid))


                                cur.execute('''select reaction from react_info where server_id = ? and city_id = ?''', data=(guildid,res[0]))
                                if cur.rowcount>0:
                                    await message.add_reaction(cur.fetchone()[0])
                                if not ((res[2].replace(' ','').isalpha() and res[2].isascii())):
                                    await message.add_reaction(regionalindicators[letters[1]])
                                cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin1,country,country_code,alt_country,time_placed,valid) values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[2],n[1][0],n[1][1],n[3][0] if n[3] else None,int(message.created_at.timestamp()),True))
                            else:
                                await message.add_reaction('\N{CROSS MARK}')
                                cur.execute('''select incorrect,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                                uinfo=cur.fetchone()
                                cur.execute('''update server_user_info set incorrect = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),guildid,authorid))
                                cur.execute('''select incorrect,score from global_user_info where user_id=?''',data=(authorid,))
                                uinfo=cur.fetchone()
                                cur.execute('''update global_user_info set incorrect = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),authorid))
                                if sinfo[3]:
                                    poss=allnames[allnames['population']>=sinfo[2]]
                                    newid=int(random.choice(poss.index))
                                    await message.channel.send('<@%s> RUINED IT AT **%s**!! Next city is **%s.** **No going twice.**'%(message.author.id,f"{len(citieslist):,}",poss.at[newid,'name']))
                                else:
                                    await message.channel.send('<@%s> RUINED IT AT **%s**!! **No going twice.**'%(message.author.id,f"{len(citieslist):,}"))
                                cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin1,country,country_code,alt_country,time_placed,valid) values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[2],n[1][0],n[1][1],n[3][0] if n[3] else None,int(message.created_at.timestamp()),False))
                                cur.execute('''update server_info set chain_end = ?, current_letter = ?, last_user = ? where server_id = ?''',data=(True,'-',None,guildid))
                                if sinfo[3]:
                                    entr=citydata[(citydata['geonameid']==newid) & (citydata['default']==1)]
                                    nname=poss.at[newid,'name']
                                    n=(nname,iso2[entr['country'].iloc[0]],entr['country'].iloc[0],admin1data[(admin1data['country']==entr['country'].iloc[0])&(admin1data['admin1']==entr['admin1'].iloc[0])&(admin1data['default']==1)]['name'].iloc[0],(entr['alt-country'].iloc[0],))
                                    cur.execute('''update server_info
                                                set choose_city = ?,
                                                    current_letter = ?
                                                where server_id = ?''', data=(True,entr['last letter'].iloc[0],guildid))
                                    cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin1,country,country_code,alt_country,time_placed,valid)
                                                values (?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,int(newid),sinfo[0]+1,1,n[0],n[3],n[1],n[2],n[4][0] if n[4] else None,int(message.created_at.timestamp()),True))

                        else:
                            await message.add_reaction('\N{CROSS MARK}')
                            cur.execute('''select incorrect,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                            uinfo=cur.fetchone()
                            cur.execute('''update server_user_info set incorrect = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),guildid,authorid))
                            cur.execute('''select incorrect,score from global_user_info where user_id=?''',data=(authorid,))
                            uinfo=cur.fetchone()
                            cur.execute('''update global_user_info set incorrect = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),authorid))
                            if sinfo[3]:
                                poss=allnames[allnames['population']>=sinfo[2]]
                                newid=int(random.choice(poss.index))
                                if sinfo[4]:
                                    await message.channel.send('<@%s> RUINED IT AT **%s**!! Next city is **%s.** **No repeats within `%s` cities.**'%(message.author.id,f"{len(citieslist):,}",poss.at[newid,'name'],f"{sinfo[1]:,}"))
                                else:
                                    await message.channel.send('<@%s> RUINED IT AT **%s**!! Next city is **%s.** **No repeats.**'%(message.author.id,f"{len(citieslist):,}",poss.at[newid,'name']))
                            else:
                                if sinfo[4]:
                                    await message.channel.send('<@%s> RUINED IT AT **%s**!! **No repeats within `%s` cities.**'%(message.author.id,f"{len(citieslist):,}",f"{sinfo[1]:,}"))
                                else:
                                    await message.channel.send('<@%s> RUINED IT AT **%s**!! **No repeats.**'%(message.author.id,f"{len(citieslist):,}"))
                            cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin1,country,country_code,alt_country,time_placed,valid) values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[2],n[1][0],n[1][1],n[3][0] if n[3] else None,int(message.created_at.timestamp()),False))
                            cur.execute('''update server_info set chain_end = ?, current_letter = ?, last_user = ? where server_id = ?''',data=(True,'-',None,guildid))
                            if sinfo[3]:
                                entr=citydata[(citydata['geonameid']==newid) & (citydata['default']==1)]
                                nname=poss.at[newid,'name']
                                n=(nname,iso2[entr['country'].iloc[0]],entr['country'].iloc[0],admin1data[(admin1data['country']==entr['country'].iloc[0])&(admin1data['admin1']==entr['admin1'].iloc[0])&(admin1data['default']==1)]['name'].iloc[0],(entr['alt-country'].iloc[0],))
                                cur.execute('''update server_info
                                            set choose_city = ?,
                                                current_letter = ?
                                            where server_id = ?''', data=(True,entr['last letter'].iloc[0],guildid))
                                cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin1,country,country_code,alt_country,time_placed,valid)
                                            values (?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,int(newid),sinfo[0]+1,1,n[0],n[3],n[1],n[2],n[4][0] if n[4] else None,int(message.created_at.timestamp()),True))
                    else:
                        await message.add_reaction('\N{CROSS MARK}')
                        cur.execute('''select incorrect,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                        uinfo=cur.fetchone()
                        cur.execute('''update server_user_info set incorrect = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),guildid,authorid))
                        cur.execute('''select incorrect,score from global_user_info where user_id=?''',data=(authorid,))
                        uinfo=cur.fetchone()
                        cur.execute('''update global_user_info set incorrect = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),authorid))
                        if sinfo[3]:
                            poss=allnames[allnames['population']>=sinfo[2]]
                            newid=int(random.choice(poss.index))
                            await message.channel.send('<@%s> RUINED IT AT **%s**!! Next city is **%s.** **City must have a population of at least `%s`.**'%(message.author.id,f"{len(citieslist):,}",poss.at[newid,'name'],f"{sinfo[2]:,}"))
                        else:
                            await message.channel.send('<@%s> RUINED IT AT **%s**!! **City must have a population of at least `%s`.**'%(message.author.id,f"{len(citieslist):,}",f"{sinfo[2]:,}"))
                        cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin1,country,country_code,alt_country,time_placed,valid) values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[2],n[1][0],n[1][1],n[3][0] if n[3] else None,int(message.created_at.timestamp()),False))
                        cur.execute('''update server_info set chain_end = ?, current_letter = ?, last_user = ? where server_id = ?''',data=(True,'-',None,guildid))
                        if sinfo[3]:
                            entr=citydata[(citydata['geonameid']==newid) & (citydata['default']==1)]
                            nname=poss.at[newid,'name']
                            n=(nname,iso2[entr['country'].iloc[0]],entr['country'].iloc[0],admin1data[(admin1data['country']==entr['country'].iloc[0])&(admin1data['admin1']==entr['admin1'].iloc[0])&(admin1data['default']==1)]['name'].iloc[0],(entr['alt-country'].iloc[0],))
                            cur.execute('''update server_info
                                        set choose_city = ?,
                                            current_letter = ?
                                        where server_id = ?''', data=(True,entr['last letter'].iloc[0],guildid))
                            cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin1,country,country_code,alt_country,time_placed,valid)
                                        values (?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,int(newid),sinfo[0]+1,1,n[0],n[3],n[1],n[2],n[4][0] if n[4] else None,int(message.created_at.timestamp()),True))
                else:
                    await message.add_reaction('\N{CROSS MARK}')
                    cur.execute('''select incorrect,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                    uinfo=cur.fetchone()
                    cur.execute('''update server_user_info set incorrect = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),guildid,authorid))
                    cur.execute('''select incorrect,score from global_user_info where user_id=?''',data=(authorid,))
                    uinfo=cur.fetchone()
                    cur.execute('''update global_user_info set incorrect = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),authorid))
                    if sinfo[3]:
                        poss=allnames[allnames['population']>=sinfo[2]]
                        newid=int(random.choice(poss.index))
                        await message.channel.send('<@%s> RUINED IT AT **%s**!! Next city is **%s.** **Wrong letter.**'%(message.author.id,f"{len(citieslist):,}",poss.at[newid,'name']))
                    else:
                        await message.channel.send('<@%s> RUINED IT AT **%s**!! **Wrong letter.**'%(message.author.id,f"{len(citieslist):,}"))
                    cur.execute('''insert into chain_info(server_id,user_id,round_number,count,city_id,name,admin1,country,country_code,alt_country,time_placed,valid) values (?,?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,res[0],n[0],n[2],n[1][0],n[1][1],n[3][0] if n[3] else None,int(message.created_at.timestamp()),False))
                    cur.execute('''update server_info set chain_end = ?, current_letter = ?, last_user = ? where server_id = ?''',data=(True,'-',None,guildid))
                    if sinfo[3]:
                        entr=citydata[(citydata['geonameid']==newid) & (citydata['default']==1)]
                        nname=poss.at[newid,'name']
                        n=(nname,iso2[entr['country'].iloc[0]],entr['country'].iloc[0],admin1data[(admin1data['country']==entr['country'].iloc[0])&(admin1data['admin1']==entr['admin1'].iloc[0])&(admin1data['default']==1)]['name'].iloc[0],(entr['alt-country'].iloc[0],))
                        cur.execute('''update server_info
                                    set choose_city = ?,
                                        current_letter = ?
                                    where server_id = ?''', data=(True,entr['last letter'].iloc[0],guildid))
                        cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin1,country,country_code,alt_country,time_placed,valid)
                                    values (?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,int(newid),sinfo[0]+1,1,n[0],n[3],n[1],n[2],n[4][0] if n[4] else None,int(message.created_at.timestamp()),True))
            else:
                await message.add_reaction('\N{CROSS MARK}')
                cur.execute('''select incorrect,score from server_user_info where server_id = ? and user_id = ?''',data=(guildid,authorid))
                uinfo=cur.fetchone()
                cur.execute('''update server_user_info set incorrect = ?, score = ?, last_active = ? where server_id = ? and user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),guildid,authorid))
                cur.execute('''select incorrect,score from global_user_info where user_id=?''',data=(authorid,))
                uinfo=cur.fetchone()
                cur.execute('''update global_user_info set incorrect = ?, score = ?, last_active = ? where user_id = ?''',data=(uinfo[0]+1,uinfo[1]-1,int(message.created_at.timestamp()),authorid))
                # print(message.guild.name,message.author.name,message.content[len(sinfo[10]):])
                if sinfo[3]:
                    poss=allnames[allnames['population']>=sinfo[2]]
                    newid=int(random.choice(poss.index))
                    await message.channel.send('<@%s> RUINED IT AT **%s**!! Next city is **%s.** **City not recognized.**'%(message.author.id,f"{len(citieslist):,}",poss.at[newid,'name']))
                else:
                    await message.channel.send('<@%s> RUINED IT AT **%s**!! **City not recognized.**'%(message.author.id,f"{len(citieslist):,}"))
                cur.execute('''insert into chain_info(server_id,user_id,round_number,count,name,time_placed,valid) values (?,?,?,?,?,?,?)''',data=(guildid,authorid,sinfo[0],len(citieslist)+1,message.content[len(sinfo[10]):],int(message.created_at.timestamp()),False))
                cur.execute('''update server_info set chain_end = ?, current_letter = ?, last_user = ? where server_id = ?''',data=(True,'-',None,guildid))
                if sinfo[3]:
                    entr=citydata[(citydata['geonameid']==newid) & (citydata['default']==1)]
                    nname=poss.at[newid,'name']
                    n=(nname,iso2[entr['country'].iloc[0]],entr['country'].iloc[0],admin1data[(admin1data['country']==entr['country'].iloc[0])&(admin1data['admin1']==entr['admin1'].iloc[0])&(admin1data['default']==1)]['name'].iloc[0],(entr['alt-country'].iloc[0],))
                    cur.execute('''update server_info
                                set choose_city = ?,
                                    current_letter = ?
                                where server_id = ?''', data=(True,entr['last letter'].iloc[0],guildid))
                    cur.execute('''insert into chain_info(server_id,city_id,round_number,count,name,admin1,country,country_code,alt_country,time_placed,valid)
                                values (?,?,?,?,?,?,?,?,?,?,?)''',data=(guildid,int(newid),sinfo[0]+1,1,n[0],n[3],n[1],n[2],n[4][0] if n[4] else None,int(message.created_at.timestamp()),True))
        conn.commit()

stats = app_commands.Group(name='stats',description="description")
@app_commands.rename(se='show-everyone')
@stats.command(description="Displays server statistics.")
async def server(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    embed=discord.Embed(title="Server Stats", color=discord.Colour.from_rgb(0,255,0))
    if interaction.guild.icon:
        embed.set_author(name=interaction.guild.name, icon_url=interaction.guild.icon.url)
    else:
        embed.set_author(name=interaction.guild.name)
    cur.execute('select round_number,min_repeat,min_pop,choose_city,repeats,current_letter,last_user,max_chain,last_best,prefix from server_info where server_id = ?',data=(guildid,))
    sinfo=cur.fetchone()
    cur.execute('select * from chain_info where server_id = ? and round_number = ?',data=(guildid,sinfo[0]))
    embed.description='Round: **%s**\nCurrent letter: **%s**\nCurrent length: **%s**\nLast user: **%s**\nLongest chain: **%s** %s\nMinimum population: **%s**\nChoose city: **%s**\nRepeats: **%s**\nPrefix: %s'%(f'{sinfo[0]:,}',sinfo[5],f'{cur.rowcount:,}','<@'+str(sinfo[6])+'>' if sinfo[6] else '-',f'{sinfo[7]:,}','<t:'+str(sinfo[8])+':R>' if sinfo[8] else '',f'{sinfo[2]:,}','enabled' if sinfo[3] else 'disabled', 'only after %s cities'%f'{sinfo[1]:,}' if sinfo[4] else 'allowed','**'+sinfo[9]+'**' if sinfo[9]!='' else None)
    await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays user statistics.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(member="The user to get statistics for.")
async def user(interaction: discord.Interaction, member:Optional[discord.Member]=None,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    if not member:
        member=interaction.user
    cur.execute('select correct,incorrect,score,last_active from global_user_info where user_id = ?',data=(member.id,))
    if cur.rowcount==0:
        if member.id==interaction.user.id:
            await interaction.followup.send(embed=discord.Embed(color=discord.Colour.from_rgb(255,0,0),description='You must join the chain to use that command. '),ephemeral=eph)
        else:
            await interaction.followup.send(embed=discord.Embed(color=discord.Colour.from_rgb(255,0,0),description=f'<@{member.id}> has no Cities Chain stats. '),ephemeral=eph)
    else:
        uinfo=cur.fetchone()
        embed=discord.Embed(title="User Stats", color=discord.Colour.from_rgb(0,255,0))
        if member.avatar:
            embed.set_author(name=member.name, icon_url=member.avatar.url)
        else:
            embed.set_author(name=member.name)
        if (uinfo[0]+uinfo[1])>0:
            embed.add_field(name='Global Stats',value=f"Correct: **{f'{uinfo[0]:,}'}**\nIncorrect: **{f'{uinfo[1]:,}'}**\nCorrect Rate: **{round(uinfo[0]/(uinfo[0]+uinfo[1])*10000)/100}%**\nScore: **{f'{uinfo[2]:,}'}**\nLast Active: <t:{uinfo[3]}:R>",inline=True)
        cur.execute('select correct,incorrect,score,last_active from server_user_info where user_id = ? and server_id = ?',data=(member.id,interaction.guild_id))
        if cur.rowcount>0:
            uinfo=cur.fetchone()
            embed.add_field(name='Stats for ```%s```'%interaction.guild.name,value=f"Correct: **{f'{uinfo[0]:,}'}**\nIncorrect: **{f'{uinfo[1]:,}'}**\nCorrect Rate: **{round(uinfo[0]/(uinfo[0]+uinfo[1])*10000)/100}%**\nScore: **{f'{uinfo[2]:,}'}**\nLast Active: <t:{uinfo[3]}:R>",inline=True)
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays list of cities that cannot be repeated.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(order='The order in which the cities are presented, sequential or alphabetical')
async def cities(interaction: discord.Interaction,order:Literal['sequential','alphabetical'],se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    cur.execute('''select round_number,repeats,min_repeat from server_info where server_id = ?''',data=(guildid,))
    s=cur.fetchone()
    cur.execute('''select name,admin1,country,country_code,alt_country from chain_info where server_id = ? and round_number = ? order by count desc''',data=(guildid,s[0]))
    if cur.rowcount>0:
        cutoff=[]
        for i in cur:
            if i[1] and i[4]:
                cutoff.append((i[0],(i[2],i[3]),i[1],(i[4],)))
            elif i[1]:
                cutoff.append((i[0],(i[2],i[3]),i[1]))
            elif i[4]:
                cutoff.append((i[0],(i[2],i[3]),(i[4],)))
            else:
                cutoff.append((i[0],(i[2],i[3])))
        if s[1]:
            cutoff=cutoff[:s[2]]
        fmt=[]
        for i in cutoff:
            if len(i)==4:
                fmt.append(i[0]+', '+i[2]+' :flag_'+i[1][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[3]))
            elif len(i)==2:
                fmt.append(i[0]+' :flag_'+i[1][1].lower()+':')
            elif type(i[2])==tuple:
                fmt.append(i[0]+' :flag_'+i[1][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[2]))
            else:
                fmt.append(i[0]+', '+i[2]+' :flag_'+i[1][1].lower()+':')
        seq=['%s. %s'%(n+1,fmt[n]) for n,i in enumerate(cutoff)]
        alph=['- '+fmt[i[1]] for i in sorted(zip(cutoff,range(len(cutoff))))]
        if order=='sequential':
            embed=discord.Embed(title="Non-Repeatable Cities - Sequential Order", color=discord.Colour.from_rgb(0,255,0),description='\n'.join(seq[:25]))
            view=Paginator(1,seq,"Non-Repeatable - Sequential Order",math.ceil(len(seq)/25),interaction.user.id)
            await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
            view.message=await interaction.original_response()
        else:
            embed=discord.Embed(title="Non-Repeatable Cities - Alphabetical Order", color=discord.Colour.from_rgb(0,255,0),description='\n'.join(alph[:25]))
            view=Paginator(1,alph,"Non-Repeatable Cities - Alphabetical Order",math.ceil(len(alph)/25),interaction.user.id)
            await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
            view.message=await interaction.original_response()
    else:
        if order=='sequential':
            embed=discord.Embed(title="Non-Repeatable Cities - Sequential Order", color=discord.Colour.from_rgb(0,255,0),description='```null```')
        else:
            embed=discord.Embed(title="Non-Repeatable Cities - Alphabetical Order", color=discord.Colour.from_rgb(0,255,0),description='```null```')
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='cities-all',description="Displays list of all cities having been said, regardless of whether they have been repeated.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(order='The order in which the cities are presented, sequential or alphabetical')
async def allcities(interaction: discord.Interaction,order:Literal['sequential','alphabetical'],se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    cur.execute('''select round_number from server_info where server_id = ?''',data=(guildid,))
    s=cur.fetchone()
    cur.execute('''select name,admin1,country,country_code,alt_country from chain_info where server_id = ? and round_number = ? order by count desc''',data=(guildid,s[0]))
    if cur.rowcount>0:
        cutoff=[]
        for i in cur:
            if i[1] and i[4]:
                cutoff.append((i[0],(i[2],i[3]),i[1],(i[4],)))
            elif i[1]:
                cutoff.append((i[0],(i[2],i[3]),i[1]))
            elif i[4]:
                cutoff.append((i[0],(i[2],i[3]),(i[4],)))
            else:
                cutoff.append((i[0],(i[2],i[3])))
        fmt=[]
        for i in cutoff:
            if len(i)==4:
                fmt.append(i[0]+', '+i[2]+' :flag_'+i[1][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[3]))
            elif len(i)==2:
                fmt.append(i[0]+' :flag_'+i[1][1].lower()+':')
            elif type(i[2])==tuple:
                fmt.append(i[0]+' :flag_'+i[1][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[2]))
            else:
                fmt.append(i[0]+', '+i[2]+' :flag_'+i[1][1].lower()+':')
        seq=['%s. %s'%(n+1,fmt[n]) for n,i in enumerate(cutoff)]
        alph=['- '+fmt[i[1]] for i in sorted(zip(cutoff,range(len(cutoff))))]
        if order=='sequential':
            embed=discord.Embed(title="All Cities - Sequential Order", color=discord.Colour.from_rgb(0,255,0),description='\n'.join(seq[:25]))
            view=Paginator(1,seq,"All Cities - Sequential Order",math.ceil(len(seq)/25),interaction.user.id)
            await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
            view.message=await interaction.original_response()
        else:
            embed=discord.Embed(title="All Cities - Alphabetical Order", color=discord.Colour.from_rgb(0,255,0),description='\n'.join(alph[:25]))
            view=Paginator(1,alph,"All Cities - Alphabetical Order",math.ceil(len(alph)/25),interaction.user.id)
            await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
            view.message=await interaction.original_response()
    else:
        if order=='sequential':
            embed=discord.Embed(title="All Cities - Sequential Order", color=discord.Colour.from_rgb(0,255,0),description='```null```')
        else:
            embed=discord.Embed(title="All Cities - Alphabetical Order", color=discord.Colour.from_rgb(0,255,0),description='```null```')
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays all cities said for one round.")
@app_commands.rename(se='show-everyone')
@app_commands.describe(round_num='Round to retrieve information from')
async def round(interaction: discord.Interaction,round_num:app_commands.Range[int,1,None],se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    guildid=interaction.guild_id
    cur.execute('''select round_number from server_info where server_id = ?''',data=(guildid,))
    s=cur.fetchone()
    cur.execute('''select name,admin1,country,country_code,alt_country,city_id,valid from chain_info where server_id = ? and round_number = ? order by count asc''',data=(guildid,round_num))
    if round_num<=s[0]:
        cutoff=[]
        for i in cur:
            if i[5]==-1:
                cutoff.append((i[0],i[6]))
            else:
                if i[1] and i[4]:
                    cutoff.append((i[0],i[6],(i[2],i[3]),i[1],(i[4],)))
                elif i[1]:
                    cutoff.append((i[0],i[6],(i[2],i[3]),i[1]))
                elif i[4]:
                    cutoff.append((i[0],i[6],(i[2],i[3]),(i[4],)))
                else:
                    cutoff.append((i[0],i[6],(i[2],i[3])))
        fmt=[]
        for n,i in enumerate(cutoff):
            if i[1]:
                if len(i)==5:
                    fmt.append(str(n+1)+'. '+i[0]+', '+i[3]+' :flag_'+i[2][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[4]))
                elif len(i)==3:
                    fmt.append(str(n+1)+'. '+i[0]+' :flag_'+i[2][1].lower()+':')
                elif type(i[3])==tuple:
                    fmt.append(str(n+1)+'. '+i[0]+' :flag_'+i[2][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[3]))
                else:
                    fmt.append(str(n+1)+'. '+i[0]+', '+i[3]+' :flag_'+i[2][1].lower()+':')
            else:
                if len(i)==2:
                    fmt.append(':x: '+i[0])
                elif len(i)==5:
                    fmt.append(':x: '+i[0]+', '+i[3]+' :flag_'+i[2][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[4]))
                elif len(i)==3:
                    fmt.append(':x: '+i[0]+' :flag_'+i[2][1].lower()+':')
                elif type(i[3])==tuple:
                    fmt.append(':x: '+i[0]+' :flag_'+i[2][1].lower()+':'+''.join(':flag_'+j.lower()+':' for j in i[3]))
                else:
                    fmt.append(':x: '+i[0]+', '+i[3]+' :flag_'+i[2][1].lower()+':')
        embed=discord.Embed(title="Round %s - `%s`"%(f'{round_num:,}',interaction.guild.name), color=discord.Colour.from_rgb(0,255,0),description='\n'.join(fmt[:25]))
        view=Paginator(1,fmt,"Round %s - `%s`"%(f'{round_num:,}',interaction.guild.name),math.ceil(len(fmt)/25),interaction.user.id)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
        view.message=await interaction.original_response()
    else:
        await interaction.followup.send("Round_num must be a number between **1** and **%s**."%s[0],ephemeral=eph)

@stats.command(description="Displays server leaderboard.")
@app_commands.rename(se='show-everyone')
async def slb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title=f"```{interaction.guild.name}``` LEADERBOARD",color=discord.Colour.from_rgb(0,255,0))
    cur.execute('''select user_id,score from server_user_info where server_id = ?''',data=(interaction.guild_id,))
    if cur.rowcount>0:
        l=sorted([(i[1],i[0]) for i in cur],reverse=1)
        embed.description='\n'.join([f'**{n+1}. **<@{i[1]}> - **{f"{i[0]:,}"}**' for n,i in enumerate(l[:10])])
    else:
        embed.description='```null```'
    await interaction.followup.send(embed=embed,ephemeral=eph)    

@stats.command(description="Displays global leaderboard.")
@app_commands.rename(se='show-everyone')
async def lb(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title=f"GLOBAL LEADERBOARD",color=discord.Colour.from_rgb(0,255,0))
    cur.execute('''select user_id,score from global_user_info''',data=(interaction.guild_id,))
    if cur.rowcount>0:
        l=sorted([(i[1],i[0]) for i in cur],reverse=1)
        embed.description='\n'.join([f'**{n+1}. **<@{i[1]}> - **{f"{i[0]:,}"}**' for n,i in enumerate(l[:10])])
    else:
        embed.description='```null```'
    await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays all cities and their reactions.")
@app_commands.rename(se='show-everyone')
async def react(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title='Cities With Reactions',color=discord.Colour.from_rgb(0,255,0))
    cur.execute('''select city_id,reaction from react_info where server_id = ?''', data=(interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[]
        for (i,r) in cur:
            j=citydata[(citydata['geonameid']==i)&(citydata['default']==1)].iloc[0]
            k,l,m,n=j['name'],admin1data[(admin1data['country']==j['country'])&(admin1data['admin1']==j['admin1'])&(admin1data['default']==1)]['name'].iloc[0],j['country'],j['alt-country']
            if l:
                if n:
                    loctuple=(k,l,m+'/'+n)
                else:
                    loctuple=(k,l,m)
            else:
                if n:
                    loctuple=(k,m+'/'+n)
                else:
                    loctuple=(k,m)
            fmt.append(('- '+', '.join(loctuple),r))
        fmt=sorted(fmt)
        embed.description='\n'.join([i[0]+' '+i[1] for i in fmt[:25]])
        view=Paginator(1,[i[0]+' '+i[1] for i in fmt],"Cities With Reactions",math.ceil(len(fmt)/25),interaction.user.id)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
        view.message=await interaction.original_response()
    else:
        embed.description="```null```"
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(description="Displays all cities that can be repeated.")
@app_commands.rename(se='show-everyone')
async def repeat(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    embed=discord.Embed(title='Repeats Rule Exceptions',color=discord.Colour.from_rgb(0,255,0))
    cur.execute('''select city_id from repeat_info where server_id = ?''', data=(interaction.guild_id,))
    if cur.rowcount>0:
        fmt=[]
        for (i,) in cur:
            j=citydata[(citydata['geonameid']==i)&(citydata['default']==1)].iloc[0]
            k,l,m,n=j['name'],admin1data[(admin1data['country']==j['country'])&(admin1data['admin1']==j['admin1'])&(admin1data['default']==1)]['name'].iloc[0],j['country'],j['alt-country']
            if l:
                if n:
                    loctuple=(k,l,m+'/'+n)
                else:
                    loctuple=(k,l,m)
            else:
                if n:
                    loctuple=(k,m+'/'+n)
                else:
                    loctuple=(k,m)
            fmt.append('- '+', '.join(loctuple))
        fmt=sorted(fmt)
        embed.description='\n'.join(fmt[:25])
        view=Paginator(1,fmt,"Repeats Rule Exceptions",math.ceil(len(fmt)/25),interaction.user.id)
        await interaction.followup.send(embed=embed,view=view,ephemeral=eph)
        view.message=await interaction.original_response()
    else:
        embed.description="```null```"
        await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='popular-cities',description="Displays most popular cities and countries added to chain.")
@app_commands.rename(se='show-everyone')
async def popular(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    cur.execute('''select distinct city_id from chain_info where server_id = ? and city_id >= 0 and valid = 1''',data=(interaction.guild_id,))
    cities=[i[0] for i in cur]
    cur.execute('''select distinct country_code from chain_info where server_id = ? and country_code is not null and valid = 1''',data=(interaction.guild_id,))
    countries=[i[0] for i in cur]
    cur.execute('''select distinct alt_country from chain_info where server_id = ? and alt_country is not null and valid = 1''',data=(interaction.guild_id,))
    countries.extend([i[0] for i in cur])
    embed=discord.Embed(title="Popular Cities/Countries - `%s`"%interaction.guild.name,color=discord.Colour.from_rgb(0,255,0))
    if len(cities)>0:
        fmt=[]
        for i in cities:
            cur.execute('''select count(*) from chain_info where server_id = ? and city_id = ? and valid = 1''',data=(interaction.guild_id,i))
            c=cur.fetchone()[0]
            j=citydata[(citydata['geonameid']==i)&(citydata['default']==1)].iloc[0]
            k,l,m,n=j['name'],admin1data[(admin1data['country']==j['country'])&(admin1data['admin1']==j['admin1'])&(admin1data['default']==1)]['name'].iloc[0],j['country'],j['alt-country']
            if l:
                if n:
                    loctuple=(k,l,m+'/'+n)
                else:
                    loctuple=(k,l,m)
            else:
                if n:
                    loctuple=(k,m+'/'+n)
                else:
                    loctuple=(k,m)
            fmt.append((c,', '.join(loctuple)))
        fmt=sorted(fmt,key = lambda x:(-x[0],x[1]))[:10]
        embed.add_field(name='Cities',value='\n'.join(['%s. %s - **%s**' %(n+1,i[1],f"{i[0]:,}") for (n,i) in enumerate(fmt)]))
        fmt=[]
        for i in countries:
            cur.execute('''select count(*) from chain_info where server_id = ? and (country_code = ? or alt_country = ? and valid = 1)''',data=(interaction.guild_id,i,i))
            c=cur.fetchone()[0]
            fmt.append((c,iso2[i]))
        fmt=sorted(fmt,key = lambda x:(-x[0],x[1]))[:10]
        embed.add_field(name='Countries',value='\n'.join(['%s. %s - **%s**' %(n+1,i[1],f"{i[0]:,}") for (n,i) in enumerate(fmt)]))
    else:
        embed.add_field(name='Cities',value='```null```')
        embed.add_field(name='Countries',value='```null```')
    await interaction.followup.send(embed=embed,ephemeral=eph)

@stats.command(name='best-rounds',description="Displays longest chains in server.")
@app_commands.rename(se='show-everyone')
async def bestrds(interaction: discord.Interaction,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    cur.execute('''select round_number from server_info where server_id = ?''',data=(interaction.guild_id,))
    rounds=range(1,cur.fetchone()[0]+1)
    embed=discord.Embed(title="Best Rounds - `%s`"%interaction.guild.name,color=discord.Colour.from_rgb(0,255,0))
    if len(rounds)>0:
        fmt=[]
        for i in rounds:
            cur.execute('''select count(*) from chain_info where server_id = ? and round_number = ? and valid = ?''',data=(interaction.guild_id,i,True))
            maxc=cur.fetchone()[0]
            if maxc>0:
                cur.execute('''select distinct user_id from chain_info where server_id = ? and round_number = ?  and valid = ? and user_id is not null''',data=(interaction.guild_id,i,True))
                part=cur.rowcount
                cur.execute('''select city_id,name from chain_info where server_id = ? and round_number = ? and count = ?''',data=(interaction.guild_id,i,1))
                b1=cur.fetchone()
                cur.execute('''select city_id,name from chain_info where server_id = ? and round_number = ? and count = ?''',data=(interaction.guild_id,i,maxc))
                b2=cur.fetchone()
                b=[]
                for j in (b1,b2):
                    o=citydata[(citydata['geonameid']==j[0])&(citydata['default']==1)].iloc[0]
                    k,l,m,n=o['name'],admin1data[(admin1data['country']==o['country'])&(admin1data['admin1']==o['admin1'])&(admin1data['default']==1)]['name'].iloc[0],o['country'],o['alt-country']
                    
                    if j[1]==k:
                        if l:
                            if n:
                                loctuple=(j[1],l,m+'/'+n)
                            else:
                                loctuple=(j[1],l,m)
                        else:
                            if n:
                                loctuple=(j[1],m+'/'+n)
                            else:
                                loctuple=(j[1],m)
                    else:
                        if l:
                            if n:
                                loctuple=(j[1]+' (%s)'%k,l,m+'/'+n)
                            else:
                                loctuple=(j[1]+' (%s)'%k,l,m)
                        else:
                            if n:
                                loctuple=(j[1]+' (%s)'%k,m+'/'+n)
                            else:
                                loctuple=(j[1]+' (%s)'%k,m)
                    b.append(', '.join(loctuple))
                fmt.append((maxc,i,part,tuple(b)))
            else:
                fmt.append((0,i,1,("None","None")))

        fmt=sorted(fmt,reverse=1)[:5]
        for i in fmt:
            if i[0]>1:
                embed.add_field(name='%s - %s'%i[3],value='Length: %s\nRound: %s\nParticipants: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}'))
            elif i[0]==1:
                embed.add_field(name='%s'%i[3][0],value='Length: %s\nRound: %s\nParticipants: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}'))
            else:
                embed.add_field(name='None',value='Length: %s\nRound: %s\nParticipants: %s'%(f'{i[0]:,}',f'{i[1]:,}',f'{i[2]:,}'))
    else:
        embed.add_field(name='',value='```null```')
    await interaction.followup.send(embed=embed,ephemeral=eph)

@tree.command(name='city-info',description='Gets information for a given city.')
@app_commands.describe(city="The city to get information for",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division',se='show-everyone')
@app_commands.autocomplete(country=countrycomplete)
async def cityinfo(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    res=search_cities(city,province,country)
    if res:
        aname=citydata[(citydata['geonameid']==res[0])]
        default=aname[(aname['default']==1)].iloc[0]
        dname=default['name']
        embed=discord.Embed(title='Information - %s'%dname,color=discord.Colour.from_rgb(0,255,0))
        embed.add_field(name='Geonames ID',value=res[0],inline=True)
        embed.add_field(name='Name',value=dname,inline=True)
        alts=aname[(aname['default']==0)]['name']
        if alts.shape[0]!=0:
            joinednames='`'+'`,`'.join(alts)+'`'
            if len(joinednames)<=1024:
                embed.add_field(name='Alternate Names',value=joinednames,inline=False)
            else:
                embed.add_field(name='Alternate Names',value='List of alternate names too long. Use `/alt-names [city]` to get list of alternate names.',inline=False)
        else:
            embed.add_field(name='',value='',inline=False)
        if default['admin1']:
            embed.add_field(name='Administrative Division',value=admin1data[(admin1data['country']==default['country'])&(admin1data['admin1']==default['admin1'])&(admin1data['default']==1)]['name'].iloc[0],inline=True)
        if default['alt-country']:
            embed.add_field(name='Countries',value=default['country']+' ('+iso2[default['country']]+')\n'+default['alt-country']+' ('+iso2[default['alt-country']]+')',inline=True)
        else:
            embed.add_field(name='Country',value=default['country']+' ('+iso2[default['country']]+')',inline=True)
        embed.add_field(name='Population',value=f"{default['population']:,}",inline=True)
        await interaction.followup.send(embed=embed,ephemeral=eph)
    else:
        await interaction.followup.send('City not recognized. Please try again. ',ephemeral=eph)

@tree.command(name='alt-names',description='Gets alternate names for a given city.')
@app_commands.describe(city="The city to get alternate names for",province="State, province, etc that the city is located in",country="Country the city is located in")
@app_commands.rename(province='administrative-division',se='show-everyone')
@app_commands.autocomplete(country=countrycomplete)
async def altnames(interaction: discord.Interaction, city:str, province:Optional[str]=None, country:Optional[str]=None,se:Optional[Literal['yes','no']]='no'):
    eph=True if se=='no' else False
    await interaction.response.defer(ephemeral=eph)
    res=search_cities(city,province,country)
    if res:
        aname=citydata[(citydata['geonameid']==res[0])]
        default=aname[aname['default']==1].iloc[0]
        dname=default['name']
        embed=discord.Embed(title='Alternate Names - %s'%dname,color=discord.Colour.from_rgb(0,255,0))
        alts=aname[aname['default']==0]['name']
        if alts.shape[0]>0:
            embed.description='`'+'`,`'.join(alts)+'`'
        else:
            embed.description='There are no alternate names for this city.'
        await interaction.followup.send(embed=embed,ephemeral=eph)
    else:
        await interaction.followup.send('City not recognized. Please try again. ',ephemeral=eph)

@tree.command(description="Tests the client's latency. ")
async def ping(interaction: discord.Interaction):
    await interaction.response.defer()
    latency = client.latency
    await interaction.followup.send('Pong! %s ms'%(latency*1000))

tree.add_command(assign)
tree.add_command(add)
tree.add_command(remove)
tree.add_command(stats)

client.run(env["DISCORD_TOKEN"])
