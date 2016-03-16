import mechanize
from bs4 import BeautifulSoup
import csv
import matplotlib

season = 2015

out = open('../../../../app/resources/output/mlb_players_current.csv', 'w')

# We will compare results with data pulled directly from online
br = mechanize.Browser()
soup = BeautifulSoup(br.open("http://www.baseball-reference.com/leagues/MLB/" + str(season) + "-standard-batting.shtml"),  "html.parser")
table = soup.find('table', attrs={'id':'players_standard_batting'})

header = table.find('thead')
header_names = header.find('tr').find_all('th')
header_index_lookup = {}
i = 0

h_names = []

for header_name in header_names:
    h_names.append(header_name.text.encode('utf-8'))
    header_index_lookup[header_name.text] = i
    i = i+1

out.write('PlayerId'+','+','.join(str(x) for x in h_names[1:]) + '\n')
# Create a dictionary for finding the index of the player of interest
rows = table.find_all('tr', attrs={'class':'full_table'})

dataset_header_index_lookup = {}
for row in rows:
    player_id = row.find_all('td')[1].a.get('href').split('/')[-1].split('.')[0].encode('utf-8')
    # Get all the player's data using the player_id as the lookup key
    player = row.find_all('td')
    stats = []
    for p in player:
        stats.append(p.text.encode('utf-8'))

    out.write(player_id + ',' + ','.join(str(x) for x in stats[1:]) + '\n')

out.close()

