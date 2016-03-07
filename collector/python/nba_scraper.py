import mechanize
from bs4 import BeautifulSoup
import csv

def checkNoneAndAppend(attr, player):
	if attr is None or attr is u'' or attr.isspace():
		player.append('0')
	else:
		player.append(attr)

seasonFrom = 1980
seasonTo = 2015

header = ["PlayerId", "Name", "Season", "Age", "Position", "Team", "Games", "MP", "FG%", "3PM", "3P%", "FT%", "REB", "AST", "STL", "BLK", "TOV", "PTS"]
with open('/tmp/artificialclairvoyance/nba/nbaPlayerTotals.csv', 'w') as out:
	writer = csv.writer(out, delimiter=',')
	writer.writerow(header)
	year = seasonFrom
	while year <= seasonTo:
		br = mechanize.Browser()
		soup = BeautifulSoup(br.open("http://www.basketball-reference.com/leagues/NBA_" + str(year) + "_totals.html"),  "html.parser")
		table = soup.find('table', attrs={'id':'totals'})
		rows = table.find_all('tr', attrs={'class':'full_table'})
		print "Scraping " + str(year) + " totals"
		for row in rows:
			cols = row.find_all('td')
			player = []
			# Player ID
			checkNoneAndAppend(cols[1].a.get('href').split('/')[-1].split('.')[0], player)
			# Name
			checkNoneAndAppend(cols[1].text, player)
			# Season
			player.append(str(year))
			# Age
			checkNoneAndAppend(cols[3].text, player)
			# Position
			checkNoneAndAppend(cols[2].text, player)
			# Team
			checkNoneAndAppend(cols[4].text, player)
			# Games Played
			checkNoneAndAppend(cols[5].text, player)
			# Minutes Played
			checkNoneAndAppend(cols[7].text, player)
			# FG%
			checkNoneAndAppend(cols[10].text, player)
			# 3PM
			checkNoneAndAppend(cols[11].text, player)
			# 3P%
			checkNoneAndAppend(cols[13].text, player)
			# FT%
			checkNoneAndAppend(cols[20].text, player)
			# REB
			checkNoneAndAppend(cols[23].text, player)
			# AST
			checkNoneAndAppend(cols[24].text, player)
			# STL
			checkNoneAndAppend(cols[25].text, player)
			# BLK
			checkNoneAndAppend(cols[26].text, player)
			# TOV
			checkNoneAndAppend(cols[27].text, player)
			# PTS
			checkNoneAndAppend(cols[29].text, player)
			writer.writerow(player)
		year += 1
