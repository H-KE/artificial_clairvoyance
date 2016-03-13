import mechanize
from bs4 import BeautifulSoup
import csv

seasonFrom = 1980
seasonTo = 2015

perGame = False

def checkNoneAndAppend(attr, player):
	if attr is None or attr is u'' or attr.isspace():
		player.append('0')
	else:
		player.append(attr)

def checkNoneAndAppendPerGame(attr, player, games):
	if attr is None or attr is u'' or attr.isspace():
		player.append('0')
	else:
		if perGame:
			g = float(games)
			stat = float(attr)
			if g is 0:
				player.append('0')
			else:
				player.append(stat/g)
		else:
			player.append(attr)

if perGame:
	file_name = "/tmp/nbaPlayerPerGame.csv"
else:
	file_name = "/tmp/nbaPlayerTotals.csv"


header = ["PlayerId", "Name", "Season", "Age", "Position", "Team", "Games", "MP", "FG%", "3PM", "3P%", "FT%", "REB", "AST", "STL", "BLK", "TOV", "PTS"]
with open(file_name, 'w') as out:
	writer = csv.writer(out, delimiter=',')
	writer.writerow(header)
	year = seasonFrom
	while year <= seasonTo:
		br = mechanize.Browser()
		soup = BeautifulSoup(br.open("http://www.basketball-reference.com/leagues/NBA_" + str(year) + "_totals.html"),  "html.parser")
		table = soup.find('table', attrs={'id':'totals'})
		rows = table.find_all('tr', attrs={'class':'full_table'})
		if perGame:
			print "Scraping " + str(year) + " per Game Averages"
		else:
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
			checkNoneAndAppendPerGame(cols[11].text, player, cols[5].text)
			# 3P%
			checkNoneAndAppend(cols[13].text, player)
			# FT%
			checkNoneAndAppend(cols[20].text, player)
			# REB
			checkNoneAndAppendPerGame(cols[23].text, player, cols[5].text)
			# AST
			checkNoneAndAppendPerGame(cols[24].text, player, cols[5].text)
			# STL
			checkNoneAndAppendPerGame(cols[25].text, player, cols[5].text)
			# BLK
			checkNoneAndAppendPerGame(cols[26].text, player, cols[5].text)
			# TOV
			checkNoneAndAppendPerGame(cols[27].text, player, cols[5].text)
			# PTS
			checkNoneAndAppendPerGame(cols[29].text, player, cols[5].text)
			writer.writerow(player)
		year += 1
