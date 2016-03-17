import mechanize
from bs4 import BeautifulSoup
import csv

season = 2016
column = "PTS"
file = "../../../../app/resources/output/nba_predictions.csv"
error_margin = 0.1

# We will compare results with data pulled directly from online
br = mechanize.Browser()
soup = BeautifulSoup(br.open("http://www.basketball-reference.com/leagues/NBA_" + str(season) + "_totals.html"),  "html.parser")
table = soup.find('table', attrs={'id':'totals'})

# Create a dictionary for finding the index of the column of interest
header = table.find('thead')
header_names = header.find('tr').find_all('th')
header_index_lookup = {}
i = 0
for header_name in header_names:
	header_index_lookup[header_name.text] = i
	i = i+1

# Create a dictionary for finding the index of the player of interest
rows = table.find_all('tr', attrs={'class':'full_table'})
player_index_lookup = {}
i = 0
for row in rows:
	player_index_lookup[row.find_all('td')[1].a.get('href').split('/')[-1].split('.')[0]] = i
	i = i+1

errnum = 0
# Dealing with the actual dataset we want to verify
with open(file, 'r') as dataset:
	reader = csv.reader(dataset)
	row_num = 0
	dataset_header_index_lookup = {}
	for row_dataset in reader:
		if row_num is 0:
			i = 0
			for col in row_dataset:
				dataset_header_index_lookup[col] = i
				i = i+1
		else:
			try:
				player_id = row_dataset[dataset_header_index_lookup['PlayerId']]
				player = rows[player_index_lookup[player_id]].find_all('td')
				actual_per_game = float(player[header_index_lookup[column]].text)/int(player[header_index_lookup['G']].text)
				predicted_per_game = float(row_dataset[dataset_header_index_lookup[column]])
				if actual_per_game > 0.0:
					error = abs(predicted_per_game - actual_per_game)/actual_per_game
				else:
					error = 1
				if error > error_margin:
					errnum = errnum+1
					print "{} {}: Actual: {} Expected: {}, Error: {}".format(player[header_index_lookup['Player']].text, player_id, actual_per_game, predicted_per_game, error)
			except KeyError:
				# For players that are not in the league anymore
				print "{} does not exist".format(row_dataset[dataset_header_index_lookup['PlayerId']])
		row_num = row_num+1

print "{}/{} Error Rate".format(errnum, row_num)

