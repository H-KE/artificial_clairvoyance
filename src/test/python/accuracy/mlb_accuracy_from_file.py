import mechanize
from bs4 import BeautifulSoup
import csv
import matplotlib

season = 2015
column = "HR"
file = "mlb_predictions.csv"

# We will compare results with data pulled directly from online
br = mechanize.Browser()
soup = BeautifulSoup(br.open("http://www.baseball-reference.com/leagues/MLB/" + str(season) + "-standard-batting.shtml"),  "html.parser")
table = soup.find('table', attrs={'id':'players_standard_batting'})

# Create a dictionary for finding the index of the column of interest (Rk, Name, Age, Tm, Lg, G, etc.)
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
                age = row_dataset[dataset_header_index_lookup['Age']]
                # Get all the player's data using the player_id as the lookup key
                player = rows[player_index_lookup[player_id]].find_all('td')
                actual_per_game = float(player[header_index_lookup[column]].text)#/int(player[header_index_lookup['G']].text)
                predicted_per_game = float(row_dataset[dataset_header_index_lookup[column]])
                #exit()
                if actual_per_game > 0.0:
                    error = abs(predicted_per_game - actual_per_game)/actual_per_game
                else:
                    error = 1
                if error > 0.1:
                    errnum = errnum+1
                    print "{} {} {}: Actual: {} Expected: {}, Error: {}".format(player[header_index_lookup['Name']].text.encode('utf-8'), player_id, age, actual_per_game, predicted_per_game, error)
            except KeyError:
                # For players that are not in the league anymore
                print "{} does not exist".format(row_dataset[dataset_header_index_lookup['PlayerId']])
        row_num = row_num+1

print "{}/{} Error Rate".format(errnum, row_num)

