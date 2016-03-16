import mechanize
from bs4 import BeautifulSoup
import csv
import matplotlib

season = 2015
column = "HR" #target variable
file = "../../../../app/resources/output/mlb_predictions.csv"
actualfile = "../../../../app/resources/output/mlb_players_current.csv"

actualdata = open(actualfile, 'r')
actual = csv.reader(actualdata)

row_num = 0
header_index_lookup = {}
first = next(actual)
i = 0
for col in first:
    header_index_lookup[col] = i
    i = i + 1

player_stats_lookup = {}
i = 0
for row in actual:
    if row != 0:
        player_stats_lookup[row[0]] = row
        i = i+1

row_num = 0
noexist = 0
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
                # Get all the player's data using the player_id as the lookup key
                stats = player_stats_lookup[player_id]

                actual_per_game = float(stats[header_index_lookup[column]])
                predicted_per_game = float(row_dataset[dataset_header_index_lookup[column]])

                if actual_per_game > 0.0:
                    error = abs(predicted_per_game - actual_per_game)/actual_per_game
                else:
                    error = 1
                if error > 0.1:
                    errnum = errnum+1
                    print "{} {}: Actual: {} Expected: {}, Error: {}".format(stats[header_index_lookup['Name']], player_id, actual_per_game, predicted_per_game, error)
            except KeyError:
                # For players that are not in the league anymore
                print "{} does not exist".format(row_dataset[dataset_header_index_lookup['PlayerId']])
                noexist = noexist + 1
        row_num = row_num+1

print "{}/{} Error Rate".format(errnum, row_num-noexist-1)

