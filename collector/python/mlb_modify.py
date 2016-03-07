import csv

players = {}
with open('src/test/resources/lahman-csv_2015-01-24/Master.csv', 'r') as master:
    reader = csv.reader(master)
    row_number = 0

    for row in reader:
        if row_number != 0:
            if row[1] != '':
                players[row[0]] = int(row[1])
        row_number += 1

with open('src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv', 'w') as out:
    with open('src/test/resources/lahman-csv_2015-01-24/Batting.csv', 'r') as batting:
        writer = csv.writer(out, delimiter=',')
        reader = csv.reader(batting)
        row_number = 0

        for row in reader:
            if row_number == 0:
                row.append('age')
                writer.writerow(row)
            else:
                if row[0] in players:
                    age = int(row[1]) - players[row[0]]
                    row.append(age)
                    writer.writerow(row)
            row_number += 1