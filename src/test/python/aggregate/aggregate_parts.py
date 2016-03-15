import glob, os



#run in shell script after Artifical Clairvoyance runs
#Change path if want to run standalone
path = "app/resources/output/"

folder_list = ["mlb_players_current2",
			   "mlb_players_historical2",
			   "nba_players_current2",
			   "nba_players_historical2",
			   "mlb_players_predictions",
			   "nba_players_predictions"]

output_files  = ["mlb_players_match.csv",
				 "mlb_players_historical.csv",
				 "nba_players_match.csv",
				 "nba_players_historical.csv",
				 "mlb_predictions.csv",
				 "nba_predictions.csv"]


for folder, output_file in zip(folder_list, output_files):

	out = open(os.path.join(path,output_file), "w" )
	first_file = 1

	for file in os.listdir(path + folder):

		if file.startswith("part"):
			f = open(path+folder+"/" + file, "r")
			if first_file:
				first_file = 0
			else:
				next(f)

			for line in f:
				out.write(line)

			f.close()

	out.close()
