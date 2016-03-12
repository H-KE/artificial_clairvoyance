import glob, os

path = "../../../../app/resources/output/"

folder_list = ["mlb_players_current2",
				"mlb_players_historical2",
				"nba_players_current2",
				"nba_players_historical2"]

output_files  = ["mlb_players_current2.csv", 
"mlb_players_historical2.csv", 
"nba_players_current2.csv", 
"nba_players_historical2.csv"]


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
