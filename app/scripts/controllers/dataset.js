'use strict';

angular.module('artificialClairvoyanceApp')
  .controller('DataCtrl', function ($scope, NgTableParams) {
      var dataset = [];
      var self = this;
      var cols = {
        "fg": "Field Goals", 
        "3pm": "3-point Field Goals", 
        "ft": "Free Throws", 
        "reb": "Rebounds", 
        "ast": "Assists", 
        "stl": "Steals", 
        "blk": "Blocks", 
        "tov": "Turnovers", 
        "pts": "Points"
      };

      var col_keys = Object.keys(cols);
      console.log(col_keys);

      d3.csv("resources/output/nba_players_historical.csv", function(playerData) {
          var players = _.pluck(playerData, "playerId");
          var seasonsByPlayer = {};
          var count = 0;

          playerData.forEach(function(d) {
            if (!seasonsByPlayer.hasOwnProperty(d.playerId)) {
              seasonsByPlayer[d.playerId] = [];
            }
            seasonsByPlayer[d.playerId].push(d);
          })
          console.log(seasonsByPlayer);

          players.forEach(function(player) {
            col_keys.forEach(function(stat) {
              var stats = {
                "stat": cols[stat]
              }
              seasonsByPlayer[player].forEach(function(season, i) {
                stats["name"] = season.player;
                stats["id"] = season.playerId + stat + count;
                stats["age" + season.age] = season[stat];
                count ++;
              })
              for (var i = 20; i <= 36; i ++) {
                if (!stats.hasOwnProperty("age" + i)) {
                  stats["age" + i] = "-";
                }
              }
              dataset.push(stats);
            })

          });

          console.log(dataset);



          self.tableParams = new NgTableParams({
            page: 1, // show first page
            count: 50, // count per page
          },  {
            data: dataset
          });
      })

  });
