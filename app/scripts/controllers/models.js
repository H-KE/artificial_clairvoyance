'use strict';

angular.module('artificialClairvoyanceApp')
  .controller('ChartModalCtrl', function ($scope, $uibModalInstance, playerData, stat, statVal, filePaths) {
    console.log(filePaths);
    $scope.stat = stat;
    $scope.player = playerData["name"];

    var renderChart = function(chart, selector) {
      var parsedData = [];
      d3.csv(filePaths.matchedFile, function(matchData) {
        d3.csv(filePaths.historicalFile, function(historicalData) {
          var matchedPlayers = _.where(matchData, {CurrentPlayerId: playerData["playerId"]});
          matchedPlayers = _.pluck(matchedPlayers, 'SimilarPlayerId');
          matchedPlayers = _.uniq(matchedPlayers);

          var min_y = 9999;
          var max_y = 0;
          matchedPlayers.forEach(function(d, i) {
            var matchedPlayer = {
              key: d,
              strokeWidth: 1
            };

            var matchedPlayerData = _.where(historicalData, {PlayerId: d});
            var matchedPlayerParsedData = []

            matchedPlayerData.forEach(function(season) {
              matchedPlayerParsedData.push({x: parseFloat(season["Age"]), y: parseFloat(season[stat])});
            })

            matchedPlayer.values = matchedPlayerParsedData;
            parsedData.push(matchedPlayer);
          })

          var currentPlayer = {
            key: playerData["name"],
            strokeWidth: 4,
            values: []
          };
          var currentPlayerData = _.where(historicalData, {PlayerId: playerData["playerId"]});
          currentPlayerData.forEach(function(season) {
            currentPlayer.values.push({x: parseFloat(season["Age"]), y: parseFloat(season[stat])})
          })
          currentPlayer.values.push({x: parseFloat(playerData["age"]), y: parseFloat(statVal)})
          parsedData.push(currentPlayer);

          selector.datum(parsedData).call(chart);
          nv.utils.windowResize(chart.update);
        })
      })
    };

    var chart;
    nv.addGraph(function() {
      chart = nv.models.lineChart()
                .margin({left: 100})  //Adjust chart margins to give the x-axis some breathing room.
                .useInteractiveGuideline(true)
                .showLegend(false)       //Show the legend, allowing users to turn on/off line series.
                .showYAxis(true)        //Show the y-axis
                .showXAxis(true)        //Show the x-axis
      ;

      chart.xAxis     //Chart x-axis settings
          .axisLabel('Age')
          .tickFormat(d3.format(',r'));

      chart.yAxis     //Chart y-axis settings
          .axisLabel(stat)
          .tickFormat(d3.format('.02f'));

      /* Done setting the chart up? Time to render it!*/
      // var myData = sinAndCos();   //You need data...

      renderChart(chart,
                  d3.select('#chart_svg'));
    });
  })
  .controller('ModelCtrl', function ($scope, $uibModal, NgTableParams) {

    var dataset = [];
    var self = this;

    $scope.showChart = function(playerData, stat, statVal) {
      var matchedPlayersFile;
      var historicalDataFile;

      if ($scope.chartToggle.type === "MLB") {
        matchedPlayersFile = "resources/output/mlb_players_match.csv";
        historicalDataFile = "resources/output/mlb_players_historical.csv";
      } else if ($scope.chartToggle.type === "NBA") {
        matchedPlayersFile = "resources/output/nba_players_match.csv";
        historicalDataFile = "resources/output/nba_players_historical.csv";
      }

      var modalInstance = $uibModal.open({
        animation: true,
        templateUrl: 'views/chartModal.html',
        controller: 'ChartModalCtrl',
        size: 'lg',
        resolve: {
          playerData: function () {
            return playerData;
          },
          stat: function () {
            return stat;
          },
          statVal: function () {
            return statVal;
          },
          filePaths: function () {
            return {
              matchedFile: matchedPlayersFile,
              historicalFile: historicalDataFile
            };
          }
        }
      });

      modalInstance.result.then(function (selectedItem) {
        $scope.selected = selectedItem;
      }, function () {
        console.log('Modal dismissed at: ' + new Date());
      });
    };
    

    $scope.chartToggle = {
      type: "MLB"
    };

    $scope.$watch('chartToggle', function() {
      var dataset = [];
      if ($scope.chartToggle.type === "MLB") {
        d3.csv("resources/output/mlb_players_historical.csv", function(modelData) {
          d3.csv("resources/output/mlb_predictions.csv", function(predictedData) {
            var currentSeason = _.where(modelData, {Season: "2014"});
            currentSeason.forEach(function(d, i) {
              var prediction = _.findWhere(predictedData, {PlayerId: d["PlayerId"]});
              if (prediction !== undefined) {
                dataset.push({
                  name: d["Name"],
                  type: "Predicted",
                  cluster: d["Cluster"],
                  age: prediction["Age"],
                  hits: prediction["H"],
                  homeruns: prediction["HR"],
                  playerId: d["PlayerId"]
                })
                dataset.push({
                  name: d["Name"],
                  type: "Actual",
                  cluster: d["Cluster"],
                  age: d["Age"],
                  hits: d["H"],
                  homeruns: d["HR"],
                  playerId: d["PlayerId"]
                })
              }
            })

            self.tableParams = new NgTableParams({
              page: 1, // show first page
              count: 10, // count per page
              group: "playerId"
            },  {
              dataset: dataset
            });
          })
        });
      } else if ($scope.chartToggle.type === "NBA") {
        d3.csv("resources/output/nba_players_current.csv", function(playerData) {
            d3.csv("resources/output/nba_predictions.csv", function(predictedData) {
              playerData.forEach(function(d, i) {
                var prediction = _.findWhere(predictedData, {PlayerId: d["PlayerId"]});
                if (prediction !== undefined) {
                  dataset.push({
                    name: d["Name"],
                    type: "Predicted",
                    cluster: d["Cluster"],
                    age: prediction["Age"],
                    pts: prediction["PTS"],
                    tpm: prediction["3PM"],
                    blk: prediction["BLK"],
                    ft: prediction["FT%"],
                    ast: prediction["AST"],
                    fg: prediction["FG%"],
                    reb: prediction["REB"],
                    stl: prediction["STL"],
                    tov: prediction["TOV"],
                    playerId: d["PlayerId"]
                  })
                  dataset.push({
                    name: d["Name"],
                    type: "Actual",
                    cluster: d["Cluster"],
                    age: d["Age"],
                    pts: d["PTS"],
                    tpm: d["3PM"],
                    blk: d["BLK"],
                    ft: d["FT%"],
                    ast: d["AST"],
                    fg: d["FG%"],
                    reb: d["REB"],
                    stl: d["STL"],
                    tov: d["TOV"],
                    playerId: d["PlayerId"]
                  })
                }
              });

              self.tableParams = new NgTableParams({
                page: 1, // show first page
                count: 10, // count per page
                group: "name"
              },  {
                dataset: dataset
              });
            })
        });
      }
    }, true);

    

    $('#mlbTableToggle').on('click', function () {
      $scope.chartToggle.type = "MLB";
    })
    $('#nbaTableToggle').on('click', function () {
      $scope.chartToggle.type = "NBA";
    })

  });
