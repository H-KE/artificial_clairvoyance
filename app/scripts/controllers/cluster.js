'use strict';

angular.module('artificialClairvoyanceApp')
  .controller('ClusterCtrl', function ($scope) {

  	$scope.chartToggle = {
  		type: "MLB"
  	};
        // create the charts	
    var mlbChart;
    var nbaChart;

	/*
	 * Render a cluster chart based on input data and axis
     */
    
    function renderChart(chart, selector, centers_csvpath, players_csvpath, x_axis, y_axis) {
        var parsedData = [];

        d3.csv(centers_csvpath, function(data) {
            data.forEach(function(d, i) {
                d[x_axis] = +d[x_axis];
                d[y_axis] = +d[y_axis];

                parsedData.push({
                    key: "Cluster " + i,
                    values: [{
                        x: d[x_axis],
                        y: d[y_axis],
                        center: i,
                        size: 50,
                        shape: "circle"
                    }]
                });

            });
            d3.csv(players_csvpath, function(playerData) {
                playerData.forEach(function(d) {
                    d.cluster = +d.cluster;
                    d[x_axis] = +d[x_axis];
                    d[y_axis] = +d[y_axis];

                    parsedData[d.cluster].values.push({
                        x: d[x_axis],
                        y: d[y_axis],
                        name: d.player,
                        shape: "circle"
                    });
                });

                selector.datum(parsedData).call(chart);
                nv.utils.windowResize(chart.update);
            });
        });
    }

  	$scope.$watch('chartToggle.type', function() {
  		if ($scope.chartToggle.type === "MLB") {
  			nv.addGraph(function() {
	        mlbChart = nv.models.scatterChart()
	            .showDistX(true)
	            .showDistY(true)
	            .useVoronoi(true)
	            .color(d3.scale.category20().range())
	            .duration(300)
	            .showLegend(false)
	        ;
	        mlbChart.dispatch.on('renderEnd', function(){
	            console.log('render complete');
	        });
	        mlbChart.xAxis.tickFormat(d3.format('.02f'));
	        mlbChart.yAxis.tickFormat(d3.format('.02f'));
	        mlbChart.tooltip.contentGenerator(function(data) {
	            if (data.point.name) {
	                return data.point.name + ": " + data.point.x + ", " + data.point.y;
	            } else {
	                return "Cluster " + data.point.center + " Center: " + data.point.x + ", " + data.point.y;
	            }
	        });

	        renderChart(mlbChart, 
	               d3.select('#mlbclusters svg'), 
	               "resources/output/mlb_centers.csv", 
	               "resources/output/mlb_players2014.csv",
	               "hits", 
	               "homeruns");
    });
  		} else if ($scope.chartToggle.type === "NBA") {
  			nv.addGraph(function() {
	        var x_axis = $("#x_select").val();
	        var y_axis = $("#y_select").val();

	        nbaChart = nv.models.scatterChart()
	            .showDistX(true)
	            .showDistY(true)
	            .useVoronoi(true)
	            .color(d3.scale.category20().range())
	            .duration(300)
	            .showLegend(false)
	        ;
	        nbaChart.dispatch.on('renderEnd', function(){
	            console.log('render complete');
	        });
	        nbaChart.xAxis.tickFormat(d3.format('.02f'));
	        nbaChart.yAxis.tickFormat(d3.format('.02f'));

	        nbaChart.tooltip.contentGenerator(function(data) {
	            if (data.point.name) {
	                return data.point.name + ": " + data.point.x + ", " + data.point.y;
	            } else {
	                return "Cluster " + data.point.center + " Center: " + data.point.x + ", " + data.point.y;
	            }
	        });

	        renderChart(nbaChart, 
	               d3.select('#nbaclusters svg'), 
	               "resources/output/nba_centers.csv", 
	               "resources/output/nba_players2014.csv",
	               x_axis, 
	               y_axis);
    });
  		}
  	});

    /*
	 * Watch for changes in the selectors and render the chart accordingly 
     */

    $("#x_select").change(function() {
        renderChart(nbaChart, 
            d3.select('#nbaclusters svg'), 
                      "resources/output/nba_centers.csv", 
                      "resources/output/nba_players2014.csv",
                      $("#x_select").val(), 
                      $("#y_select").val()
            );
    });
    $("#y_select").change(function() {
        renderChart(nbaChart, 
            d3.select('#nbaclusters svg'), 
                      "resources/output/nba_centers.csv", 
                      "resources/output/nba_players2014.csv",
                      $("#x_select").val(), 
                      $("#y_select").val()
            );
    });

    $('#mlbToggle').on('click', function () {
    	$scope.chartToggle.type = "MLB";
  	})
    $('#nbaToggle').on('click', function () {
    	$scope.chartToggle.type = "NBA";
  	})
  });
