'use strict';

angular.module('artificialClairvoyanceApp')
  .controller('ModelCtrl', function ($scope, NgTableParams) {
      var dataset = [];
      var self = this;

      d3.csv("resources/output/mlb_playsers2014_models.csv", function(modelData) {
          modelData.forEach(function(d, i) {
              dataset.push({
                  name: d["playerID"],
                  intercept: d["intercept"],
                  weight: d["weight"]
              });
          });

          self.tableParams = new NgTableParams({
            page: 1, // show first page
            count: 10 // count per page
          },  {
            data: dataset
          });
          console.log(self.tableParams);
      });

  });
