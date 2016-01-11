'use strict';

/**
 * @ngdoc overview
 * @name artificialClairvoyanceApp
 * @description
 * # artificialClairvoyanceApp
 *
 * Main module of the application.
 */
angular
  .module('artificialClairvoyanceApp', [
    'ngAnimate',
    'ngCookies',
    'ngResource',
    'ngRoute',
    'ngSanitize',
    'ngTouch'
  ])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/main.html',
        controller: 'MainCtrl',
        controllerAs: 'main'
      })
      .when('/clusters', {
        templateUrl: 'views/clusters.html',
        controllerAs: 'cluster'
      })
      .when('/about', {
        templateUrl: 'views/about.html',
        controller: 'AboutCtrl',
        controllerAs: 'about'
      })
      .otherwise({
        redirectTo: '/'
      });
  });
