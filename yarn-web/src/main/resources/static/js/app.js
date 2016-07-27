(function(){
    var app = angular.module("yarnJob", ["ngRoute"]);
    app.controller("AMController", ["$http", "$route", function($http, $route){
        var master = this;
        master.status = "123";
        this.ping = function() {
            $http.get("/ping").then(function mySucces(response) {
                master.status = response.data.status;
            }, function myError(response) {
                master.status = "connection error - " + response.statusText;
            });
        };
        this.terminate = function() {
            $http.get("/terminate").then(function mySucces(response) {
                $route.reload();
            });
        };
        this.sort = function() {
            $http.get("/sort");
        };
    }]);
})();