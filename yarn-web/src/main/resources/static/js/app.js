(function(){
    var app = angular.module("yarnJob", []);
    app.controller("AMController", ["$http", function($http){
        var master = this;
        master.status = "waiting for connection";
        master.memory = 256;
        master.cores = 1;
        master.containers = 1;
        master.info = {maxMem: 0, maxVCores: 0};
        master.taskStatus = {containersTotal: 0, containersCompleted: 0, inProgress: false};
        
        this.getInfo = function() {
            $http.get("/getInfo").then(function mySucces(response) {
                master.status = "connected";
                master.info = response.data;
            }, function myError(response) {
                master.status = "connection error";
            });
        };
        this.terminate = function() {
            $http.post("/terminate");
        };
        this.startTasks = function() {
            master.taskStatus.inProgress = true;
            $http.get("/startTasks", {
                params: {
                    memory:  master.memory,
                    cores:  master.cores,
                    containers: master.containers
                }
            });
        };
        this.getTaskStatus = function() {
            $http.get("/getTaskStatus").then(function mySucces(response) {
                master.taskStatus = response.data;
            });
        };
        master.getInfo();
        setInterval(this.getTaskStatus, 1000);
    }]);
})();