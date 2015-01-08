'use strict';

KylinApp.controller('PageCtrl', function ($scope, $q, AccessService,$modal, $location, $rootScope, $routeParams, $http, UserService,ProjectService,SweetAlert,$cookieStore,$log) {

    $scope.header = {show: true};
    $scope.footer = {
        year: new Date().getFullYear(),
        version: Config.version
    };

    $scope.$on('$routeChangeSuccess', function ($event, current) {
        $scope.activeTab = current.tab;
        $scope.header.show = ($location.url() && $location.url().indexOf('/home') == -1);
    });

    $scope.config = Config;
    $scope.routeParams = $routeParams;
    $scope.angular = angular;
    $scope.userService = UserService;
    $scope.activeTab = "";

    // Set up common methods
    $scope.logout = function () {
        $scope.$emit('event:logoutRequest');
        $http.get(Config.service.base + 'j_spring_security_logout').success(function () {
            UserService.setCurUser({});
            $scope.username = $scope.password = null;
            $location.path('/login');

            console.debug("Logout Completed.");
        }).error(function () {
            UserService.setCurUser({});
            $scope.username = $scope.password = null;
            $location.path('/login');

            console.debug("Logout Completed.");
        });
        ;
    };

    Messenger.options = {
        extraClasses: 'messenger-fixed messenger-on-bottom messenger-on-right',
        theme: 'air'
    };

    $scope.getInt = function (ivalue) {
        return parseInt(ivalue);
    };

    $scope.getLength = function (obj) {
        if (!obj) {
            return 0;
        }
        var size = 0, key;
        for (key in obj) {
            if (obj.hasOwnProperty(key)) size++;
        }
        return size;
    };

    // common acl methods
    $scope.hasPermission = function (entity) {
        var curUser = UserService.getCurUser();
        if(!curUser){
            return curUser;
        }

        var hasPermission = false;
        var masks = [];
        for (var i = 1; i < arguments.length; i++) {
            if (arguments[i])
            {
                masks.push(arguments[i]);
            }
        }

        if (entity) {
            angular.forEach(entity.accessEntities, function (acessEntity, index) {
                if (masks.indexOf(acessEntity.permission.mask) != -1) {
                    if ((curUser.userDetails.username == acessEntity.sid.principal) || UserService.hasRole(acessEntity.sid.grantedAuthority)) {
                        hasPermission = true;
                    }
                }
            });
        }

        return hasPermission;
    };

    $scope.listAccess = function (entity, type) {
        var defer = $q.defer();

        entity.accessLoading = true;
        AccessService.list({type: type, uuid: entity.uuid}, function (accessEntities) {
            entity.accessLoading = false;
            entity.accessEntities = accessEntities;
            defer.resolve();
        });

        return defer.promise;
    };

    // Compute data size so as to auto convert to KB/MB/GB/TB)
    $scope.dataSize = function (data) {
        var size;
        if(data/1024/1024/1024/1024 >= 1){
            size = (data/1024/1024/1024/1024).toFixed(2) + ' TB';
        }else if(data/1024/1024/1024 >= 1){
            size = (data/1024/1024/1024).toFixed(2) + ' GB';
        }else if(data/1024/1024 >= 1) {
            size = (data/1024/1024).toFixed(2) + ' MB';
        }else {
            size = (data/1024).toFixed(2) + ' KB';
        }
        return size;
    };



    $scope.project = {
        projects:[],
        selectedProject: null
    };

    $scope.projectVisible = function(project){
        $log.info(project);
        return project!='-- Select All --';
    }

    ProjectService.list({}, function (projects) {
        angular.forEach(projects, function(project, index){
            $scope.project.projects.push(project.name);
        });
        $scope.project.projects = _.sortBy($scope.project.projects, function (i) { return i.toLowerCase(); });

        var absUrl = $location.absUrl();

        var projectInCookie = $cookieStore.get("project");
        if(absUrl.indexOf("/login")==-1){
            $scope.project.selectedProject=projectInCookie!=null?projectInCookie:null;
        }else{
            $scope.project.selectedProject=$scope.project.selectedProject!=null?$scope.project.selectedProject:projectInCookie!=null?projectInCookie:$scope.project.projects[0];
        }
    });

    $scope.toCreateProj = function () {
        $modal.open({
            templateUrl: 'project.html',
            controller: projCtrl,
            resolve: {
                projects: function () {
                    return null;
                },
                project: function(){
                    return null;
                },
                scope:function(){
                    return $scope;
                }
            }
        });
    };


    $scope.$watch('project.selectedProject', function (newValue, oldValue) {
        if(newValue!=oldValue){
            $log.log("project updated in page controller,from:"+oldValue+" To:"+newValue);
            $cookieStore.put("project",$scope.project.selectedProject);
        }

    });

});

var projCtrl = function ($scope, $modalInstance, ProjectService, MessageService, projects, project,SweetAlert,scope) {
    $scope.state = {
        isEdit: false,
        oldProjName: null
    };
    $scope.isEdit = false;
    $scope.proj = {name: '', description: ''};

    if (project)
    {
        $scope.state.isEdit = true;
        $scope.state.oldProjName = project.name;
        $scope.proj = project;
    }

    $scope.createOrUpdate = function () {
        if ($scope.state.isEdit)
        {

            var requestBody = {
                formerProjectName: $scope.state.oldProjName,
                newProjectName: $scope.proj.name,
                newDescription: $scope.proj.description
            };
            ProjectService.update({}, requestBody, function (newProj) {
                SweetAlert.swal('Success!', 'Project update successfully!', 'success');
                $modalInstance.dismiss('cancel');
            },function(e){
                if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : 'Failed to take action.';
                    SweetAlert.swal('Oops...', msg, 'error');
                }else{
                    SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
            });
        }
        else
        {
            ProjectService.save({}, $scope.proj, function (newProj) {
                SweetAlert.swal('Success!', 'New project created successfully!', 'success');
                $modalInstance.dismiss('cancel');
                if(projects) {
                    projects.push(newProj);
                }
                scope.project.projects.push(newProj.name);
            }, function(e){
                if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : 'Failed to take action.';
                    SweetAlert.swal('Oops...', msg, 'error');
                }else{
                    SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
            });
        }
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

};

