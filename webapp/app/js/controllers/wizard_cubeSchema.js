'use strict';

KylinApp.controller('WizardCubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService,SweetAlert,WizardHandler) {
        $scope.finished = function() {
            alert("Wizard finished :)");
        }

        $scope.logStep = function() {
            console.log("Step continued");
        }

        $scope.goBack = function() {
            WizardHandler.wizard().goTo(0);
        }
});
