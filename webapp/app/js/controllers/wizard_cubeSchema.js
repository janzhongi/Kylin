
KylinApp.controller('WizardCubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService,SweetAlert,WizardHandler,$log) {
    //~ Define metadata & class
    $scope.projects = [];
    $scope.newDimension = null;
    $scope.newMeasure = null;
    $scope.curWizardStep = 0;


        $scope.logStep = function() {
            console.log("Step continued");
        }

        $scope.goBack = function() {
            WizardHandler.wizard().goTo(0);
        }

    var Dimension = {
        createNew: function () {
            var dimension = {
                "id": "",
                "name": "",
                "table": "",
                "column": "",
                "datatype": "",
                "derived": [],
                "join": {
                    "type": "",
                    "primary_key": [],
                    "foreign_key": []
                },
                "hierarchy": [],
                "status": {
                    "joinCount": 1,
                    "hierarchyCount": 1,
                    "useHierarchy": false,
                    "useJoin": false
                }
            };

            return dimension;
        }
    };

    var Measure = {
        createNew: function () {
            var measure = {
                "id": "",
                "name": "",
                "function": {
                    "expression": "",
                    "returntype": "",
                    "parameter": {
                        "type": "",
                        "value": ""
                    }
                }
            };

            return measure;
        }
    };

    // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

    $scope.$watch('cube.detail', function (newValue, oldValue) {
        if (newValue) {
            $scope.cubeMetaFrame = newValue;
        }
    });

    $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
        if ($scope.cubeMode=="editExistCube"&&newValue && !newValue.project) {
            initProject();
            generateCubeStatus($scope.cubeMetaFrame);
        }
    });


    // ~ public methods
    $scope.filterProj = function(project){
        return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project,$scope.permissions.ADMINISTRATION.mask);
    }

    $scope.addNewDimension = function (dimension) {
        $scope.newDimension = (!!dimension)? dimension: Dimension.createNew();
        if(!$scope.newDimension.join){
            $scope.newDimension.join = { "type": "","primary_key": [],"foreign_key": []}
        }
        if($scope.newDimension.status&&$scope.newDimension.status.useJoin||$scope.newDimension.join.foreign_key.length!=0){
            $scope.newDimension.status.useJoin = true;
        }
    }

    $scope.clearNewDimension = function () {
        $scope.newDimension = null;
    }

    $scope.saveNewDimension = function () {
        if($scope.cubeMetaFrame.fact_table==$scope.newDimension.table&&$scope.newDimension.derived.length){
            SweetAlert.swal('', "Derived column can only be defined on lookup  table!", 'info');
            return;
        }
        if($scope.editFlag.dimensionEdited=="init"){
            $scope.editFlag.dimensionEdited = false;
        }else{
            $scope.editFlag.dimensionEdited=!$scope.editFlag.dimensionEdited;
        }

        if ($scope.cubeMetaFrame.dimensions.indexOf($scope.newDimension) === -1) {
            $scope.cubeMetaFrame.dimensions.push($scope.newDimension);
        }
        $scope.newDimension = null;
    }

    $scope.addNewMeasure = function (measure) {
        $scope.newMeasure = (!!measure)? measure:Measure.createNew();
    }

    $scope.clearNewMeasure = function () {
        $scope.newMeasure = null;
    }

    $scope.saveNewMeasure = function () {
        if ($scope.cubeMetaFrame.measures.indexOf($scope.newMeasure) === -1) {
            $scope.cubeMetaFrame.measures.push($scope.newMeasure);
        }
        $scope.newMeasure = null;
    }

    $scope.addNewRowkeyColumn = function () {
        $scope.cubeMetaFrame.rowkey.rowkey_columns.push({
            "column": "",
            "length": 0,
            "dictionary": "true",
            "mandatory": false
        });
    }

    $scope.addNewAggregationGroup = function () {
        $scope.cubeMetaFrame.rowkey.aggregation_groups.push([]);
    }

    $scope.refreshAggregationGroup = function (list, index, aggregation_groups) {
        if (aggregation_groups) {
            list[index] = aggregation_groups;
        }
    }

    $scope.addNewHierarchy = function (dimension) {
        if (!dimension.hierarchy) {
            dimension.hierarchy = [];
        }
        dimension.hierarchy.push({
            "level": (dimension.hierarchy.length + 1),
            "column": undefined
        });
    }

    $scope.addNewDerived = function (dimension) {

        if($scope.cubeMetaFrame.fact_table==dimension.table){
            SweetAlert.swal('', "Derived column can only be defined on lookup  table!", 'info');
            return;
        }

        if(!dimension.derived){
            dimension.derived = [];
        }
        dimension.derived.push("");
    }

    $scope.toggleJoin = function (dimension,$event) {
        if (dimension.join&&dimension.join.type!='') {
            if(!confirm('Delete the join relation?')){
                $event.preventDefault();
                return;
            }else{
                delete dimension.join;
            }
        }
        else {
            dimension.join = dimension.join==undefined?{}:dimension.join;
            dimension.join.type = 'left';
        }
    }

    $scope.toggleHierarchy = function (dimension) {
        if (dimension.status.useHierarchy) {
            dimension.hierarchy = [];
        }
    }

    $scope.removeElement = function (arr, element) {
        var index = arr.indexOf(element);
        if (index > -1) {
            arr.splice(index, 1);
        }
    }

    $scope.removeDimension = function (arr, element) {
        var index = arr.indexOf(element);
        if (index > -1) {
            arr.splice(index, 1);
            if($scope.editFlag.dimensionEdited=="init"){
                $scope.editFlag.dimensionEdited = false;
            }else{
                $scope.editFlag.dimensionEdited=!$scope.editFlag.dimensionEdited;
            }
        }
    }

    $scope.open = function ($event) {
        $event.preventDefault();
        $event.stopPropagation();

        $scope.opened = true;
    };

    $scope.preView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex >= 1) {
            $scope.curStep.isComplete = false;
            $scope.curStep = $scope.wizardSteps[stepIndex - 1];
        }
    }

    $scope.nextView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex < ($scope.wizardSteps.length - 1)) {
            $scope.curStep.isComplete = true;
            $scope.curStep = $scope.wizardSteps[stepIndex + 1];

            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
            });
        }
    }

    $scope.getJoinToolTip = function (dimension) {
        var tooltip = "";

        if (dimension.join) {
            angular.forEach(dimension.join.primary_key, function (pk, index) {
                tooltip += (pk + " = " + dimension.join.foreign_key[index] + "<br>");
            });
        }

        return tooltip;
    }


    $scope.forms = {};
    $scope.cubeValidate={
        dimension:{
            empty:false
        }
    }

    // wizard validateion

    $scope.validateCubeInfo = function(){


        $log.info("validate cube info:"+$scope.forms);
        var cube_name = $scope.cubeMetaFrame.name;
        if($scope.forms.cubeInfoForm.$invalid){
            $scope.forms.cubeInfoForm.cube_name.$dirty = true;
            return false;
        }
        return true;

    }

    $scope.validateDemensions = function(){

        var factTableValid = true;
        var dimensionLengthValid = true;
        if(!$scope.cubeMetaFrame.dimensions.length){
            dimensionLengthValid =  false;
        }

        if($scope.forms.dimensionForm.$invalid){
            $scope.forms.dimensionForm.factTable.$dirty = true;
            if($scope.forms.dimensionForm.dimensionTable){
                $scope.forms.dimensionForm.dimensionTable.$dirty = true;
                $scope.forms.dimensionForm.dim_name.$dirty = true;
            }
            factTableValid =  false;
        }
        return dimensionLengthValid&&factTableValid;
    }

    $scope.validateMeasures = function(){
        var measureLengthValid = true;
        if(!$scope.cubeMetaFrame.measures.length){
            measureLengthValid =  false;
        }
        return measureLengthValid;
    }

    $scope.validateFilter = function(){
        return true;
    }

    $scope.validateRefreshSetting = function(){
        return true;
    }

    $scope.validateAdvancedSetting = function(){
        return true;
    }


    // ~ private methods
    function initProject() {
        ProjectService.list({}, function (projects) {
            $scope.projects = projects;

            var cubeName = (!!$scope.routeParams.cubeName)? $scope.routeParams.cubeName:$scope.state.cubeName;
            if (cubeName) {
                var projName = null;
                angular.forEach($scope.projects, function (project, index) {
                    angular.forEach(project.cubes, function (cube, index) {
                        if (!projName && cube === cubeName.toUpperCase()) {
                            projName = project.name;
                        }
                    });
                });

                $scope.cubeMetaFrame.project = projName;
            }

            angular.forEach($scope.projects, function (project, index) {
                $scope.listAccess(project, 'ProjectInstance');
            });
        });
    }

    function generateCubeStatus(cubeMeta) {
        angular.forEach(cubeMeta.dimensions, function (dimension, index) {
            dimension.status = {};
            if (dimension.hierarchy) {
                dimension.status.useHierarchy = true;
                dimension.status.joinCount = (!!dimension.join.primary_key) ? dimension.join.primary_key.length : 0;
                dimension.status.hierarchyCount = (!!dimension.hierarchy) ? dimension.hierarchy.length : 0;
            }
        });
    }


});
