KylinApp.service('ProjectModel',function(){

    this.projects = [];
    this.selectedProject =null;


    this.setSelectedProject = function(project) {
        if(this.projects.indexOf(project) > -1) {
            this.selectedProject = project;
        }
    };

    this.setProjects = function(projects){
        if(projects.length){
            this.projects = projects;
        }
    }

    this.getProjects = function(){
        return this.projects;
    }

})