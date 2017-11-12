#!/usr/bin/env groovy

node{
    stage('Prepare'){
        cleanWs()
        git(
        url: 'git@github.com:asardana/spark-financial-analysis.git',
        credentialsId: '8af17cba-4867-4f7d-be22-ea4f1bb16591',
        branch: 'master'
        )
    }
    stage('Build'){
        if(isUnix()){
        echo 'unix os'
        sh 'pwd'
        sh './gradlew clean build -xtest --info'
  }
  else{
    echo 'not a unix os'
  }
    }
}
