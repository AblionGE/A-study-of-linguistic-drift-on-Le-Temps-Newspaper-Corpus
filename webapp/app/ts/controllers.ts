/// <reference path="../../DefinitelyTyped/angularjs/angular.d.ts" />

'use strict';

/* Controllers */

declare var Graph:any;

interface Entry{
    id:number;
    name:string;
    display:string
}
interface Category{
    id:number;
    entries_id:number;
    name:string
    synonyms:string
}
interface Pair{
    min_year:number;
    mean_frequency:string;
}
interface Synonym{
    word:string;
    freqs:number[];
}
interface SynonymSelected {
    word:string
    checked:boolean
}
interface Scope{
  test: string;
  searchTextChange:(name:string)=>void;
  selectedItemChange:(name:Entry)=>void;
  onCategoryChosen:(id:number)=>void;
  entries:Entry[];
  synonyms:SynonymSelected[];
    buffer:{[name:string]:Synonym}
  entrySelected:Entry;
  checkboxClicked:(s:SynonymSelected)=>void;
  data:number[][];
    width:number;
    normalize:boolean;
    changeNormalize:()=>void;
    graphDisplayed:boolean;
}

angular.module('myApp.controllers', []).
  controller('MyCtrl1', ["$scope","$http","$q", function($scope:Scope, $http:ng.IHttpService,$q:ng.IQService) {
      $scope.width = 800;

      $scope.entries = [];
      $scope.synonyms = [];
      $scope.entrySelected = null;
      $scope.buffer = {};

      $scope.normalize = false;
      $scope.changeNormalize = () => {
          $scope.normalize = !$scope.normalize;
          redrawGraph();
      };
      $scope.graphDisplayed = false;

      $scope.searchTextChange = (newText:string) => {
          if(!$scope.entrySelected || newText !== $scope.entrySelected.display){
              $http.get("../data/entries.php?like=" + newText).success(function(entries:Entry[]) {
                  $scope.entries.length = 0;
                  Array.prototype.push.apply($scope.entries, entries);
              });
          } else {
              $scope.entries.length = 0;
          }
      }
      $scope.selectedItemChange = (item:Entry) => {
          $http.get("../data/synonyms.php?like=" + item.name).success(function(synonyms:string[]) {
              $scope.synonyms.length = 0
              var source = {word:item.name, checked:false};
              $scope.synonyms.push(source);
              Array.prototype.push.apply($scope.synonyms, synonyms.map(function(s:string){
                  return {word:s.trim(), checked:false};
              }));
          });
      };
      $scope.checkboxClicked = (s:SynonymSelected)=> {
          s.checked = !s.checked;
          if(!$scope.buffer[s.word]){
              $http.get("../data/stats.php?target="+s.word).success(function(synonyms:Synonym) {
                  $scope.buffer[s.word] = synonyms;
                  redrawGraph();
              });
          } else {
              redrawGraph();
          }

      };
       var redrawGraph = function () {
            $scope.data = [];
            var words:Synonym[] = [];
            var start = 1840;
            var step = 5;
            var labels = ['x']

            $scope.synonyms.forEach(function (base) {
                if (base.checked && $scope.buffer[base.word].freqs) {
                    labels.push(base.word);
                    words.push($scope.buffer[base.word]);
                }
            });

            //doesn't make sense to draw if there is no data
            if(words.length === 0){
                return;
            }
            $scope.graphDisplayed = true;

            for (var i = 0; i < 32; i++) {
                var sum = 0.;
                words.forEach(function (stats) {
                    sum += stats.freqs[i];
                });
                sum = (sum === 0 ? 1 : sum);
                var div = ($scope.normalize ? sum : 1);

                var year = start + i*step;
                var yearData = [year];
                words.forEach(function (stats) {
                    yearData.push(stats.freqs[i]/div);
                });
                $scope.data.push(yearData);
            }

            Graph.makeGraph("few", $scope.data, labels, true);
       };
    }])
  .controller('MyCtrl2', [function() {
  }]);