
var http = require('./http.js');
var gen = require('./gen.js');

var sleep = require('sleep');


function submit(session, ts, visitor, vars, targetVar, appId) {

  //
  // The main line is following: there 2 groups of users that may be
  // easily discriminated, one of them prefering variation #1, and the other --
  // #2
  // 

  var port = '8080';

  var vid = gen.randomInt(vars.length);

  var start = {
    session:    '' + session,
    timestamp:  ts,
    identity:   visitor,
    variation:  vars[vid]
  };

  http.post('/app/' + appId + '/event/start', port, start);

  if (vid === targetVar) 
  {
    var finish = {
      session:    '' + session,
      timestamp:  ts
    };

    http.post('/app/' + appId + '/event/finish', port, finish);
  }
}

(function main() {
  'use strict';

  // Step #0: Sweep

  // clearDB();
  

  // Step #1: Push app-conf

  var appId = "1";

  var variations = [ "1", "2" ];

  var appconf = {
    variations:  variations,
    descriptors: [ "browser", "os", "lang" ]
  }

  http.post('/app/' + appId + '/control/create', '8081', appconf);
 

  // Step #2: Push events

  var props = { 
    browser:  [ "Safari 9.0", "Chrome 39.1", "Firefox 10" ],
    os:       [ "Linux", "Windows", "Mac OS X", "iOS" ],
    lang:     [ "ru_RU", "en_US", "en_GB", "de_DE" ]
  };

  var weightsA = {
    browser:  [ 10, 65, 25 ],
    os:       [ 1, 70, 20, 8 ],
    lang:     [ 7, 60, 5, 5 ]
  }

  var weightsB = {
    browser:  [ 25, 10, 65 ],
    os:       [ 8, 1, 70, 20 ],
    lang:     [ 5, 5, 7, 60 ]
  }

  var N = 10;

  var sampleA = gen.generate(props, weightsA, N);
  var sampleB = gen.generate(props, weightsB, N);

  var sid = 0;
  var ts = 0; 

  sampleA.forEach(function (visitor) {
    submit(sid++, ts++, visitor, variations, 0 /* targetVar */, appId);
  });

  sampleB.forEach(function (visitor) {
    submit(sid++, ts++, visitor, variations, 1 /* targetVar */, appId);
  });


  // Step #3: Query prediction

  sleep.sleep(2);

  var testSample = gen.generate(props, weightsB, N);

  testSample.forEach(function (visitor) {

    http.post('/app/' + appId + '/event/predict', '8080', { identity: visitor }, true /* verbose */);

  });


  // Step #4: Trigger training


})();

