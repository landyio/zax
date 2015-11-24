
var http = require('./http.js');
var gen = require('./gen.js');

var sleep = require('sleep')


function submit(session, ts, visitor, vars, targetVar, appId) {

  //
  // The main line is following: there 2 groups of users that may be
  // easily discriminated, one of them prefering variation #1, and the other --
  // #2
  // 

  // _DBG
  console.log("OLOLOL: ");

  var port = '8080';
  var verbose = true;

  var vid = gen.randomInt(vars.length);

  var start = {
    session:    '' + session,
    timestamp:  ts,
    identity:   visitor,
    variation:  vars[vid]
  };


  var ps = http.post('/app/' + appId + '/event/start', port, start, verbose);

  if (vid === targetVar) 
  {
    var finish = {
      session:    '' + session,
      timestamp:  ts
    };

    var pf = http.post('/app/' + appId + '/event/finish', port, finish, verbose);

    return Promise.all([ ps, pf ]);
  }

  return ps;
}

(function main() {
  'use strict';

  // Step #0: Sweep
  
  var appId = "1";

  var variations = [ "1", "2" ];

  // Step #1: Push app-conf

  function step_1() {

    console.log("========================================================")
    console.log("Step #1")
    console.log("========================================================")
    
    var appconf = {
      variations:  variations,
      descriptors: [ "browser", "os", "lang" ]
    }

    return http.post('/app/' + appId + '/control/create', '8081', appconf, true);

  }

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
    
  var N = 5;
  var M = 1;

  
  function step_2() {

    sleep.sleep(2);

    console.log("========================================================")
    console.log("Step #2")
    console.log("========================================================")

    var sampleA = gen.generate(props, weightsA, N);
    var sampleB = gen.generate(props, weightsB, N);

    var sid = 0;
    var ts = 0; 

    var ps = [];

    sampleA.forEach(function (visitor) {
      ps.push(
        submit(sid++, ts++, visitor, variations, 0 /* targetVar */, appId)
      );
    });

    sampleB.forEach(function (visitor) {
      ps.push(
        submit(sid++, ts++, visitor, variations, 1 /* targetVar */, appId)
      );
    });

    return Promise.all(ps);
  }

  // Step #3: Query prediction

  function step_3() {

    sleep.sleep(2);

    console.log("========================================================")
    console.log("Step #3")
    console.log("========================================================")

    var testSample = gen.generate(props, weightsB, M);

    var ps = [];

    testSample.forEach(function (visitor) {

      ps.push(
        http.post('/app/' + appId + '/event/predict', '8080', { identity: visitor }, true /* verbose */)
      );

    });

    return Promise.all(ps);
  }

  // Step #4: Trigger training

  function step_4() {

    // ...    

  }

  // ...

  step_1().then(function () {
    step_2().then(function () {
      step_3().then(function () {
        // #4
      })
    })
  });

})();

