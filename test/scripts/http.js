
var http  = require('http');
var https = require('https');


// Since certificate is bound to `zax.landy.io`, just 
// ignore it and allow unauthorized access
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

function request(host, method, path, port, data, succCallback, useHttps, verbose) {

  var options = {
    hostname: host,
    port:     port,
    path:     path,
    method:   method,
    headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data)
      }
  }

  return (useHttps ? https : http).request(options, function (r) {
    // _DBG
    if (verbose) {
      console.log("[RECV]: Status <", r.statusCode, ">");
      console.log("[RECV]: Headers {", JSON.stringify(r.headers), "}");
      console.log("[RECV]: Path /", path, "/");
    }

    r.setEncoding('utf8');

    r.on('error', function (error) {
      throw error;
    });

    r.on('data', function (chunk) {
      // _DBG
      if (verbose)
        console.log("[RECV]: ", chunk);
    });

    r.on('end', function () {
      succCallback();
    })
  });
}

exports.post = function post(host, path, port, data, useHttps, verbose) {
  var data = JSON.stringify(data);

  return new Promise(function (succCallback) {
    var req = request(host, 'POST', path, port, data, succCallback, useHttps, verbose);

    // _DBG
    console.log("[SENT]: ", data);

    req.write(data);
    req.end();
  });
}


exports.get = function get(host, path, port, data, useHttps, verbose) {
  return new Promise(function (succCallback) {
    request(host, 'GET', path, port, data, useHttps, verbose).end();
  });
}
