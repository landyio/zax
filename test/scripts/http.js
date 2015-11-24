
var http = require('http');


function request(method, path, port, data, succCallback, verbose) {

  var options = {
    hostname: 'localhost',
    port:     port,
    path:     path,
    method:   method,
    headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data)
      }
  }

  return http.request(options, function (r) {
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

exports.post = function post(path, port, data, verbose) {
  var data = JSON.stringify(data);

  return new Promise(function (succCallback) {
    var req = request('POST', path, port, data, succCallback, verbose);

    // _DBG
    console.log("[SENT]: ", data);

    req.write(data);
    req.end();
  });
}


exports.get = function get(path, port, data, verbose) {
  return new Promise(function (succCallback) {
    request('GET', path, port, data, verbose).end();
  });
}
