function printStackTrace() {
  var e = new Error('dummy');
  var stack = e.stack .replace(/^[^\(]+?[\n$]/gm, '')
                      .replace(/^\s+at\s+/gm, '')
                      .replace(/^Object.<anonymous>\s*\(/gm, '{anonymous}()@')
                      .split('\n');
  console.log(stack);
}

function assert(condition, message) {
    if (!condition) {
        printStackTrace();
        throw message || "Assertion failed";
    }
}


function upperBound(xs, x) {
  assert(xs.length > 0);

  if (xs[0] > x)
    return 0;

  if (xs[xs.length - 1] < x)
    return -1;

  var l = 0;
  var r = xs.length - 1;

  while (r - l > 1)
  {
    var m = (r + l) / 2 >>> 0;

    if (xs[m] <= x)
      l = m;
    else
      r = m;
  }

  return r;
}


function randomInt(N) {
  return Math.floor(Math.random() * N);
}

function weightedRandomInt(ws) {
  var sum = ws.reduce(function (p, c) { return p + c; }, 0);
  var lat = ws.reduce(function (p, c) { 
    if (p.length > 0) 
      p.push(p[p.length - 1] + c)
    else 
      p.push(c);

    return p;
  }, []);

  assert(lat[lat.length - 1] == sum);

  var r = randomInt(sum);
  var i = upperBound(lat, r);

  return i;
}

function generate(props, weights, N) {

  function randomlySelectFrom(arr, weights) {
    return arr[weightedRandomInt(weights)];
  }

  function generateAtRandom(props, weights) {
    var o = {};

    Object.keys(props).forEach(function (k) {
      assert(weights[k].length == props[k].length)

      o[k] = randomlySelectFrom(props[k], weights[k]);
    });

    return o;
  }

  assert(props.lenght == weights.length);
  
  var a = [];

  for (var i = 0; i < N; ++i)
  {
    a.push(generateAtRandom(props, weights));
  }

  return a;
}


//
// Event wrappers
// 

function generateEvent() {
  
}

exports.generate  = generate;
exports.randomInt = randomInt;
