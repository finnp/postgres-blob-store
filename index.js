var pg = require('pg')
var QueryStream = require('pg-query-stream')
// var copyFrom = require('pg-copy-streams').from

module.exports = Blobstore

function Blobstore(opts) {
  if (!(this instanceof Blobstore)) return new Blobstore(opts)
  if(!opts.url) throw new Error('Must specify database url')
  this.url = opts.url
  this.table = opts.table || 'blob'
}

Blobstore.prototype.createReadStream = function (opts) {
  // is there a cooler way to do this?
  if(!opts.hash) throw new Error('You have to specify a hash')
  var stream = new Transform
  stream._transform = function (a, enc, cb) { this.push(a); cb() }
  pg.connect(this.url, function (err, client, done) {
    var query = new QueryStream('SELECT $1 FROM $2', [opts.hash, this.table])
    client.query(query).pipe(stream).on('end', function () {
      done()
    })
  })
  return stream
}

//https://github.com/brianc/node-postgres/wiki/Client different way without callback?
Blobstore.prototype.createWriteStream = function (cb) {
  throw new Error('no implemented')
  var stream = new Transform
  stream._transform = function (a, enc, cb) { this.push(a); cb() }
  pg.connect(this.url, function (err, client, done) {
    var toDb = client.copyFrom('COPY keys (value, key) WITH BINARY FROM STDIN WITH CSV')
    toDb.on('close', function () {
      done()
      db({hash: 'TBD'})
    })
    stream.pipe(toDb).on('end', function () {
      toDb.write('\t')
    })
  })
  return stream
}

Blobstore.prototype.exists = function (metadata, cb) {
  // pg.connect(this.url, function (err, client, done) {
  //   ..
  // })
}

Blobstore.prototype.remove = function (metadata, cb) {
//  pg.connect()
}

// 
// pg.connect(opts.url, function(err, client, done) {
//   client.query('SELECT * FROM your_table', function(err, result) {
//     done();
//     if(err) return console.error(err);
//     console.log(result.rows);
//   })
// })