var pg = require('pg.js')
var copy = require('pg-copy-streams')
var fs = require('fs')
var crypto = require('crypto')

module.exports = Blobstore

function Blobstore(opts) {
  if (!(this instanceof Blobstore)) return new Blobstore(opts)
  if(!opts.url) throw new Error('Must specify database url')
  this.url = opts.url
  this.table = opts.table || 'blob'
}

Blobstore.prototype.createReadStream = function createReadStream(opts) {
  if(!opts.hash) throw new Error('You have to specify a hash key')
  var client = new pg.Client(this.url)
  client.connect()
  var query = copy.to('COPY (SELECT value FROM blob.blob WHERE key=\'' + opts.hash +'\') TO STDOUT (FORMAT text)')
  var stream = client.query(query)
  stream.on('end', function () {
    client.end()
  })
  stream.on('error', function (err) {
    console.error(err)
    client.end()
  })
  return stream
}

Blobstore.prototype.createWriteStream = function createWriteStream(cb) {
  var client = new pg.Client(this.url)
  client.connect()
  var query = copy.from('COPY blob.blob (key, value) FROM STDIN')
  var stream = client.query(query)
  var randomKey = crypto.randomBytes(32).toString('hex')
  stream.write(randomKey)
  stream.write('\t')
  stream.on('end', function () {
    cb({hash: randomKey}) // note this is random right now!
    client.end()
  })
  return stream
}
//example
// var test = createWriteStream(function (data) {
//   console.log(data.key)
// })
// test.write('myvalues')
// test.end()

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