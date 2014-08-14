var pg = require('pg.js')
var copy = require('pg-copy-streams')
var fs = require('fs')
var crypto = require('crypto')
var base64 = require('base64-stream')

module.exports = Blobstore

function Blobstore(opts) {
  if (!(this instanceof Blobstore)) return new Blobstore(opts)
  if(!opts.url) throw new Error('Must specify database url')
  this.url = opts.url
  this.table = opts.table || 'blob'
  // create table in beginning
  // "CREATE SCHEMA blob; CREATE TABLE blob.blob (value text, key VARCHAR(256));"
}

Blobstore.prototype.createReadStream = function createReadStream(opts) {
  if(!opts.hash) throw new Error('You have to specify a hash key')
  var client = new pg.Client(this.url)
  client.connect()
  var query = copy.to('COPY (SELECT value FROM blob.text WHERE key=\'' + opts.hash +'\') TO STDOUT (FORMAT text)')
  var stream = client.query(query)
  stream.on('end', function () {
    client.end()
  })
  stream.on('error', function (err) {
    console.error(err)
    client.end()
  })
  return stream.pipe(base64.decode())
}

Blobstore.prototype.createWriteStream = function createWriteStream(cb) {
  var client = new pg.Client(this.url)
  client.connect()
  var query = copy.from('COPY blob.text (key, value) FROM STDIN')
  var stream = client.query(query)
  var randomKey = crypto.randomBytes(32).toString('hex')
  stream.write(randomKey)
  stream.write('\t')
  stream.on('end', function () {
    cb({hash: randomKey}) // note this is random right now!
    client.end()
  })
  var encode = base64.encode()
  encode.pipe(stream)
  return encode
}

Blobstore.prototype.exists = function (metadata, cb) {
  throw new Error('Not implemented')
}

Blobstore.prototype.remove = function (metadata, cb) {
  throw new Error('Not implemented')
}
