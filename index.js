var pg = require('pg.js')
var copy = require('pg-copy-streams')
var fs = require('fs')
var crypto = require('crypto')
var base64 = require('base64-stream')
var PassThrough = require('stream').PassThrough

module.exports = Blobstore

function Blobstore(opts, cb) {
  if (!(this instanceof Blobstore)) return new Blobstore(opts, cb)
  if(!opts.url) throw new Error('Must specify database url')
  this.url = opts.url
  this.schema = opts.schema || 'blob'
  this.table = opts.table || 'blob'
}


Blobstore.prototype.createReadStream = function createReadStream(opts) {
  if(!opts.hash) throw new Error('You have to specify a hash key')
  var passthrough = new PassThrough
  var self = this
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect()
    var query = copy.to('COPY (SELECT value FROM ' + self.schema + '.' + self.table + ' WHERE key=\'' + opts.hash +'\') TO STDOUT (FORMAT text)')
    var stream = client.query(query)
    stream.on('end', function () {
      client.end()
    })
    stream.on('error', function (err) {
      console.error(err)
      client.end()
    })
    stream.pipe(base64.decode()).pipe(passthrough)
  })
  
  return passthrough
}

Blobstore.prototype.createWriteStream = function createWriteStream(cb) {
  var passthrough = new PassThrough
  var self = this
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect()
    var query = copy.from('COPY ' + self.schema + '.' + self.table + ' (key, value) FROM STDIN')
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
    passthrough.pipe(encode)
  })
  return passthrough
}

Blobstore.prototype.exists = function (metadata, cb) {
  throw new Error('Not implemented')
}

Blobstore.prototype.remove = function (metadata, cb) {
  throw new Error('Not implemented')
}

Blobstore.prototype._createTable = function (done) {
  var schema = 'CREATE SCHEMA IF NOT EXISTS ' + this.schema
  var table = 'CREATE TABLE IF NOT EXISTS ' + this.schema + '.' + this.table + '(value TEXT, key VARCHAR(256))'
  var client = new pg.Client(this.url)
  client.connect(function (err) {
    if(err) throw err
    client.query([schema, table].join(';'), function (err, result) {
      if(err) throw err
      done()
      client.end()
    })
  })
}