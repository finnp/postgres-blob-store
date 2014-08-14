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
  var empty = true
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect()
    var query = copy.to('COPY (SELECT value FROM ' + self.schema + '.' + self.table + ' WHERE key=\'' + opts.hash +'\') TO STDOUT (FORMAT text)')
    var stream = client.query(query)
    stream.on('data', function () {
      empty = false
    })
    stream.on('end', function () {
      if(empty) passthrough.emit('error', new Error('does not exist'))
      client.end()
    })
    stream.on('error', function (err) {
      passthrough.emit('error', err)
      client.end()
    })
    stream.pipe(base64.decode()).pipe(passthrough)
  })
  
  return passthrough
}

Blobstore.prototype.createWriteStream = function createWriteStream(opts, cb) {
  if (typeof opts === 'function') return this.createWriteStream({}, opts)
    
  var passthrough = new PassThrough
  var self = this
  var size = 0
  var hash = crypto.createHash('sha1')
  var digested = null
  
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect()
    var query = copy.from('COPY ' + self.schema + '.' + self.table + ' (value, key) FROM STDIN')
    var stream = client.query(query)
    stream.on('end', function () {
      cb(null, {hash: digested, size: size}) // note this is random right now!
      client.end()
    })
    
    passthrough
      .on('data', function (chunk) {
        stream.write(chunk.toString('base64'))
        size += chunk.length
        hash.update(chunk)
      })
      .on('end', function () {
        digested = hash.digest('hex')
        stream.write('\t' + digested)
        stream.end()
      })
  })
  return passthrough
}

Blobstore.prototype.exists = function (metadata, cb) {
  var self = this
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect(function (err) {
      if(err) {
        cb(err, false)
        client.end()
      } else {
        client.query('SELECT COUNT(*) FROM ' + self.schema + '.' + self.table + ' WHERE key=\'' + metadata.hash + '\'', function (err, response) {
          if(err) {
            cb(err, false)
            client.end()
          } else {
            cb(null, response.rows[0].count !== '0')
            client.end()
          }
        })
      }
      
    })
  })
}

Blobstore.prototype.remove = function (metadata, cb) {
  var self = this
  cb = cb || function noop () {}
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect(function (err) {
      if(err) {
        cb(err, false)
        client.end()
      } else {
        client.query('DELETE FROM ' + self.schema + '.' + self.table + ' WHERE key=\'' + metadata.hash + '\'', function (err, response) {
          if(err) {
            cb(err, false)
            client.end()
          } else {
            cb(null, true)
            client.end()
          }
        })
      }
    })
  })
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