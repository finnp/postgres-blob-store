var pg = require('pg.js')
var copy = require('pg-copy-streams')
var fs = require('fs')
var crypto = require('crypto')
var base64 = require('base64-stream')
var PassThrough = require('stream').PassThrough
var lengthStream = require('length-stream')
var pgescape = require('pg-escape')

module.exports = Blobstore

function Blobstore(opts, cb) {
  if (!(this instanceof Blobstore)) return new Blobstore(opts, cb)
  if(!opts.url) throw new Error('Must specify database url')
  this.url = opts.url
  this.schema = opts.schema || 'blob'
  this.table = opts.table || 'blob'
}


Blobstore.prototype.createReadStream = function createReadStream(opts) {
  if(typeof opts === 'string') opts = {key: opts}
  if(!opts.key) throw new Error('You have to specify a key')
  var passthrough = new PassThrough
  var self = this
  var empty = true
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect()
    var query = copy.to(pgescape('COPY (SELECT value FROM %I.%I WHERE key=%L) TO STDOUT (FORMAT text)', self.schema, self.table, opts.key))
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
  if(typeof opts === 'string') opts = {key: opts}
    
  var passthrough = new PassThrough
  var self = this
  var size = 0
  var hash = crypto.createHash('sha1')
  var key = opts.key
  
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect()
    var query = copy.from(pgescape('COPY %I.%I (value, key) FROM STDIN', self.schema, self.table))
    var stream = client.query(query)
    stream.on('end', function () {
      cb(null, {key: key, size: size})
      client.end()
    })
    
    var encode = base64.encode()
    
    var lengthCount = lengthStream(function (s) {
      size = s
    })

  if(key) {
    stream.write('\t' + key)
    stream.end()
  } else {
    passthrough
      .pipe(lengthCount)
      .pipe(encode)
      .on('data', function (chunk) {
        stream.write(chunk)
        hash.update(chunk)
      })
      .on('end', function () {
        key = hash.digest('hex')
        stream.write('\t' + key)
        stream.end()
      })  
    }
  })
  return passthrough
}

Blobstore.prototype.exists = function (opts, cb) {
  if(typeof opts === 'string') opts = {key: opts}
  var self = this
  this._createTable(function () {
    var client = new pg.Client(self.url)
    client.connect(function (err) {
      if(err) {
        cb(err, false)
        client.end()
      } else {
        client.query(pgescape('SELECT COUNT(*) FROM %I.%I WHERE key=%L', self.schema, self.table, opts.key), function (err, response) {
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
        client.query(pgescape('DELETE FROM %I.%I WHERE key=%L', self.schema, self.table, metadata.key), function (err, response) {
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