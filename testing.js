var blob = require('./')
var url = 'postgresql://localhost:5432/blob'
var store = new blob({url: url})

var fs = require('fs')
var file = fs.createReadStream('./example.js')

var save = store.createWriteStream(function (data) {
  console.log('saved ' + data.hash)
  var readStream = store.createReadStream(data)
  readStream.pipe(fs.createWriteStream('./blablub.js'))
})

file.pipe(save)

// save.write('sdfsdfdf')
// save.end()

// var pg = require('pg.js')
// var copy = require('pg-copy-streams')
// var fs = require('fs')
// var crypto = require('crypto')
// 
// var url = 'postgresql://localhost:5432/blob'
// 
// // SELECT DATA
// 
// 
// 
// 
// function createReadStream(opts) {
//   if(!opts.hash) throw new Error('You have to specify a hash key')
//   var client = new pg.Client(url)
//   client.connect()
//   var query = copy.to('COPY (SELECT value FROM blob.blob WHERE key=\'' + opts.hash +'\') TO STDOUT (FORMAT text)')
//   var stream = client.query(query)
//   stream.on('end', function () {
//     client.end()
//   })
//   stream.on('error', function (err) {
//     console.error(err)
//     client.end()
//   })
//   return stream
// }
// 
// function createWriteStream(cb) {
//   var client = new pg.Client(url)
//   client.connect()
//   var query = copy.from('COPY blob.blob (key, value) FROM STDIN')
//   var stream = client.query(query)
//   var randomKey = crypto.randomBytes(32).toString('hex')
//   stream.write(randomKey)
//   stream.write('\t')
//   stream.on('end', function () {
//     cb({hash: randomKey}) // note this is random right now!
//     client.end()
//   })
//   return stream
// }
// 
// var test = createWriteStream(function (data) {
//   // console.log(data.hash)
//   createReadStream(data).pipe(process.stdout)
// })
// test.write(new Buffer('myvalues'))
// test.end()

// pg.connect(url, function (err, client, done) {
//   var stream = copy.from('COPY blob.blob FROM STDIN')
//   var testFile = fs.createReadStream('./testtest.csv')
//   testFile.pipe(process.stdout)
//   testFile.pipe(stream).on('end', function () {
//     console.log('wat')
//   }).on('error', function () {
//     console.error('what')
//   })

  // var testFile = fs.createReadStream('./index.js')
  // testFile.pipe(stream)
  // testFile.on('end', function () {
  //   console.log('over?')
  //   client.end()
  // })
  // testFile.on('error', function (err) {
  //   console.error(err)
  //   client.end()
  // })
// })


// "CREATE SCHEMA blob; CREATE TABLE blob.blob (value BYTEA, key VARCHAR(256));"