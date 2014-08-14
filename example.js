var blob = require('./')
var url = 'postgresql://localhost:5432/blob'
var store = new blob({url: url})

var fs = require('fs')
var file = fs.createReadStream('./index.js')

var save = store.createWriteStream(function (data) {
  console.log('saved ' + data.hash)
  var readStream = store.createReadStream(data)
  readStream.pipe(fs.createWriteStream('./index2.js'))
})

file.pipe(save)