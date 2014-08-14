var blob = require('./')
var fs = require('fs')

var url = 'postgresql://localhost:5432/blob'

blob({url: url}, function (store) {
  var file = fs.createReadStream('./index.js')

  var save = store.createWriteStream(function (data) {
    console.log('saved ' + data.hash)
    var readStream = store.createReadStream(data)
    readStream.pipe(fs.createWriteStream('./index2.js'))
  })

  file.pipe(save)
})