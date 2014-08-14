var blob = require('./')
var fs = require('fs')

var url = 'postgresql://localhost:5432/blob'

var store = blob({url: url})

var file = fs.createReadStream('./index.js')

var save = store.createWriteStream(function (data) {
  console.log('saved ' + data.hash)
  store.exists(data, function (err, exists) {
    if(exists) {
      console.log('data exists :)')
      var readStream = store.createReadStream(data)
      readStream.pipe(fs.createWriteStream('./index2.js'))
      store.remove(data, function (err, deleted) {
        store.exists(data, function (err, exists) {
          console.log(exists ? 'was not deleted' : 'was deleted')
        })
      }) 
    } else {
      console.log('not found')
    }
  })

})

file.pipe(save)