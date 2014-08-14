# postgres-blob-store

This module is a PostgresSQL implementation of [abstract-blob-store](https://github.com/maxogden/abstract-blob-store)
and passes its tests.

Right now it doesn't use `bytea`, but is saving the binary files in `text` format as `base64`
Strings.

It is very experimental, since I have no clue about PostgresSQL. Your can install it 
with `npm install postgres-blob-store`.

## Usage

Right now the usage looks like this, however if possible I would like to get
rid of the callback structure in the beginning.

```js
var blob = require('postgres-blob-store')
var fs = require('fs')

var url = 'postgresql://localhost:5432/blob'

var store = blob({url: url})
var file = fs.createReadStream('./index.js')

var save = store.createWriteStream(function (data) {
  console.log('saved ' + data.hash)
  var readStream = store.createReadStream(data)
  readStream.pipe(fs.createWriteStream('./index2.js'))
})

file.pipe(save)
```