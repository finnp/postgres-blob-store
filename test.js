var abstractBlobTests = require('abstract-blob-store/tests')
var test = require('tape')
var blob = require('./')

var common = {
  setup: function (t, cb) {
    var store = blob({url: 'postgresql://localhost:5432/blob'})
    cb(null, store)
  },
  teardown: function (t, store, blob, cb) {
    if(blob) store.remove(blob, cb)
    else cb()
  }
}

abstractBlobTests(test, common)