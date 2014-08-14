var abstractBlobTests = require('abstract-blob-store/tests')
var test = require('tape')
var blob = require('./')
var dbURL = process.env['TEST_DB'] || process.argv[2] || 'postgresql://localhost:5432/test'

var common = {
  setup: function (t, cb) {
    var store = blob({url: dbURL})
    cb(null, store)
  },
  teardown: function (t, store, blob, cb) {
    if(blob) store.remove(blob, cb)
    else cb()
  }
}

abstractBlobTests(test, common)