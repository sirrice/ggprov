require "../env"
vows = require "vows"
assert = require "assert"



suite = vows.describe "row.coffee"

Schema = data.Schema
Row = data.Row

suite.addBatch
  "row":
    topic: ->
      row = [0, 0, {}, "hi"]
      schema = Schema.fromJSON
        a: Schema.numeric
        b: Schema.numeric
        c: Schema.object
        d: Schema.ordinal

      new Row schema, row

    "has cols": (row) ->
      for col in ['a', 'b', 'c', 'd']
        assert row.has(col)

    "can project": (row) ->
      newrow = row.project ['a', 'b']
      assert newrow.has 'a'
      assert newrow.has 'b'
      assert.equal newrow.get('a'), 0
      assert.equal newrow.get('b'), 0
      assert.not newrow.has 'c'

    "@torow is the same": (row) ->
      row2 = data.Row.toRow 
        a: 0
        b: 0
        c: {}
        d: "hi"
      for col in ['a', 'b', 'c', 'd']
        assert.deepEqual row.get(col), row2.get(col)
        
suite.export module
