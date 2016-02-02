require "./env"
_ = require "underscore"
assert = require "assert"

print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()
  console.log "timings: #{t1.timings()}"
  console.log "\n"



rows = _.times 10, (i) -> { a: i%2, x: i, y: i, b: i%5}
t = data.fromArray rows, null, 'col'

t = t.project [
  {
    alias: 'foo'
    f: (x,y) -> x * y + 100000
    cols: ['x', 'y']
  }
  {
    alias: 'bar'
    f: (x) -> new Date("2013/#{x}/01")
    cols: 'x'
    }
  {
    alias: 'baz'
    f: (x) -> "#{x}"
    cols: 'x'
  }
  {
    alias: 'tam'
    f: (x) -> ["#{x}"]
    cols: 'x'
  }
]

t = t.partition 'b'

p = prov.Prov.get()
p.addView t

console.log p.ids [t.any()]

