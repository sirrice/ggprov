util = require 'ggutil'

class prov.PStore extends util.Graph
  @pstores = {}  # flow id -> op.id -> pstore instance

  # @param flowid id of wf.Flow instance
  # @param opid id of operator specific provstore.  
  #             null to fetch workflow level port->port pstore
  @get: (flow, op=null) ->
    flowid = null
    flowid = flow.id if flow?
    @pstores[flowid] = new prov.PStore flow unless flowid of @pstores

    if op?
      @pstores[flowid].get op
    else
      @pstores[flowid]


  constructor: (@flow) ->
    super()
    @opstores = {}
    @id (o) -> "op-#{o.id}"#JSON.stringify o

  get: (op) ->
    unless op.id of @opstores
      @opstores[op.id] = new prov.OPStore @flow, op 
    @opstores[op.id]

  query: (query) ->


