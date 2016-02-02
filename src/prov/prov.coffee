_ = require 'underscore'

class prov.Prov extends util.Graph
  @pstores = {}  # flow id -> op.id -> pstore instance

  # @param flowid id of wf.Flow instance
  # @param opid id of operator specific provstore.  
  #             null to fetch workflow level port->port pstore
  @get: (name=null) ->
    name = String name
    unless name of @pstores
      @pstores[name] = new prov.Prov name, (t)->
        if t?
          if t.id?
            t.id 
          else if _.isString(t) or _.isNumber(t)
            String(t)
          else
            console.log t
            throw Error "prov store expects objects with IDs"
        else
          t

    @pstores[name]

  constructor: (@name, @idFunc) ->
    super @idFunc
    @tag2id = {}
    @id2tag = {}
    @badtables = {}


  rm: (node) ->
    super

    id = @idFunc node
    tags = @id2tag[id]
    delete @id2tag[id]
    for tag in tags
      delete @tags[tag][id]

  # tags are like relations or roots (in PQL)
  tag: (node, tag) ->
    @add node
    id = @idFunc node
    @id2tag[id] = {} unless id of @id2tag

    if tag?
      @tag2id[tag] = {} unless tag of @tag2id
      @tag2id[tag][id] = yes
      @id2tag[id][tag] = yes
    _.keys @id2tag[id]

  nodesByTag: (tag) ->
    ids = _.keys(@tag2id[tag] or {})
    _.compact ids.map (id) => @id2node[id] if id of @id2node

  isTag: (node, tag) ->
    id = @idFunc node
    if @id2tag[id]?
      tag in @id2tag[id]
    else
      no

  backward: (outputs, type, userf=null) ->
    bads = @badtables
    ret = {} 
    userf ?= (node) => @parents(node, type).length == 0
    f = (node, path) =>
      return yes if @idFunc(node) of bads
      if userf(node, path)
        ret[@idFunc node] = node
    @dfs f, outputs, type, (n) =>
      @parents n, type
    _.values ret

  forward: (inputs, type, userf=null) ->
    bads = @badtables
    ret = {}
    userf ?= (node) => @children(node, type).length == 0
    f = (node, path) =>
      return yes if @idFunc(node) of bads
      if userf(node, path)
        ret[@idFunc node] = node
    @dfs f, inputs, type
    _.values ret

  shared: (outputs, endnodes, type) ->
    outputs = _.flatten [outputs]
    endnodes = _.flatten [endnodes]
    at2os = @paths at, outputs, type
    at2ends = @paths at, endnodes, type

  # given objects at granularity G
  #  with certain subgraph properties
  # want objects at granularity G'
  #  with certain subgraph properties
  #
  # a) that share a path to a set of nodes 
  #    with certain subgraph properties
  #
  
    

  # NOTE: assumes from is an ancestor of to!
  # @param from a node or list of nodes in the graph
  # @param to a node or list of nodes we are searching for
  paths: (from, to, type) ->
    paths = []
    to = _.compact _.flatten [to]
    toids = to.map (n) => @idFunc n
    f = (node, path) =>
      if toids.length == 0 
        if @children(node, type).length == 0
          paths.push _.clone path
      else if @idFunc(node) in toids
        paths.push _.clone path
      yes
    @dfs f, from, type
    paths

  clone: -> @

  roots: ->
    @nodesByTag('table').filter (n) =>
      @parents(n, 'table').length == 0

  ids: (rows) ->
    _.flatten rows.map (d) -> d.prov()

  data: (geoms) ->
    _.flatten geoms.map (g) -> g.data()

  lookup: (geoms) ->
    @lookupByIds @ids(geoms)

  lookupByIds: (ids) ->
    ret = []
    for root in @roots()
      filtered = root.filter (row) -> row.id in ids
      filtered.map (row) -> ret.push row.shallowClone()
    ret


  done: ->
    @badtables = @badTables
  
  # @return hashtable of bad table ids { tableid -> yes }
  badTables: ->
    tables = @nodesByTag 'table'
    leaftables = tables.filter (t) => @children(t, 'table').length == 0
    bad = {}
    while leaftables.length > 0
      leaf = leaftables.pop()
      tid = @idFunc leaf
      if (
        (@parents(leaf, 'input').length == 0) and 
        (@parents(leaf, 'output').length == 0) and
        _.difference(@children(leaf, 'table').map((n)->n.id), _.keys(bad)).length == 0)
        bad[tid] = yes
        for p in @parents(leaf, 'table')
          leaftables.push p
    return bad 

  prune: ->
    for bt in @badTables()
      @rm bt

  # Export graph is a DOT formatted string
  toDot: (rankdir='TD') ->
    badtables = @badTables()
    text = []
    text.push "digraph G {"
    text.push "graph [rankdir=#{rankdir}]"
    _.each @edges(), (edge) =>
      [n1, n2, type, weight] = edge
      if 'table' in @tag(n1)
        if @idFunc(n1) of badtables
          return

      color = switch type
        when 'output' then 'orange'
        when 'table' then 'green'
        when 'wf' then 'black'
        when 'input' then 'red'
        else 'grey'
      if color is 'grey'
        return
      text.push "\"#{n1.name or '?'}:#{n1.id}\" -> \"#{n2.name or '?'}:#{n2.id}\" [color=\"#{color}\", label=\"#{type}\"];"
    text.push "}"
    text.join("\n")

  isTableUsed: (t) ->
    ret = no
    seen = {}
    f = (n) =>
      seen[@idFunc(n)] = yes
      ps = @parents(n, 'input')
      if ps.length > 0 
        ret = yes
    @dfs f, t, 'table'

    [ret, _.keys(seen)]
    


  ###
  # copied 
  #
  # find the columns finalcol depends on.
  # a depends on cols if the cols contains a
  #
  # @param finalcol the column to compute the provenance for
  colProv: (finalcol, targetTableName=null) ->
    lookup = {}
    lookup[@id] = [finalcol]
    res = data.util.Traverse.bfs @, (node) ->
      return if node.children().length == 0
      cols = lookup[node.id]

      prov = _.flatten _.map cols, (col) -> node.colDependsOn col
      for c in node.children()
        lookup[c.id] = prov

      if finalcol in cols
        if not(targetTableName?) or targetTableName == node.name
          return cols
      []
    _.uniq _.compact _.flatten res
  ###

