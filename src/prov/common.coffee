
prov.BaseOp =
  pstores: -> []
  inputshapes: -> []
  fmap: (id, path) -> [id, path]
  bmap: (id, path) -> [id, path]
  pmap: (payload, path) -> null

prov.One2One = _.extend prov.BaseOp, {
  pstores: -> []
}




