# => SRC FOLDER
#
toast
  folders:
    'src/prov': 'prov'

  # EXCLUDED FOLDERS (optional)
  exclude: [
  ]

  # => VENDORS (optional)
  vendors: [
    #'vendor/js/d3.min.js',
    #'vendor/js/jquery.min.js',
    'vendor/js/json2.js',
    'vendor/js/underscore.min.js',
    'vendor/js/seedrandom.js',
    'vendor/js/science.js'
  ]


  # => OPTIONS (optional, default values listed)
  # bare: false
  #packaging: false
  #expose: 'window'
  minify: false

  # => HTTPFOLDER (optional), RELEASE / DEBUG (required)
  httpfolder: '../js/'
  release: 'build/compiled/ggprov.js'
  debug: 'build/compiled/ggprov-debug.js'



