var request = require('request')
var split = require('split2')

module.exports = function(url, opts) {
  if (!opts) opts = {}
  if (typeof opts.retry !== 'number' && opts.retry !== false) opts.retry = 3000

  var dataBuf = ''
  var event = ''
  var id = ''
  var req
  var timeout

  var parse = split(function(line) {
    if (!line) {
      if (!dataBuf) return
      var data = dataBuf
      dataBuf = ''
      return {
        id : id,
        event : event,
        data : data
      }
    }
    if (line.indexOf('data: ') === 0) dataBuf += (dataBuf ? '\n' : '') + line.slice(6)
    if (line.indexOf('id: ') === 0) id = line.slice(4)
    if (line.indexOf('event: ') === 0) event = line.slice(6)

  })

  var connect = function() {
    dataBuf = ''
    req = request({url: url, headers: opts.headers})

    req.on('error', function(err) {
      if (!opts.retry) parse.emit('error', err)
    })

    req.on('complete', function() {
      if (destroyed) return

      if (!opts.retry) {
        destroyed = true
        return parse.end()
      }

      timeout = setTimeout(connect, opts.retry)
      parse.emit('retry')
    })

    req.pipe(parse, {end:false})
  }

  connect()

  var destroyed = false
  parse.destroy = function() {
    if (destroyed) return
    destroyed = true
    clearTimeout(timeout)
    if (req) req.abort()
    parse.emit('close')
  }

  return parse
}