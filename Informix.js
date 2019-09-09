/**
 * The informix service
 */
const _ = require('lodash')
const config = require('config')
const ifxnjs = require('ifxnjs')

const logger = require('./common/logger')
const tracer = require('./common/tracer')

const paramReg = /@(\w+?)@/ // sql param regex

const pool = new ifxnjs.Pool({
  maxPoolSize: config.DB_POOL_SIZE
})
pool.options.fetchMode = 3 // Fetch mode 3 means to fetch array result

/**
 * Process sql with params.
 * It will find param regex and replace with ? or value in param value if define replace=true as param value
 * @param sql the sql
 * @param params the sql params
 * @private
 */
function processSql (sql, params) {
  let template = String(sql)
  const paramValues = []
  while (true) {
    const match = paramReg.exec(template)
    if (!match) {
      break
    }
    const paramName = match[1]
    const paramValue = params[paramName]
    const replace = _.isObject(paramValue) && !_.isUndefined(paramValue.replace)
    if (Object.prototype.hasOwnProperty.call(params, paramName)) {
      template = template.replace(paramReg, replace ? paramValue.replace : '?')
    } else {
      throw new Error(`Not found param name ${paramName} in given params ${JSON.stringify(params)}.`)
    }
    if (!replace) {
      paramValues.push(paramValue)
    }
  }
  return [template, paramValues]
}

/**
 * Execute informix query.
 * @param {Object} opts the db options
 * @param {String} sql the sql
 * @param {Object} params the sql params
 * @param {Object} parentSpan the parent span object
 * @returns {Array} query result
 */
async function executeQuery (opts, sql, params, parentSpan = null) {
  const span = tracer.buildSpans('Informix.executeQuery', parentSpan)
  span.log({
    event: 'debug',
    opts,
    sql,
    params
  })
  const ctx = new InformixContext(opts)
  try {
    await ctx.begin(span)
    const result = await ctx.query(sql, params, span)
    await ctx.commit(span)
    return result
  } catch (e) {
    await ctx.rollback(span)
    tracer.logSpanError(span, e)
    throw e
  } finally {
    await ctx.end(span)
    span.finish()
  }
}

/**
 * Informix context. A context has its own connection and transaction.
 */
class InformixContext {
  /**
   * Constructor with db options.
   * @param {Object} opts The db options
   */
  constructor (opts) {
    const { server, database, host, protocol, port, username, password, locale } = opts
    this.connStr = `SERVER=${server};DATABASE=${database};HOST=${host};Protocol=${protocol};SERVICE=${port};UID=${username};PWD=${password};DB_LOCALE=${locale};CLIENT_LOCALE=${locale}`
  }

  /**
   * Check if context is opened.
   * @private
   */
  checkOpened () {
    if (!this.conn) {
      throw new Error('Connection is not opened')
    }
  }

  /**
   * Begin context, connection will be opened and transaction will be started.
   * The connection is obtained from pool for efficience.
   * @param {Object} parentSpan the parent span object
   */
  async begin (parentSpan = null) {
    const span = tracer.buildSpans('Informix.begin', parentSpan)
    this.conn = await new Promise((resolve, reject) => {
      pool.open(this.connStr, (err, conn) => {
        if (err) {
          tracer.logSpanError(span, err)
          span.finish()
          reject(err)
        } else {
          span.finish()
          resolve(conn)
        }
      })
    })

    this.conn.beginTransactionSync()
  }

  /**
   * Query with sql and params
   * @param {String} sql the sql
   * @param {Object} params the sql params
   * @param {Object} parentSpan the parent span object
   */
  async query (sql, params, parentSpan = null) {
    const span = tracer.buildSpans('Informix.query', parentSpan)
    span.log({
      event: 'debug',
      sql,
      params
    })
    this.checkOpened()

    let template = sql
    let paramValues

    if (_.isObject(params)) {
      [template, paramValues] = processSql(sql, params)
      logger.debug(`sql template '${template}' with param values ${JSON.stringify(paramValues)}`)
    }

    const result = await this.conn.querySync(template, paramValues)

    if (!_.isArray(result)) {
      span.log({
        event: 'error',
        message: 'Result is not an array',
        'error.object': result
      })
      span.setTag('error', true)
      span.finish()
      // The result must be an array
      throw result
    }

    // Normalize number
    const normalized = result.map(row => {
      return row.map(cell => {
        if (typeof cell !== 'string') {
          return cell
        }
        return /^\d+\.?\d*$/.test(cell) ? parseFloat(cell) : cell
      })
    })
    span.finish()
    return normalized
  }

  /**
   * Commit context transaction.
   * @param {Object} parentSpan the parent span object
   */
  async commit (parentSpan = null) {
    const span = tracer.buildSpans('Informix.commit', parentSpan)
    this.checkOpened()
    await this.conn.commitTransactionSync()
    span.finish()
  }

  /**
   * Rollback context transaction.
   * @param {Object} parentSpan the parent span object
   */
  async rollback (parentSpan = null) {
    const span = tracer.buildSpans('Informix.rollback', parentSpan)
    if (!this.conn) {
      // Nothing to rollback
      span.log({
        event: 'debug',
        message: 'No connection to rollback'
      })
      span.finish()
      return
    }
    await this.conn.rollbackTransactionSync()
    span.finish()
  }

  /**
   * End context. The connection will be released back to pool.
   * @param {Object} parentSpan the parent span object
   */
  async end (parentSpan = null) {
    const span = tracer.buildSpans('Informix.end', parentSpan)
    if (!this.conn) {
      // Not opened yet
      span.log({
        event: 'debug',
        message: 'Connection not opened yet. Cannot end.'
      })
      span.finish()
      return
    }

    await new Promise((resolve, reject) => {
      this.conn.close(err => {
        if (err) {
          tracer.logSpanError(span, err)
          span.finish()
          reject(err)
        } else {
          span.finish()
          resolve()
        }
      })
    })

    this.conn = null
  }
}

module.exports = {
  InformixContext,
  executeQuery
}
