/**
 * The ID generator service
 */
const logger = require('./common/logger')
const tracer = require('./common/tracer')
const util = require('util')
const _ = require('lodash')
const config = require('config')
const Mutex = require('async-mutex').Mutex
const { InformixContext } = require('./Informix')

const QUERY_GET_ID_SEQ = 'select next_block_start, block_size from id_sequences where name = @seqName@'
const QUERY_UPDATE_ID_SEQ = 'update id_sequences set next_block_start = @nextStart@ where name = @seqName@'

// db informix option
const dbOpts = {
  server: config.DB_SERVER,
  database: config.DB_ID_NAME,
  host: config.DB_HOST,
  protocol: config.DB_PROTOCOL,
  port: config.DB_PORT,
  username: config.DB_USERNAME,
  password: config.DB_PASSWORD,
  locale: config.DB_LOCALE
}

/**
 * Main class of IDGenerator
 */
class IDGenerator {
  /**
   * Constructor
   * @param {Informix} db database
   * @param {String} seqName sequence name
   */
  constructor (seqName) {
    this.seqName = seqName
    this._availableId = 0
    this.mutex = new Mutex()
  }

  /**
   * Get next id
   * @param {Object} parentSpan the parent span object
   * @returns {Number} next id
   */
  async getNextId (parentSpan = null) {
    const span = tracer.buildSpans('IdGenerator.getNextId', parentSpan)
    const release = await this.mutex.acquire()
    try {
      logger.debug('Getting nextId')
      --this._availableId
      logger.debug(`this._availableId = ${this._availableId}`)

      if (this._availableId <= 0) {
        const ctx = new InformixContext(dbOpts)
        try {
          await ctx.begin(span)

          const [nextId, availableId] = await this.getNextBlock(ctx, span)
          await this.updateNextBlock(ctx, nextId + availableId + 1, span)

          await ctx.commit(span)

          // Only set to this's properties after successful commit
          this._nextId = nextId
          this._availableId = availableId
          span.setTag('nextId', nextId)
          span.setTag('availableId', availableId)
        } catch (e) {
          await ctx.rollback(span)
          throw e
        } finally {
          await ctx.end(span)
        }
      }

      logger.debug(`this._availableId = ${this._availableId}`)
      return ++this._nextId
    } catch (e) {
      tracer.logSpanError(span, e)
      throw e
    } finally {
      span.finish()
      release()
    }
  }

  /**
   * Fetch next block from id_sequence
   * @param {InformixContext} ctx informix db context
   * @param {Object} parentSpan the parent span object
   * @returns {Array} [nextId, availableId]
   * @private
   */
  async getNextBlock (ctx, parentSpan = null) {
    const span = tracer.buildSpans('IdGenerator.getNextBlock', parentSpan)
    try {
      const result = await ctx.query(QUERY_GET_ID_SEQ, {
        seqName: this.seqName
      }, span)

      if (!_.isArray(result) || _.isEmpty(result)) {
        throw new Error(`null or empty result for ${this.seqName}`)
      }
      return [result[0][0] - 1, result[0][1]]
    } catch (e) {
      logger.error(util.inspect(e))
      tracer.logSpanError(span, e)
      throw e
    } finally {
      span.finish()
    }
  }

  /**
   * Update id_sequence
   * @param {InformixContext} ctx informix db context
   * @param {Number} nextStart next start id
   * @param {Object} parentSpan the parent span object
   * @private
   */
  async updateNextBlock (ctx, nextStart, parentSpan = null) {
    const span = tracer.buildSpans('IdGenerator.updateNextBlock', parentSpan)
    try {
      await ctx.query(QUERY_UPDATE_ID_SEQ, {
        seqName: this.seqName,
        nextStart
      }, span)
    } catch (e) {
      logger.error('Failed to update id sequence: ' + this.seqName)
      logger.error(util.inspect(e))
      tracer.logSpanError(span, e)
      throw e
    } finally {
      span.finish()
    }
  }
}

module.exports = IDGenerator
