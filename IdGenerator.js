/**
 * The ID generator service
 */
const logger = require("./common/logger");
const util = require("util");
const _ = require("lodash");
const config = require("config");
const Mutex = require("async-mutex").Mutex;
const { InformixContext } = require("./Informix");

const QUERY_GET_ID_SEQ =
  "select next_block_start, block_size from id_sequences where name = @seqName@";
const QUERY_UPDATE_ID_SEQ =
  "update id_sequences set next_block_start = @nextStart@ where name = @seqName@";

// db informix option
const dbOpts = {
  server: config.DB_SERVER,
  database: config.DB_ID_NAME,
  host: config.DB_HOST,
  service: config.DB_SERVICE,
  username: config.DB_USERNAME,
  password: config.DB_PASSWORD,
};

/**
 * Main class of IDGenerator
 */
class IDGenerator {
  /**
   * Constructor
   * @param {Informix} db database
   * @param {String} seqName sequence name
   */
  constructor(seqName) {
    this.seqName = seqName;
    this._availableId = 0;
    this.mutex = new Mutex();
  }

  /**
   * Get next id
   * @returns {Number} next id
   */
  async getNextId() {
    const release = await this.mutex.acquire();
    try {
      logger.debug("Getting nextId");
      --this._availableId;
      logger.debug(`this._availableId = ${this._availableId}`);

      if (this._availableId <= 0) {
        const ctx = new InformixContext(dbOpts);
        try {
          await ctx.begin();

          const [nextId, availableId] = await this.getNextBlock(ctx);
          await this.updateNextBlock(ctx, nextId + availableId + 1);

          await ctx.commit();

          // Only set to this's properties after successful commit
          this._nextId = nextId;
          this._availableId = availableId;
        } catch (e) {
          await ctx.rollback();
          throw e;
        } finally {
          await ctx.end();
        }
      }

      logger.debug(`this._availableId = ${this._availableId}`);
      return ++this._nextId;
    } catch (e) {
      throw e;
    } finally {
      release();
    }
  }

  /**
   * Fetch next block from id_sequence
   * @param {InformixContext} ctx informix db context
   * @returns {Array} [nextId, availableId]
   * @private
   */
  async getNextBlock(ctx) {

    try {
      const result = await ctx.query(QUERY_GET_ID_SEQ, {
        seqName: this.seqName
      });

      if (!_.isArray(result) || _.isEmpty(result)) {
        throw new Error(`null or empty result for ${this.seqName}`);
      }

      return [result[0][0] - 1, result[0][1]];
    } catch (e) {
      logger.error(util.inspect(e));
      throw e;
    }
  }

  /**
   * Update id_sequence
   * @param {InformixContext} ctx informix db context
   * @param {Number} nextStart next start id
   * @private
   */
  async updateNextBlock(ctx, nextStart) {
    try {
      await ctx.query(QUERY_UPDATE_ID_SEQ, {
        seqName: this.seqName,
        nextStart
      });
    } catch (e) {
      logger.error("Failed to update id sequence: " + this.seqName);
      logger.error(util.inspect(e));
      throw e;
    }
  }
}

module.exports = IDGenerator;
