/**
 * The ID generator service
 */
const logger = require("./common/logger");
const util = require("util");
const _ = require("lodash");
const config = require("config");
const Mutex = require("async-mutex").Mutex;
const {
  getInformixConnection,
  createContext,
  query
} = require("./Informix");

const QUERY_GET_ID_SEQ =
  "select next_block_start, block_size from id_sequences where name = @seqName@";
const QUERY_UPDATE_ID_SEQ =
  "update id_sequences set next_block_start = @nextStart@ where name = @seqName@";

// db informix option
const dbOpts = {
  database: config.DB_ID_NAME,
  username: config.DB_USERNAME,
  password: config.DB_PASSWORD,
  pool: {
    min: 0,
    max: 10
  }
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
        await this.getNextBlock();
        await this.updateNextBlock(this._nextId + this._availableId + 1);
      }

      logger.debug(`this._availableId = ${this._nextId}`);
      return ++this._nextId;
    } catch (e) {
      throw e;
    } finally {
      release();
    }
  }

  /**
   * Fetch next block from id_sequence
   * @private
   */
  async getNextBlock() {
    let dbConnection = getInformixConnection(dbOpts);

    try {
      const result = await query(dbConnection, QUERY_GET_ID_SEQ, {
        seqName: this.seqName
      });

      if (!_.isArray(result) || _.isEmpty(result)) {
        throw new Error(`null or empty result for ${this.seqName}`);
      }
      this._nextId = --result[0][0];
      this._availableId = result[0][1];
    } catch (e) {
      logger.error(util.inspect(e));
      throw e;
    }
  }

  /**
   * Update id_sequence
   * @param {Number} nextStart next start id
   * @private
   */
  async updateNextBlock(nextStart) {
    let dbConnection = getInformixConnection(dbOpts);
    let ctx = createContext(dbConnection);
    try {
      await ctx.begin();

      await query(ctx, QUERY_UPDATE_ID_SEQ, {
        seqName: this.seqName,
        nextStart
      });
      await ctx.commit();
    } catch (e) {
      logger.error("Failed to update id sequence: " + this.seqName);
      logger.error(util.inspect(e));
      await ctx.rollback();
      throw e;
    } finally {
      await ctx.end();
    }
  }
}

module.exports = IDGenerator;
