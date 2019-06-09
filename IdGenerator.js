/**
 * The ID generator service
 */
const logger = require("./common/logger");
const util = require("util");
const _ = require("lodash");
const config = require("config");
const Mutex = require("async-mutex").Mutex;
const Informix = require("./Informix");

const QUERY_GET_ID_SEQ =
  "select next_block_start, block_size from id_sequences where name = @seqName@";
const QUERY_UPDATE_ID_SEQ =
  "update id_sequences set next_block_start = @nextStart@ where name = @seqName@";

// db informix option
const dbOpts = {
  database: config.DB_NAME,
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
  constructor(db, seqName) {
    this.db = db;
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
    try {
      let informix = new Informix(dbOpts);

      logger.debug(`inside getNextBlock = ${this.db}`);
      const result = await informix.query(QUERY_GET_ID_SEQ, {
        seqName: this.seqName
      });

      logger.debug(`getNextBlock = ${JSON.stringify(result)}`);

      if (!_.isArray(result) || _.isEmpty(result)) {
        throw new Error(`null or empty result for ${this.seqName}`);
      }
      this._nextId = --result[0][0];
      logger.debug(`getNextBlock = ${ this._nextId}`);
      this._availableId = result[0][1];
      logger.debug(`getNextBlock = ${this._availableId}`);
    } catch (e) {
      logger.error("Failed to get id sequence: " + this.seqName);
      logger.error(util.inspect(e));
    }
  }

  /**
   * Update id_sequence
   * @param {Number} nextStart next start id
   * @private
   */
  async updateNextBlock(nextStart) {
    try {
      let informix = new Informix(dbOpts);

      await informix.query(QUERY_UPDATE_ID_SEQ, {
        seqName: this.seqName,
        nextStart
      });
    } catch (e) {
      logger.error("Failed to update id sequence: " + this.seqName);
      logger.error(util.inspect(e));
    }
  }
}

module.exports = IDGenerator;
