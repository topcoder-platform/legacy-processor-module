/**
 * The informix service
 */
const _ = require('lodash');
const config = require('config');
const ifxnjs = require('ifxnjs');

const logger = require('./common/logger');

const paramReg = /@(\w+?)@/; // sql param regex

const pool = new ifxnjs.Pool({
  maxPoolSize: config.DB_POOL_SIZE
});
pool.options.fetchMode = 3; // Fetch mode 3 means to fetch array result

/**
 * Process sql with params.
 * It will find param regex and replace with ? or value in param value if define replace=true as param value
 * @param sql the sql
 * @param params the sql params
 * @private
 */
function processSql(sql, params) {
  let template = String(sql);
  const paramValues = [];
  while (true) {
    let match = paramReg.exec(template);
    if (!match) {
      break;
    }
    const paramName = match[1];
    const paramValue = params[paramName];
    const replace = _.isObject(paramValue) && !_.isUndefined(paramValue.replace);
    if (params.hasOwnProperty(paramName)) {
      template = template.replace(paramReg, replace ? paramValue.replace : '?');
    } else {
      throw new Error(`Not found param name ${paramName} in given params ${JSON.stringify(params)}.`);
    }
    if (!replace) {
      paramValues.push(paramValue);
    }
  }
  return [template, paramValues];
}

/**
 * Execute informix query.
 * @param {Object} opts the db options
 * @param {String} sql the sql
 * @param {Object} params the sql params
 * @returns {Array} query result
 */
async function executeQuery(opts, sql, params) {
  const ctx = new InformixContext(opts);
  try {
    await ctx.begin();
    const result = await ctx.query(sql, params);
    await ctx.commit();
    return result;
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
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
  constructor(opts) {
    const { server, database, host, protocol, port, username, password, locale } = opts;
    this.connStr = `SERVER=${server};DATABASE=${database};HOST=${host};Protocol=${protocol};SERVICE=${port};UID=${username};PWD=${password};DB_LOCALE=${locale}`;
    logger.debug(this.connStr);
  }

  /**
   * Check if context is opened.
   * @private
   */
  checkOpened() {
    if (!this.conn) {
      throw new Error('Connection is not opened');
    }
  }

  /**
   * Begin context, connection will be opened and transaction will be started.
   * The connection is obtained from pool for efficience.
   */
  async begin() {
    this.conn = await new Promise((resolve, reject) => {
      pool.open(this.connStr, (err, conn) => {
        if (err) {
          reject(err);
        } else {
          resolve(conn);
        }
      });
    });

    this.conn.beginTransactionSync();
  }

  /**
   * Query with sql and params
   * @param {String} sql the sql
   * @param {Object} params the sql params
   */
  async query(sql, params) {
    this.checkOpened();

    let template = sql;
    let paramValues;

    if (_.isObject(params)) {
      [template, paramValues] = processSql(sql, params);
      logger.debug(`sql template '${template}' with param values ${JSON.stringify(paramValues)}`);
    }

    const result = await this.conn.querySync(template, paramValues);

    if (!_.isArray(result)) {
      // The result must be an array
      throw result;
    }

    // Normalize number
    const normalized = result.map(row => {
      return row.map(cell => {
        if (typeof cell !== 'string') {
          return cell;
        }
        return /^\d+\.?\d*$/.test(cell) ? parseFloat(cell) : cell;
      });
    });

    return normalized;
  }

  /**
   * Commit context transaction.
   */
  async commit() {
    this.checkOpened();
    await this.conn.commitTransactionSync();
  }

  /**
   * Rollback context transaction.
   */
  async rollback() {
    if (!this.conn) {
      // Nothing to rollback
      return;
    }
    await this.conn.rollbackTransactionSync();
  }

  /**
   * End context. The connection will be released back to pool.
   */
  async end() {
    if (!this.conn) {
      // Not opened yet
      return;
    }

    await new Promise((resolve, reject) => {
      this.conn.close(err => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });

    this.conn = null;
  }
}

module.exports = {
  InformixContext,
  executeQuery
};
