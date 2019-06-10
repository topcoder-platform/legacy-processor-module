/**
 * The informix service
 */
const _ = require("lodash");
const config = require("config");
const ifxnjs = require("ifxnjs");
const Pool = ifxnjs.Pool;
const pool = Promise.promisifyAll(new Pool());
pool.setMaxPoolSize(config.get("INFORMIX.POOL_MAX_SIZE"));

/**
 * Get Informix connection using the configured parameters
 * @return {Object} Informix connection
 */
async function getInformixConnection() {
  // construct the connection string from the configuration parameters.
  const connectionString =
    "SERVER=" +
    config.get("INFORMIX.SERVER") +
    ";DATABASE=" +
    config.get("INFORMIX.DATABASE") +
    ";HOST=" +
    config.get("INFORMIX.HOST") +
    ";Protocol=" +
    config.get("INFORMIX.PROTOCOL") +
    ";SERVICE=" +
    config.get("INFORMIX.PORT") +
    ";DB_LOCALE=" +
    config.get("INFORMIX.DB_LOCALE") +
    ";UID=" +
    config.get("INFORMIX.USER") +
    ";PWD=" +
    config.get("INFORMIX.PASSWORD");
  const conn = await pool.openAsync(connectionString);
  return Promise.promisifyAll(conn);
}

/**
 * Prepare Informix statement
 * @param connection the Informix connection
 * @param sql the sql
 * @return {Object} Informix statement
 */
async function prepare(connection, sql) {
  const stmt = await connection.prepareAsync(sql);
  return Promise.promisifyAll(stmt);
}

/**
 * Wrap Informix queries with transaction support
 * @param func the Informix multi queries with write operations
 */
async function wrapTransaction(func) {
  const conn = await getInformixConnection();
  try {
    await conn.beginTransactionAsync();
    const result = await func(conn);
    await conn.commitTransactionAsync();
    return result;
  } catch (e) {
    await conn.rollbackTransactionAsync();
    throw e;
  } finally {
    await conn.closeAsync();
  }
}

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
      const replace =
        _.isObject(paramValue) && !_.isUndefined(paramValue.replace);
      if (params.hasOwnProperty(paramName)) {
        template = template.replace(
          paramReg,
          replace ? paramValue.replace : "?"
        );
      } else {
        throw new Error(
          `Not found param name ${paramName} in given params ${JSON.stringify(
            params
          )}.`
        );
      }
      if (!replace) {
        paramValues.push(paramValue);
      }
    }
    return [template, paramValues];
  }

module.exports = {
  getInformixConnection,
  prepare,
  wrapTransaction,
  processSql
};
