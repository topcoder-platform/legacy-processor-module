/**
 * The informix service
 */
const _ = require("lodash");
const Informix = require("informix").Informix;
const logger = require("./common/logger");

const paramReg = /@(\w+?)@/; // sql param regex
let instances = {};

/**
 * Main class of InformixService
 */
function getInformixConnection(opts) {
  let { database, username, password } = opts;
  let key = `${database}-${username}-${password}`;

  if (instances[key]) {
    logger.debug(`InformixService->constructor(): found instance of ${key}`);
    return instances[key];
  }

  logger.debug(
    `InformixService->constructor(): did not find instance of ${key}. creating new instance`
  );
  let dbConnection = new Informix(opts);
  instances[key] = dbConnection;
  return dbConnection;
}

function createContext(dbConnection) {
  return dbConnection.createContext();
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
      template = template.replace(paramReg, replace ? paramValue.replace : "?");
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

/**
 * Query with sql and params
 * @param{Object} conn the connection
 * @param{String} sql the sql
 * @param{Object} params the sql params
 */
async function query(conn, sql, params) {
  let fetchAll = sql
    .toLowerCase()
    .trimLeft()
    .startsWith("select");

  let cursor = null;
  let stmt = null;
  let result = null;

  try {
    if (_.isObject(params)) {
      const [template, paramValues] = processSql(sql, params);
      logger.debug(
        `sql template '${template}' with param values [${paramValues.join()}]`
      );
      stmt = await conn.prepare(template);
      cursor = await stmt.exec(paramValues);
    } else {
      cursor = await conn.query(sql);
    }
    if (fetchAll) {
      result = await cursor.fetchAll();
    }
  } catch (e) {
    throw e;
  } finally {
    if (cursor) {
      await cursor.close();
      logger.log("cursor closed");
    }
    if (stmt) {
      await stmt.free();
      logger.log("stmt free");
    }
  }
  return result;
}

module.exports = {
  getInformixConnection,
  createContext,
  query
};
