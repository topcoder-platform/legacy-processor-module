/**
 * Helper used in unit tests.
 */
const config = require('config')
const should = require('should')
const _ = require('lodash')

const { executeQuery } = require('../Informix')

const opts = {
  server: config.DB_SERVER,
  database: config.DB_NAME,
  host: config.DB_HOST,
  service: config.DB_SERVICE,
  protocol: config.DB_PROTOCOL,
  port: config.DB_PORT,
  username: config.DB_USERNAME,
  password: config.DB_PASSWORD,
  locale: config.DB_LOCALE
}

/**
 * Sleep with time from input
 * @param time the time input
 */
async function sleep (time) {
  await new Promise((resolve) => {
    setTimeout(resolve, time)
  })
}

/**
 * Query informix.
 * @param {String} sql the sql
 * @param {Object} params the sql params
 */
async function queryInformix (sql, params) {
  return executeQuery(opts, sql, params)
}

/**
 * Expect count of table rows with params
 * @param table the table
 * @param count the rows to expect
 * @param params the sql params
 */
async function expectTable (table, count, params) {
  let sql = `select count(*) from ${table}`
  if (!_.isEmpty(params)) {
    sql += ` where ${Object.keys(params).map((k) => {
      const v = params[k]
      if (_.isNull(v)) {
        return `${k} is null`
      } else {
        return `${k}=@${k}@`
      }
    }).join(' and ')}`
    Object.keys(params).forEach((key) => {
      params[key] = typeof params[key] === 'number' ? params[key] + '' : params[key]
    })
  }
  const result = await queryInformix(sql, params)
  should.equal(result[0][0], count, `Table ${table} got wrong expect count result expect ${count} actual ${result[0][0]}`)
}

/**
 * Clear submissions in db
 */
async function clearSubmissions () {
  await queryInformix('DELETE FROM informixoltp:long_comp_result')
  await queryInformix('DELETE FROM informixoltp:long_submission')
  await queryInformix('DELETE FROM informixoltp:long_component_state')
  await queryInformix('DELETE FROM informixoltp:round_registration')

  await queryInformix('DELETE FROM resource_submission')
  await queryInformix('DELETE FROM submission')
  await queryInformix('DELETE FROM upload')
}

module.exports = {
  sleep,
  expectTable,
  clearSubmissions,
  queryInformix
}
