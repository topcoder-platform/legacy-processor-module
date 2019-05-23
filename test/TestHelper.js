/**
 * Helper used in unit tests.
 */
const config = require('config')
const should = require('should')
const _ = require('lodash')

const Informix = require('../Informix')

const informix = new Informix({
  database: config.DB_NAME,
  username: config.DB_USERNAME,
  password: config.DB_PASSWORD,
  pool: {
    min: 0,
    max: 10
  }
})

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
  }
  const result = await informix.query(sql, params)
  should.equal(result[0][0], count, `Table ${table} got wrong expect count result expect ${count} actual ${result[0][0]}`)
}

/**
 * Clear submissions in db
 */
async function clearSubmissions () {
  await informix.query('DELETE FROM informixoltp:long_comp_result')
  await informix.query('DELETE FROM informixoltp:long_submission')
  await informix.query('DELETE FROM informixoltp:long_component_state')
  await informix.query('DELETE FROM informixoltp:round_registration')

  await informix.query('DELETE FROM resource_submission')
  await informix.query('DELETE FROM submission')
  await informix.query('DELETE FROM upload')
}

module.exports = {
  sleep,
  expectTable,
  clearSubmissions,
  informix
}
