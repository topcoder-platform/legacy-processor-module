/**
 * The Kafka consumer service.
 */
const util = require('util')

const config = require('config')
const Kafka = require('no-kafka')
const healthcheck = require('topcoder-healthcheck-dropin')

const logger = require('./common/logger')

/**
 * Get kafka options.
 * @returns {Object} kafka options
 */
function getKafkaOptions () {
  const options = { connectionString: config.KAFKA_URL, groupId: config.KAFKA_GROUP_ID }
  logger.info(`KAFKA Options - ${JSON.stringify(options)}`)

  if (config.KAFKA_CLIENT_CERT && config.KAFKA_CLIENT_CERT_KEY) {
    options.ssl = { cert: config.KAFKA_CLIENT_CERT, key: config.KAFKA_CLIENT_CERT_KEY }
  }

  return options
}

const consumer = new Kafka.GroupConsumer(getKafkaOptions())

/**
 * Handle the messages from Kafka.
 * @param {Object} submissionService the submission service
 * @param {Array<Object>} messages the messages
 * @param {String} topic the topic
 * @param {Number} partition the partition
 * @returns {Promise} the promise
 * @private
 */
function handleMessages (submissionService, messages, topic, partition) {
  return Promise.each(messages, (m) => {
    const messageValue = m.message.value ? m.message.value.toString('utf8') : null
    const messageInfo = `message from topic ${topic}, partition ${partition}, offset ${m.offset}: ${messageValue}`

    logger.debug(`Received ${messageInfo}`)

    // Handle the event
    return submissionService.handle(messageValue)
      .then(() => {
        logger.debug(`Completed handling ${messageInfo}`)

        // Commit offset
        return consumer.commitOffset({
          topic, partition, offset: m.offset
        })
          .catch(err => {
            logger.error(`Failed to commit offset for ${messageInfo}: ${err.message}`)
            logger.error(util.inspect(err))
          })
      })
      .catch(err => {
        // Catch all errors thrown by the handler
        logger.error(`Failed to handle ${messageInfo}: ${err.message}`)
        logger.error(util.inspect(err))
      })
  })
}

/**
 * Check if there is kafka connection alive
 * @returns true if kafka connection alive, false otherwise
 * @private
 */
function check () {
  if (!consumer.client.initialBrokers && !consumer.client.initialBrokers.length) {
    return false
  }
  let connected = true
  consumer.client.initialBrokers.forEach(conn => {
    logger.debug(`url ${conn.server()} - connected=${conn.connected}`)
    connected = conn.connected & connected
  })
  return connected
}

/**
 * Start kafka consumer.
 * @param {Object} submissionService the submission service
 * @param {Array<String>} topics the topics to subscribe
 * @returns kafka consumer
 */
function startConsumer (submissionService, topics) {
  consumer
    .init([{
      subscriptions: topics,
      handler: async (messages, topic, partition) => handleMessages(submissionService, messages, topic, partition)
    }])
    .then(() => {
      healthcheck.init([check])
    })
    .catch((err) => logger.error(err))

  return consumer
}

module.exports = {
  getKafkaOptions,
  startConsumer
}
