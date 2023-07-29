/**
 * The Kafka consumer service.
 */
const util = require('util');
const config = require('config');
const Kafka = require('no-kafka');
const healthcheck = require('topcoder-healthcheck-dropin');
const logger = require('./common/logger');
const busApi = require('topcoder-bus-api-wrapper');
const _ = require('lodash');

global.Promise = require('bluebird');

const busConfigObj = {
  AUTH0_URL: config.AUTH0_URL,
  AUTH0_AUDIENCE: config.AUTH0_AUDIENCE,
  TOKEN_CACHE_TIME: config.TOKEN_CACHE_TIME,
  AUTH0_CLIENT_ID: config.AUTH0_CLIENT_ID,
  AUTH0_CLIENT_SECRET: config.AUTH0_CLIENT_SECRET,
  BUSAPI_URL: config.BUSAPI_URL,
  KAFKA_ERROR_TOPIC: config.KAFKA_ERROR_TOPIC,
  AUTH0_PROXY_SERVER_URL: config.AUTH0_PROXY_SERVER_URL
};

const errorConfigObj = JSON.parse(JSON.stringify(busConfigObj));
errorConfigObj.LOG_LEVEL = config.LOG_LEVEL;
errorConfigObj.KAFKA_MESSAGE_ORIGINATOR = config.KAFKA_MESSAGE_ORIGINATOR;
errorConfigObj.POST_KAFKA_ERROR_ENABLED = true;

const busApiClient = busApi(busConfigObj);

/**
 * Get kafka options.
 * @returns {Object} kafka options
 */
function getKafkaOptions() {
  const options = {
    handlerConcurrency: config.KAFKA_CONCURRENCY,
    connectionString: config.KAFKA_URL,
    groupId: config.KAFKA_GROUP_ID
  };
  logger.info(`KAFKA Options - ${JSON.stringify(options)}`);

  if (config.KAFKA_CLIENT_CERT && config.KAFKA_CLIENT_CERT_KEY) {
    options.ssl = {
      cert: config.KAFKA_CLIENT_CERT,
      key: config.KAFKA_CLIENT_CERT_KEY
    };
  }

  return options;
}

const consumer = new Kafka.GroupConsumer(getKafkaOptions());

/**
 * Handle the messages from Kafka.
 * @param {Object} submissionService the submission service
 * @param {Array<Object>} messages the messages
 * @param {String} topic the topic
 * @param {Number} partition the partition
 * @returns {Promise} the promise
 * @private
 */
const handleMessages = (messageSet, topic, partition, submissionService) =>
  Promise.each(messageSet, m => {
    const message = m.message.value ? m.message.value.toString('utf8') : null;
    const messageInfo = `Topic: ${topic}; Partition: ${partition}; Offset: ${m.offset}; Message: ${message}.`;
    logger.info(`Handle Kafka event message; ${messageInfo}`);

    if (!message) {
      logger.error('Skipped null or empty event');
      return;
    }

    let messageJSON;
    try {
      messageJSON = JSON.parse(message);
    } catch (e) {
      logger.error('Skipped Invalid message JSON');
      logger.error(e);
      // ignore the message
      return;
    }

    if (!messageJSON) {
      logger.error('Skipped null or empty event');
      return;
    }

    if (messageJSON.topic !== topic) {
      logger.error(
        `Skipped the message topic "${messageJSON.topic}" doesn't match the Kafka topic ${topic}.`
      );
      // ignore the message
      return;
    }

    return submissionService
      .handle(messageJSON)
      .then(() => {
        logger.debug(`committing offset for ${JSON.stringify(messageJSON)}`);

        consumer.commitOffset({
          topic,
          partition,
          offset: m.offset
        });
      })
      .catch(err => {
        logger.error(`Failed to handle ${messageInfo}: ${err.message}`);
        logger.debug(err);

        logger.debug(
          `Handling failed message; max retry count`
        );

        if (
          _.get(messageJSON, 'payload.retryCount', 0) >
          config.MESSAGE_RETRY_COUNT
        ) {
          logger.error(err);

          logger.debug(
            `Error after processing the message ${config.MESSAGE_RETRY_COUNT} times, committing offset and sending message to error topic`
          );

          logger.debug(`sending error to error module`);
          logger.error(err);

          consumer.commitOffset({
            topic,
            partition,
            offset: m.offset
          });
        } else {
          logger.debug(`Reprocessing the message`);

          let retryCount = messageJSON.payload.retryCount
            ? Number(messageJSON.payload.retryCount) + 1
            : 1;
          messageJSON.payload.retryCount = retryCount;

          logger.debug(messageJSON);
          busApiClient.postEvent(messageJSON);
        }
      });
  });

/**
 * Check if there is kafka connection alive
 * @returns true if kafka connection alive, false otherwise
 * @private
 */
function check() {
  if (
    !consumer.client.initialBrokers &&
    !consumer.client.initialBrokers.length
  ) {
    return false;
  }
  let connected = true;
  consumer.client.initialBrokers.forEach(conn => {
    logger.debug(`url ${conn.server()} - connected=${conn.connected}`);
    connected = conn.connected & connected;
  });
  return connected;
}

/**
 * Start kafka consumer.
 * @param {Object} submissionService the submission service
 * @param {Array<String>} topics the topics to subscribe
 * @returns kafka consumer
 */
function startConsumer(submissionService, topics) {
  consumer
    .init([
      {
        subscriptions: topics,
        handler: async (messageSet, topic, partition) =>
          handleMessages(messageSet, topic, partition, submissionService)
      }
    ])
    .then(() => {
      healthcheck.init([check]);
      logger.debug('Consumer initialized successfully');
    })
    .catch(err => logger.error(err));

  return consumer;
}

module.exports = {
  getKafkaOptions,
  startConsumer
};
