/**
 * The script to send test events to Kafka.
 */
// Make sure that we don't send test events in production environment by mistake
// will load same NODE_ENV in docker container and still use test as default
process.env.NODE_ENV = process.env.NODE_ENV || 'test'

const util = require('util')
const config = require('config')
const Kafka = require('no-kafka')

const _ = require('lodash')

const logger = require('..//common/logger')
const { getKafkaOptions } = require('../KafkaConsumer')
const {
  sampleSubmission,
  sampleFinalFixSubmission,
  sampleNotAllowMultipleSubmission,
  sampleNoChallengePropertiesSubmission,
  sampleMMSubmission,
  sampleMMProvisionalReview,
  sampleMMFinalReview,
  sampleMMSubmission2,
  sampleMMProvisionalReview2,
  sampleMMFinalReview2
} = require('../mock/mock-api')

const header = {
  topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
  originator: config.KAFKA_NEW_SUBMISSION_ORIGINATOR,
  timestamp: '2018-02-16T00:00:00',
  'mime-type': 'application/json'
}

// The good sample message
const sampleMessage = {
  ...header,
  payload: {
    ...sampleSubmission
  }
}

// The good final fix sample message
const sampleFinalFixMessage = {
  ...header,
  payload: {
    ...sampleFinalFixSubmission
  }
}

// The good not allow multiple submission sample message
const sampleNotAllowMultipleMessage = {
  ...header,
  payload: {
    ...sampleNotAllowMultipleSubmission
  }
}

// The good no challenge properties sample message
const sampleNoChallengePropertiesMessage = {
  ...header,
  payload: {
    ...sampleNoChallengePropertiesSubmission
  }
}

// The good mm sample message
const sampleMMMessage = {
  ...header,
  payload: {
    ...sampleMMSubmission
  }
}

// The good mm review provisional sample message
const sampleMMReviewProvisionalMessage = {
  ...header,
  payload: {
    ...sampleMMProvisionalReview
  }
}

// The good mm review final sample message
const sampleMMReviewFinalMessage = {
  ...header,
  payload: {
    ...sampleMMFinalReview
  }
}

// The good mm sample message
const sampleMMMessage2 = {
  ...header,
  payload: {
    ...sampleMMSubmission2
  }
}

// The good mm review provisional sample message
const sampleMMReviewProvisionalMessage2 = {
  ...header,
  payload: {
    ...sampleMMProvisionalReview2
  }
}

// The good mm review final sample message
const sampleMMReviewFinalMessage2 = {
  ...header,
  payload: {
    ...sampleMMFinalReview2
  }
}

// The test events
const events = {
  // Different topic, not consumed by the app
  'different-topic': { topic: 'different-topic', message: { value: 'message' } },

  // Null message
  'null-message': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: null } },

  // Empty message
  'empty-message': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: '' } },

  // Not well-formed JSON string
  'invalid-json': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: 'abc' } },

  // Empty JSON string
  'empty-json': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: '{}' } },

  // Invalid timestamp and payload
  'invalid-payload': {
    topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
    message: {
      value: JSON.stringify(_.merge({}, sampleMessage, {
        timestamp: 'invalid date',
        payload: {
          id: 0,
          challengeId: 'a',
          memberId: 'b',
          url: 'invalid url',
          resource: 'submission',
          type: null
        }
      }))
    }
  },

  // Wrong message topic
  'wrong-topic': {
    topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
    message: {
      value: JSON.stringify(_.merge({}, sampleMessage, {
        topic: 'wrong-topic'
      }))
    }
  },

  // Wrong message originator
  'wrong-originator': {
    topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
    message: {
      value: JSON.stringify(_.merge({}, sampleMessage, {
        originator: 'wrong-originator'
      }))
    }
  },

  // Sample submission
  submission: { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMessage) } },
  // Sample final fix submission
  'final-fix': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleFinalFixMessage) } },
  // Sample not-allow-multiple submission
  'not-allow-multiple': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleNotAllowMultipleMessage) } },
  // Sample no-challenge-props submission
  'no-challenge-props': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleNoChallengePropertiesMessage) } },

  // Sample update url message
  'update-url': {
    topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
    message: {
      value: JSON.stringify(_.merge({}, sampleMessage, {
        topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
        payload: {
          url: 'http://content.topcoder.com/some/path/updated'
        }
      }))
    }
  },

  // Sample mm submission
  'mm-submission': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMMMessage) } },
  // Sample mm submission
  'mm-submission2': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMMMessage2) } },

  // Sample update mm submission url message
  'update-mm-url': {
    topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
    message: {
      value: JSON.stringify(_.merge({}, sampleMMMessage, {
        topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC,
        payload: {
          url: 'http://content.topcoder.com/some/path/updated'
        }
      }))
    }
  },

  // Sample mm provisional score
  'mm-provisional-score': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMMReviewProvisionalMessage) } },
  // Sample mm provisional score
  'mm-provisional-score2': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMMReviewProvisionalMessage2) } },

  // Sample mm final score
  'mm-final-score': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMMReviewFinalMessage) } },
  // Sample mm final score
  'mm-final-score2': { topic: config.KAFKA_AGGREGATE_SUBMISSION_TOPIC, message: { value: JSON.stringify(sampleMMReviewFinalMessage2) } }
}

// Init the producer and send the test events
const producer = new Kafka.Producer(getKafkaOptions())

// Get event id from argument
// npm run produce-test-event submission
const eventId = process.argv[2]

producer.init()
  .then(() => producer.send(events[eventId]))
  .then((results) => {
    logger.debug(`Produced event ${util.inspect(events[eventId])}`)
    logger.debug(results)
  })
  .then(() => process.exit(0))
  .catch((err) => {
    logger.error(err)
    process.exit(1)
  })
