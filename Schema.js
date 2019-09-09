/**
 * The Joi schema service
 */
const Joi = require('joi')
const _ = require('lodash')

const logger = require('./common/logger')

// Custom Joi type
Joi.id = () =>
  Joi.number()
    .integer()
    .positive() // positive integer id
Joi.sid = () => Joi.alternatives().try(Joi.id(), Joi.string().uuid()) // string id or positive integer id
// valid resource enum
Joi.resource = () =>
  Joi.alternatives().try(
    Joi.string().valid('submission'),
    Joi.string().valid('review'),
    Joi.string().valid('reviewSummation')
  )

/**
 * Create event schema.
 * @param {Object} payloadKeys the keys of event payload
 * @returns {Object} the event schema
 */
function createEventSchema (payloadKeys) {
  return Joi.object().keys({
    topic: Joi.string().required(),
    originator: Joi.string().required(),
    timestamp: Joi.date().required(),
    'mime-type': Joi.string().required(),
    payload: Joi.object()
      .keys(payloadKeys)
      .required()
      .unknown(true)
  })
}

/**
 * Validate event.
 * @param {Object} event the event
 * @param {Object} schema the event schema
 * @returns {Object} the validation result if valid, null if invalid
 */
function validateEvent (event, schema) {
  const validationResult = Joi.validate(event, schema, {
    abortEarly: false,
    stripUnknown: true
  })
  if (validationResult.error) {
    const validationErrorMessage = _.map(
      validationResult.error.details,
      'message'
    ).join(', ')
    logger.debug(`Skipped invalid event, reasons: ${validationErrorMessage}`)
    return
  }
  return validationResult
}

// The common event schema
const commonEventSchema = createEventSchema({
  id: Joi.sid().required(),
  resource: Joi.resource()
})

module.exports = {
  createEventSchema,
  validateEvent,
  commonEventSchema
}
