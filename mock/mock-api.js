/**
 * The mock Submission && Challenge API.
 */
const config = require('config')
const http = require('http')
const send = require('http-json-response')
const _ = require('lodash')

const logger = require('../common/logger')

// The good sample submission
const sampleSubmission = {
  id: 111,
  challengeId: 30005521,
  memberId: 124916,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95245,
  created: '2018-02-16T00:00:00',
  originalTopic: config.KAFKA_NEW_SUBMISSION_TOPIC
}

// The good studio sample submission
const sampleStudioSubmission = {
  id: 112,
  challengeId: 30005530,
  memberId: 124764,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95284,
  created: '2018-02-16T00:00:00',
  originalTopic: config.KAFKA_NEW_SUBMISSION_TOPIC
}

// The good no challenge properties sample submission
const sampleNoChallengePropertiesSubmission = {
  id: 113,
  challengeId: 30005530,
  memberId: 132458,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95308,
  created: '2018-02-16T00:00:00'
}

// The good final fix sample submission
const sampleFinalFixSubmission = {
  id: 114,
  challengeId: 30005540,
  memberId: 132458,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95308,
  created: '2018-02-16T00:00:00',
  originalTopic: config.KAFKA_NEW_SUBMISSION_TOPIC
}

// The good not allow multiple submission sample submission
const sampleNotAllowMultipleSubmission = {
  id: 115,
  challengeId: 30005540,
  memberId: 132458,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95301,
  created: '2018-02-16T00:00:00',
  originalTopic: config.KAFKA_NEW_SUBMISSION_TOPIC
}

// The good sample MM submission
const sampleMMSubmission = {
  id: 118,
  challengeId: 30054163,
  memberId: 132458,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95311,
  updated: '2018-02-16T00:00:00',
  created: '2018-02-16T00:00:00',
  originalTopic: config.KAFKA_NEW_SUBMISSION_TOPIC
}

// The good sample MM submission
const sampleMMSubmission2 = {
  id: 119,
  challengeId: 30054163,
  memberId: 124916,
  resource: 'submission',
  url: 'http://content.topcoder.com/some/path',
  type: 'Contest Submission',
  submissionPhaseId: 95311,
  updated: '2018-02-16T00:00:00',
  created: '2018-02-16T00:00:00',
  originalTopic: config.KAFKA_NEW_SUBMISSION_TOPIC
}

// The good mm provisional review
const sampleMMProvisionalReview = {
  id: 1,
  resource: 'review',
  submissionId: 118,
  typeId: 'bcf2b43b-20df-44d1-afd3-7fc9798dfcae',
  score: 90.2,
  metadata: {
    testType: 'provisional',
    testCases: [
      'DPK.CP001_A549_24H_X1_B42',
      'LITMUS.KD017_A549_96H_X1_B42'
    ]
  }
}

// The good mm provisional review
const sampleMMProvisionalReview2 = {
  id: 2,
  resource: 'review',
  submissionId: 119,
  typeId: 'bcf2b43b-20df-44d1-afd3-7fc9798dfcae',
  score: 92.3,
  metadata: {
    testType: 'provisional',
    testCases: [
      'DPK.CP001_A549_24H_X1_B42',
      'LITMUS.KD017_A549_96H_X1_B42'
    ]
  }
}

// The good mm final review
const sampleMMFinalReview = {
  id: 3,
  resource: 'reviewSummation',
  submissionId: 118,
  aggregateScore: 97.5,
  metadata: {
    testType: 'final',
    testCases: [
      'DPK.CP001_A549_24H_X1_B42',
      'LITMUS.KD017_A549_96H_X1_B42'
    ]
  }
}

// The good mm final review
const sampleMMFinalReview2 = {
  id: 4,
  resource: 'reviewSummation',
  submissionId: 119,
  aggregateScore: 98.5,
  metadata: {
    testType: 'final',
    testCases: [
      'DPK.CP001_A549_24H_X1_B42',
      'LITMUS.KD017_A549_96H_X1_B42'
    ]
  }
}

const normalSubmission = {
  resource: 'submission',
  id: 'cfdbc0cf-6437-433e-8af1-c56f317f2afd',
  type: 'Contest Submission',
  url: 'https://topcoder-dev-submissions.s3.amazonaws.com/cfdbc0cf-6437-433e-8af1-c56f317f2afd',
  memberId: 124916,
  challengeId: 30005521,
  created: '2018-07-31T17:05:17.835Z',
  updated: '2018-07-31T17:05:17.835Z',
  createdBy: 'callmekatootie',
  updatedBy: 'callmekatootie',
  submissionPhaseId: 95245,
  isFileSubmission: true,
  fileType: 'zip',
  filename: 'Photo on 7-30-18 at 11.47 AM #2.jpg'
}

const submissions = {
  '/submissions/111': sampleSubmission,
  '/submissions/112': sampleStudioSubmission,
  '/submissions/113': sampleNoChallengePropertiesSubmission,
  '/submissions/114': sampleFinalFixSubmission,
  '/submissions/115': sampleNotAllowMultipleSubmission,
  '/submissions/118': sampleMMSubmission,
  '/submissions/119': sampleMMSubmission2
}

// only include used properties and you may check real response from https://api.topcoder-dev.com/v4/challenges?filter=id=30005521
const getChallenge = (subTrack) => ({
  result: {
    content: {
      subTrack
    }
  }
})
const challenges = {
  '/challenges?filter=id=30005521': getChallenge('DEVELOPMENT'),
  '/challenges?filter=id=30005540': getChallenge('ARCHITECTURE'),
  '/challenges?filter=id=30005530': getChallenge('DEVELOPMENT'),
  '/challenges?filter=id=30054163': getChallenge('DEVELOP_MARATHON_MATCH'),
  '/challenges?filter=id=30054164': getChallenge('DEVELOP_MARATHON_MATCH'),
  '/challenges?filter=id=60005521': getChallenge('')
}

const mockApi = http.createServer((req, res) => {
  logger.debug(`${req.method} ${req.url}`)
  // PUT /submissions/:id
  if ((req.method === 'PUT' || req.method === 'PATCH') &&
      (req.url.match(/^\/submissions\/[1-9]\d*$/) ||
       req.url.match(/^\/submissions\/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/gi))) {
    let body = ''
    req.on('data', chunk => {
      // Convert Buffer to string
      body += chunk.toString()
    })
    req.on('end', () => {
      logger.debug(`Patch ${req.url} with ${body}`)

      if (submissions[req.url]) {
        const patchObject = JSON.parse(body)
        _.merge(submissions[req.url], patchObject)
      }
      // Always return 200 response
      res.statusCode = 200
      res.end()
    })
  } else if (req.method === 'GET' &&
    (req.url.match(/^\/submissions\/[1-9]\d*$/) ||
     req.url.match(/^\/submissions\/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/gi))) {
    return send(res, 200, submissions[req.url] || normalSubmission)
  } else if (req.method === 'GET' && Object.keys(challenges).includes(req.url)) {
    return send(res, 200, challenges[req.url])
  } else if (req.method === 'GET' && req.url.match(/^\/challenges\?filter=id=\d+$/)) {
    // default to mock as mm challenge
    return send(res, 200, getChallenge('DEVELOP_MARATHON_MATCH'))
  } else {
    // 404 for other routes
    res.statusCode = 404
    res.end('Not Found')
  }
})
if (!module.parent) {
  const port = config.MOCK_API_PORT || 3000
  mockApi.listen(port)
  console.log(`mock submission api is listening on port ${port}`)
}
module.exports = {
  sampleSubmission,
  sampleFinalFixSubmission,
  sampleStudioSubmission,
  sampleNotAllowMultipleSubmission,
  sampleNoChallengePropertiesSubmission,
  sampleMMSubmission,
  sampleMMProvisionalReview,
  sampleMMFinalReview,
  sampleMMSubmission2,
  sampleMMProvisionalReview2,
  sampleMMFinalReview2,
  mockApi
}
