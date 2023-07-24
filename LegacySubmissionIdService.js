/**
 * Legacy submission service
 */
const util = require('util');

const Axios = require('axios');
const config = require('config');
const Flatted = require('flatted');
const m2mAuth = require('tc-core-library-js').auth.m2m;
const moment = require('moment');
const momentTZ = require('moment-timezone');

const logger = require('./common/logger');
const constant = require('./common/constant');
const { InformixContext } = require('./Informix');
const IDGenerator = require('./IdGenerator');

const _ = require('lodash');

const m2m = m2mAuth(
  _.pick(config, ['AUTH0_URL', 'AUTH0_AUDIENCE', 'AUTH0_PROXY_SERVER_URL'])
);

// db informix option
const dbOpts = {
  server: config.DB_SERVER,
  database: config.DB_NAME,
  host: config.DB_HOST,
  protocol: config.DB_PROTOCOL,
  port: config.DB_PORT,
  username: config.DB_USERNAME,
  password: config.DB_PASSWORD,
  locale: config.DB_LOCALE,
};

let idUploadGen = new IDGenerator(config.ID_SEQ_UPLOAD);
let idSubmissionGen = new IDGenerator(config.ID_SEQ_SUBMISSION);
let componentStateGen = new IDGenerator(config.ID_SEQ_COMPONENT_STATE);

// The query to insert into "upload" table
const QUERY_INSERT_UPLOAD = `insert into upload(upload_id, project_id, project_phase_id, resource_id,
  upload_type_id, upload_status_id, parameter, url, create_user, create_date, modify_user, modify_date)
  values(@uploadId@, @challengeId@, @phaseId@, @resourceId@, @uploadType@, @uploadStatusId@,
  @parameter@,@url@, @createUser@, @createDate@, @modifyUser@, @modifyDate@)`;

// The query to insert into "submission" table
const QUERY_INSERT_SUBMISSION = `insert into submission (submission_id, upload_id, submission_status_id,
  submission_type_id, create_user, create_date, modify_user, modify_date) values(@submissionId@,
  @uploadId@, @submissionStatusId@,@submissionTypeId@, @createUser@, @createDate@, @modifyUser@, @modifyDate@)`;

// The query to insert into "resource_submission" table
const QUERY_INSERT_RESOURCE_SUBMISSION = `insert into resource_submission (resource_id,
   submission_id, create_user, create_date, modify_user, modify_date)
  values(@resourceId@, @submissionId@, @createUser@, @createDate@, @modifyUser@, @modifyDate@)`;

// The query to mark record as deleted in "submission" table
const QUERY_DELETE_SUBMISSION = `update submission set submission_status_id =${
  constant.SUBMISSION_STATUS['Deleted']
}
   where upload_id in (select upload_id from upload where project_id=@challengeId@ and resource_id=@resourceId@
   and upload_status_id=${constant.UPLOAD_STATUS['Deleted']})`;

// The query to mark record as deleted in "upload" table
const QUERY_DELETE_UPLOAD = `update upload set upload_status_id =${
  constant.UPLOAD_STATUS['Deleted']
}
  where project_id=@challengeId@ and resource_id=@resourceId@ and upload_id <> @uploadId@`;

// The query by id to mark submission as "deleted" in submission table
const QUERY_DELETE_SUBMISSION_BY_ID = `update submission set submission_status_id =${
  constant.SUBMISSION_STATUS['Deleted']
}
   where submission_id=@submissionId@`;

// The query by id to mark upload as "deleted" in upload table
const QUERY_DELETE_UPLOAD_BY_ID = `update upload set upload_status_id =${
  constant.UPLOAD_STATUS['Deleted']
}
  where upload_id=@uploadId@`;

// The query to get challenge properties
const QUERY_GET_CHALLENGE_PROPERTIES = `select r.resource_id, pi28.value, pp.phase_type_id, pcl.project_type_id
  from project p, project_category_lu pcl, resource r, project_phase pp, outer project_info pi28
  where p.project_category_id = pcl.project_category_id and p.project_id = r.project_id
  and r.user_id = @userId@ and r.resource_role_id = @resourceRoleId@ and p.project_id = pp.project_id
  and pp.project_phase_id = @phaseId@ and p.project_id = pi28.project_id
  and pi28.project_info_type_id = 28 and p.project_id = @challengeId@`;

const QUERY_GET_SUBMISSION_DETAILS = `select upload_id from submission where submission_id=@submissionId@`;

// The query to update url in "upload" table
const QUERY_UPDATE_UPLOAD_BY_SUBMISSION_ID = `update upload set url = @url@ where
  upload_id in (select s.upload_id from submission s, upload uu where uu.upload_id = s.upload_id and s.submission_id = @submissionId@)`;

// The query to update url in "upload" table
const QUERY_UPDATE_UPLOAD = `update upload set url = @url@ where
  upload_id in (select upload_id from
   (select first 1 upload_id from upload where project_id = @challengeId@ and project_phase_id = @phaseId@
   and resource_id = @resourceId@ and upload_status_id = 1 order by create_date desc))`;

// The query to get MM challenge properties
const QUERY_GET_MMCHALLENGE_PROPERTIES = `select rc.round_id, rc.component_id, lcs.long_component_state_id, NVL(lcs.submission_number,0) as submission_number, NVL(lcs.points,0) as points, r.rated_ind
  from project p
  join project_info pi56 on p.project_id = @challengeId@ and p.project_id = pi56.project_id and pi56.project_info_type_id = 56 and p.project_category_id=37
  join informixoltp:round_component rc on rc.round_id =pi56.value
  join informixoltp:round r on rc.round_id=r.round_id
  left join informixoltp:long_component_state lcs on lcs.coder_id= @userId@ and lcs.round_id = rc.round_id and lcs.component_id = rc.component_id`;

// The query to get from "round_registration" table
const QUERY_GET_MM_ROUND_REGISTRATION = `
  select rr.round_id, rr.coder_id from informixoltp:round_registration rr where rr.round_id=@roundId@ and rr.coder_id=@userId@`;

// The query to insert into "round_registration" table
const QUERY_INSERT_MM_ROUND_REGISTRATION = `
  insert into informixoltp:round_registration (round_id, coder_id, timestamp, eligible, team_id)
  values(@roundId@, @userId@, @timestamp@, @eligible@, null)`;

// The query to insert into "long_submission" table
const QUERY_INSERT_LONG_SUBMISSION = `insert into informixoltp:long_submission(long_component_state_id, submission_number,
  submission_text, open_time, submit_time, submission_points, language_id, example) values(@componentStateId@, @numSubmissions@,
  @submissionText@, @openTime@, @submitTime@, @submissionPoints@, @languageId@, @isExample@)`;

// The query to update submission_points in "long_submission" table
const QUERY_UPDATE_LONG_SUBMISSION_SCORE = `update informixoltp:long_submission set submission_points=@reviewScore@ where long_component_state_id=@componentStateId@ and submission_number=@submissionNumber@ and example=0`;

// The query to insert into "long_component_state" table
const QUERY_INSERT_LONG_COMPONENT_STATE = `
  insert into informixoltp:long_component_state
  (long_component_state_id, round_id, coder_id, component_id, points, status_id, submission_number, example_submission_number)
  values(@componentStateId@, @roundId@, @userId@, @componentId@, @points@, @statusId@, @numSubmissions@, @numExampleSubmissions@)`;

// The query to update submission_number in "long_component_state" table
const QUERY_UPDATE_LONG_COMPONENT_STATE_NUN_SUBMISSIONS = `update informixoltp:long_component_state set submission_number=@numSubmissions@
  where long_component_state_id=@componentStateId@`;

// The query to update points in "long_component_state" table
const QUERY_UPDATE_LONG_COMPONENT_STATE_POINTS = `update informixoltp:long_component_state set points=@reviewScore@
  where long_component_state_id=@componentStateId@`;

// The query to get user's submission number in a challenge
const QUERY_GET_SUBMISSION_NUMBER = `
  select count(*)
  from submission s, upload u, resource r
  where s.upload_id = u.upload_id and u.resource_id = r.resource_id and u.project_id = r.project_id
  and u.project_id=@challengeId@ and u.project_phase_id=@phaseId@
  and r.user_id=@userId@ and r.resource_role_id=@resourceRoleId@
  and s.submission_id <= @submissionId@`;

// The query to get initial_score from "submission" table
const QUERY_GET_SUBMISSION_INITIAL_SCORE = `select initial_score from submission where submission_id = @submissionId@`;

// The query to update initial_score in "submission" table
const QUERY_UPDATE_SUBMISSION_INITIAL_REVIEW_SCORE = `update submission set initial_score=@reviewScore@ where submission_id=@submissionId@`;

// The query to update final_score in "submission" table
const QUERY_UPDATE_SUBMISSION_FINAL_REVIEW_SCORE = `update submission set final_score=@finalScore@ where submission_id=@submissionId@`;

// The query to check where user result exists in "long_comp_result" table
const QUERY_CHECK_COMP_RESULT_EXISTS = `select count(*) from informixoltp:long_comp_result where round_id=@roundId@ and coder_id=@userId@`;

// The query to get user's last entry from "long_comp_result" table
const QUERY_GET_LAST_COMP_RESULT = `select first 1 new_rating, new_vol from informixoltp:long_comp_result where round_id < @roundId@ and coder_id=@userId@ and rated_ind = 1 order by round_id desc`;

// The query to insert into "long_comp_result" table
const QUERY_INSERT_COMP_RESULT = `insert into informixoltp:long_comp_result
  (round_id, coder_id, point_total, attended, placed, system_point_total, old_rating, new_rating, old_vol, new_vol, rated_ind, advanced)
  values(@roundId@, @userId@, @initialScore@, 'N', 0, @finalScore@, @oldRating@, null, @oldVol@, null, @ratedInd@, 'N')`;

// The query to update point_total and system_point_total in "long_comp_result" table
const QUERY_UPDATE_COMP_RESULT_SCORE = `update informixoltp:long_comp_result
  set point_total=@initialScore@, system_point_total=@finalScore@, old_rating=@oldRating@, old_vol=@oldVol@, rated_ind=@ratedInd@ where round_id=@roundId@ and coder_id=@userId@`;

// The query to get result from "long_comp_result" table ordered by scores
const QUERY_GET_COMP_RESULT = `select coder_id, placed from informixoltp:long_comp_result where round_id=@roundId@ order by system_point_total desc, point_total desc`;

// The query to update placed in "long_comp_result" table
const QUERY_UPDATE_COMP_RESULT_PLACE = `update informixoltp:long_comp_result set placed = @placed@ where round_id=@roundId@ and coder_id=@userId@`;

/**
 * Get resourceId, isAllowMultipleSubmission, phaseTypeId and challengeTypeId
 * for a given user, challenge id, resource role id and phase id
 *
 * @param {InformixContext} ctx informix db context
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @param {Number} resourceRoleId resource role id
 * @param {Number} phaseId submission phasse id
 * @returns {Array} [resourceId, isAllowMultipleSubmission, phaseTypeId, challengeTypeId]
 */
async function getChallengeProperties(
  ctx,
  challengeId,
  userId,
  resourceRoleId,
  phaseId
) {
  try {
    const result = await ctx.query(QUERY_GET_CHALLENGE_PROPERTIES, {
      challengeId,
      userId,
      resourceRoleId,
      phaseId,
    });
    logger.debug(
      `Challenge properties for: ${challengeId} are: ${JSON.stringify(result)}`
    );

    if (!_.isArray(result) || _.isEmpty(result)) {
      throw new Error(
        `null or empty result get challenge properties for: challengeId ${challengeId}, userId ${userId}, resourceRoleId ${resourceRoleId}, phaseId ${phaseId}`
      );
    }
    return result[0];
  } catch (e) {
    logger.error(e);
    throw e;
  }
}

/**
 * Get mm challenge related properties
 *
 * @param {InformixContext} ctx informix db context
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @returns {Array} [roundId, componentId, componentStateId, numSubmissions, points, ratedInd]
 */
async function getMMChallengeProperties(ctx, challengeId, userId) {
  try {
    const result = await ctx.query(QUERY_GET_MMCHALLENGE_PROPERTIES, {
      challengeId,
      userId,
    });

    logger.debug(
      `MM Challenge properties for: ${challengeId} are: ${JSON.stringify(
        result
      )}`
    );

    if (!_.isArray(result) || _.isEmpty(result)) {
      throw new Error(
        `null or empty result get mm challenge properties for : challenge id ${challengeId}, user id ${userId}`
      );
    }
    return result[0];
  } catch (e) {
    logger.error(e);
    throw e;
  }
}

/**
 * @param {InformixContext} ctx informix db context
 * @param submissionId submission id
 * @return {Array} [uploadId]
 */
async function getSubmissionDetails(ctx, submissionId) {
  try {
    const result = await ctx.query(QUERY_GET_SUBMISSION_DETAILS, {
      submissionId
    });
    logger.debug(
      `Submission details for: ${submissionId} are: ${JSON.stringify(result)}`
    );

    if (!_.isArray(result) || _.isEmpty(result)) {
      throw new Error(
        `null or empty result get Submission properties for: submissionId ${submissionId}`
      );
    }
    return result[0];
  } catch (e) {
    logger.error(e);
    throw e;
  }
}

/**
 * Add submission for marathon match challenge
 *
 * @param {String} newSubmissionId id of new submission from Submission API
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @param {Number} phaseId phase id
 * @param {String} url submission url
 * @param {String} submissionType submission type
 * @param {Number} submissionTime the submission timestamp
 * @private
 */
async function addMMSubmission(
  newSubmissionId,
  challengeId,
  userId,
  phaseId,
  url,
  submissionType,
  submissionTime
) {
  const ctx = new InformixContext(dbOpts);

  try {
    await ctx.begin();

    const [
      resourceId,
      value,
      phaseTypeId,
      challengeTypeId,
    ] = await getChallengeProperties(
      ctx,
      challengeId,
      userId,
      constant.SUBMISSION_TYPE[submissionType].roleId,
      phaseId
    );

    let uploadType = null;
    let submissionId = null;

    let isAllowMultipleSubmission = value === 'true';

    const uploadId = await idUploadGen.getNextId();

    logger.info(`uploadId = ${uploadId}`);

    if (phaseTypeId === constant.PHASE_TYPE['Final Fix']) {
      uploadType = constant.UPLOAD_TYPE['Final Fix'];
    } else {
      submissionId = await idSubmissionGen.getNextId();
      uploadType = constant.UPLOAD_TYPE['Submission'];
    }

    logger.info(
      `add challenge submission for resourceId: ${resourceId}
           uploadId: ${uploadId}
           submissionId: ${submissionId}
           allow multiple submission: ${isAllowMultipleSubmission}
           uploadType: ${uploadType}
           challengeTypeId: ${challengeTypeId}`
    );

    let patchObject;
    const audits = {
      createUser: userId,
      createDate: momentTZ
        .tz(submissionTime, 'America/New_York')
        .format('YYYY-MM-DD HH:mm:ss'),
      modifyUser: userId,
      modifyDate: momentTZ
        .tz(submissionTime, 'America/New_York')
        .format('YYYY-MM-DD HH:mm:ss'),
    };
    let params = {
      uploadId,
      challengeId,
      phaseId,
      resourceId,
      uploadType,
      url,
      uploadStatusId: constant.UPLOAD_STATUS['Active'],
      parameter: 'N/A',
      ...audits,
    };

    logger.debug(`insert upload with params : ${JSON.stringify(params)}`);
    await ctx.query(QUERY_INSERT_UPLOAD, params);

    if (uploadType === constant.UPLOAD_TYPE['Final Fix']) {
      logger.debug(`final fix upload, only insert upload`);
      patchObject = {
        legacyUploadId: uploadId,
      };
    } else {
      params = {
        submissionId,
        uploadId,
        submissionStatusId: constant.SUBMISSION_STATUS['Active'],
        submissionTypeId: constant.SUBMISSION_TYPE[submissionType].id,
        ...audits,
      };
      logger.debug(`insert submission with params : ${JSON.stringify(params)}`);

      await ctx.query(QUERY_INSERT_SUBMISSION, params);

      params = {
        submissionId,
        resourceId,
        submissionStatusId: constant.SUBMISSION_STATUS['Active'],
        submissionTypeId: constant.SUBMISSION_TYPE[submissionType].id,
        ...audits,
      };

      logger.debug(
        `insert resource submission with params : ${JSON.stringify(params)}`
      );
      await ctx.query(QUERY_INSERT_RESOURCE_SUBMISSION, params);

      if (!isAllowMultipleSubmission) {
        logger.debug(
          `delete previous submission for challengeId: ${challengeId} resourceId: ${resourceId} uploadId:${uploadId}`
        );
        const delParams = {
          challengeId,
          resourceId,
          uploadId,
        };
        await ctx.query(QUERY_DELETE_UPLOAD, delParams);
        await ctx.query(QUERY_DELETE_SUBMISSION, delParams);
      }

      let [
        roundId,
        componentId,
        componentStateId,
        numSubmissions,
        points,
      ] = await getMMChallengeProperties(ctx, challengeId, userId);

      logger.debug(
        `get mm challenge properties roundId: ${roundId} componentId: ${componentId} componentStateId: ${componentStateId} numSubmissions: ${numSubmissions} points: ${points}`
      );
      const rrResult = await ctx.query(QUERY_GET_MM_ROUND_REGISTRATION, {
        roundId,
        userId,
      });

      if (_.isEmpty(rrResult)) {
        // Add entry in informixoltp:round_registration
        const rrParams = {
          roundId,
          userId,
          timestamp: moment(submissionTime).format('YYYY-MM-DD HH:mm:ss'),
          eligible: 1,
          teamId: {
            replace: 'null',
          },
        };

        logger.debug(
          `insert round_registration with params : ${JSON.stringify(rrParams)}`
        );
        await ctx.query(QUERY_INSERT_MM_ROUND_REGISTRATION, rrParams);
      } else {
        logger.debug(
          `round_registration already exists, roundId: ${roundId}, userId: ${userId}`
        );
      }

      if (_.isFinite(componentStateId)) {
        // Increment submission_number by 1
        numSubmissions++;

        logger.debug(
          `increment long_component_state#submission_number by 1, componentStateId: ${componentStateId}, numSubmissions: ${numSubmissions}`
        );
        await ctx.query(QUERY_UPDATE_LONG_COMPONENT_STATE_NUN_SUBMISSIONS, {
          componentStateId,
          numSubmissions,
        });
      } else {
        numSubmissions = 1;
        componentStateId = await componentStateGen.getNextId();
        // Add entry in informixoltp:long_component_state
        const lcsParams = {
          componentStateId,
          roundId,
          componentId,
          userId,
          points: 0,
          statusId: constant.COMPONENT_STATE.ACTIVE,
          numSubmissions,
          numExampleSubmissions: 0,
        };

        logger.debug(
          `insert long_component_state with params : ${JSON.stringify(
            lcsParams
          )}`
        );
        await ctx.query(QUERY_INSERT_LONG_COMPONENT_STATE, lcsParams);
      }

      // Add entry in informixoltp:long_submission
      const lsParams = {
        componentStateId,
        numSubmissions,
        submissionText: {
          replace: 'null',
        },
        openTime: submissionTime,
        submitTime: submissionTime,
        submissionPoints: points,
        languageId: constant.LANGUAGE.OTHERS,
        isExample: 0,
      };

      logger.debug(
        `insert long_submission with params : ${JSON.stringify(lsParams)}`
      );
      await ctx.query(QUERY_INSERT_LONG_SUBMISSION, lsParams);

      patchObject = {
        legacySubmissionId: submissionId,
      };
    }

    await ctx.commit();
    await patchSubmission(newSubmissionId, patchObject);
    return patchObject;
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
  }
}

/**
 * Add submission for challenge. The legacy submission id will be patched to Submission API
 *
 * @param {String} newSubmissionId id of new submission from Submission API
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @param {Number} phaseId phase id
 * @param {String} url submission url
 * @param {String} submissionType submission type
 * @param {Number} submissionTime the submission timestamp
 * @param {Boolean} isMM is marathon match challenge
 * @returns {Object} the patch object applied to Submission API
 */
async function addSubmission(
  newSubmissionId,
  challengeId,
  userId,
  phaseId,
  url,
  submissionType,
  submissionTime,
  isMM
) {
  const ctx = new InformixContext(dbOpts);

  try {
    await ctx.begin();

    const [
      resourceId,
      value,
      phaseTypeId,
      challengeTypeId,
    ] = await getChallengeProperties(
      ctx,
      challengeId,
      userId,
      constant.SUBMISSION_TYPE[submissionType].roleId,
      phaseId
    );

    let uploadType = null;
    let submissionId = null;
    let isAllowMultipleSubmission = value === 'true';

    if (challengeTypeId === constant.CHALLENGE_TYPE['Studio'] || isMM) {
      isAllowMultipleSubmission = true;
    }

    logger.debug('Getting uploadId');
    const uploadId = await idUploadGen.getNextId();
    logger.info(`uploadId = ${uploadId}`);

    if (phaseTypeId === constant.PHASE_TYPE['Final Fix']) {
      uploadType = constant.UPLOAD_TYPE['Final Fix'];
    } else {
      submissionId = await idSubmissionGen.getNextId();
      uploadType = constant.UPLOAD_TYPE['Submission'];
    }

    logger.info(
      `add challenge submission for resourceId: ${resourceId}
           uploadId: ${uploadId}
           submissionId: ${submissionId}
           allow multiple submission: ${isAllowMultipleSubmission}
           uploadType: ${uploadType}
           challengeTypeId: ${challengeTypeId}`
    );

    let patchObject;

    const audits = {
      createUser: userId,
      createDate: momentTZ
        .tz(submissionTime, 'America/New_York')
        .format('YYYY-MM-DD HH:mm:ss'),
      modifyUser: userId,
      modifyDate: momentTZ
        .tz(submissionTime, 'America/New_York')
        .format('YYYY-MM-DD HH:mm:ss'),
    };

    let params = {
      uploadId,
      challengeId,
      phaseId,
      resourceId,
      uploadType,
      url,
      uploadStatusId: constant.UPLOAD_STATUS['Active'],
      parameter: 'N/A',
      ...audits,
    };

    logger.debug(`insert upload with params : ${JSON.stringify(params)}`);

    await ctx.query(QUERY_INSERT_UPLOAD, params);

    if (uploadType === constant.UPLOAD_TYPE['Final Fix']) {
      logger.debug(`final fix upload, only insert upload`);
      patchObject = {
        legacyUploadId: uploadId,
      };
    } else {
      params = {
        submissionId,
        uploadId,
        submissionStatusId: constant.SUBMISSION_STATUS['Active'],
        submissionTypeId: constant.SUBMISSION_TYPE[submissionType].id,
        ...audits,
      };
      logger.debug(`insert submission with params : ${JSON.stringify(params)}`);

      await ctx.query(QUERY_INSERT_SUBMISSION, params);
      params = {
        submissionId,
        resourceId,
        submissionStatusId: constant.SUBMISSION_STATUS['Active'],
        submissionTypeId: constant.SUBMISSION_TYPE[submissionType].id,
        ...audits,
      };

      logger.debug(
        `insert resource submission with params : ${JSON.stringify(params)}`
      );
      await ctx.query(QUERY_INSERT_RESOURCE_SUBMISSION, params);

      if (!isAllowMultipleSubmission) {
        logger.debug(
          `delete previous submission for challengeId: ${challengeId} resourceId: ${resourceId} uploadId:${uploadId}`
        );
        const delParams = {
          challengeId,
          resourceId,
          uploadId,
        };
        await ctx.query(QUERY_DELETE_UPLOAD, delParams);
        await ctx.query(QUERY_DELETE_SUBMISSION, delParams);
      }

      patchObject = {
        legacySubmissionId: submissionId,
      };
    }
    await ctx.commit();
    await patchSubmission(newSubmissionId, patchObject);
    return patchObject;
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
  }
}

/**
 * Delete submission id
 * @param {String} id id of submission to delete
 * @return {Promise<void>}
 */
async function deleteSubmission(id) {
  const ctx = new InformixContext(dbOpts);

  try {
    await ctx.begin();

    const [ uploadId ] = await getSubmissionDetails(ctx, id);

    let params = {
      uploadId
    };

    logger.debug(`Delete upload by id with params: ${JSON.stringify(params)}`);
    await ctx.query(QUERY_DELETE_UPLOAD_BY_ID, params);

    params = {
      submissionId: id
    };

    logger.debug(`Delete submission by id with params: ${JSON.stringify(params)}`);
    await ctx.query(QUERY_DELETE_SUBMISSION_BY_ID, params);
    await ctx.commit();
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
  }
}

/**
 * Patch submission
 *
 * @param {String} submissionId submission id in Submission API
 * @param {Object} patchObject the object to patch
 */
async function patchSubmission(submissionId, patchObject) {
  // Axios instance to make calls to the Submission API
  const api = await getSubmissionApi();

  // Patch to the Submission API
  try {
    await api.patch(`/submissions/${submissionId}`, patchObject);
  } catch (err) {
    handleAxiosError(err, 'Submission API');
  }
}

/**
 * Update provisional score for MM challenge
 *
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @param {Number} phaseId phase id
 * @param {String} submissionId submission id
 * @param {String} submissionType submission type
 * @param {Number} reviewScore the provisional review score
 */
async function updateProvisionalScore(
  challengeId,
  userId,
  phaseId,
  submissionId,
  submissionType,
  reviewScore
) {
  logger.debug(`Update provisional score for submission: ${submissionId}`);

  const ctx = new InformixContext(dbOpts);

  try {
    await ctx.begin();

    // Query componentStateId
    const [, , componentStateId] = await getMMChallengeProperties(
      ctx,
      challengeId,
      userId
    );
    if (!componentStateId) {
      throw new Error(
        `MM component state not found, challengeId: ${challengeId}, userId: ${userId}`
      );
    }
    logger.debug(`Get componentStateId: ${componentStateId}`);

    // Query submission number
    const result = await ctx.query(QUERY_GET_SUBMISSION_NUMBER, {
      challengeId,
      userId,
      phaseId,
      submissionId,
      resourceRoleId: constant.SUBMISSION_TYPE[submissionType].roleId,
    });
    const [submissionNumber] = result[0];
    logger.debug(`Get submission number: ${submissionNumber}`);

    // Update the initial_score in submission table
    await ctx.query(QUERY_UPDATE_SUBMISSION_INITIAL_REVIEW_SCORE, {
      submissionId,
      reviewScore,
    });

    // Update the submission_points in informixoltp:long_submission table
    await ctx.query(QUERY_UPDATE_LONG_SUBMISSION_SCORE, {
      componentStateId,
      submissionNumber,
      reviewScore,
    });

    // Update the points in informixoltp:long_component_state table
    await ctx.query(QUERY_UPDATE_LONG_COMPONENT_STATE_POINTS, {
      componentStateId,
      reviewScore,
    });

    await ctx.commit();
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
  }
}

/**
 * Update final score for MM challenge
 *
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @param {String} submissionId submission id
 * @param {Number} finalScore the final review score
 */
async function updateFinalScore(challengeId, userId, submissionId, finalScore) {
  logger.debug(`Update final score for submission: ${submissionId}`);

  const ctx = new InformixContext(dbOpts);

  try {
    await ctx.begin();

    // Query roundId
    let [roundId, , , , , ratedInd] = await getMMChallengeProperties(
      ctx,
      challengeId,
      userId
    );

    if (!roundId) {
      throw new Error(
        `MM round not found, challengeId: ${challengeId}, userId: ${userId}`
      );
    }
    logger.debug(`Get roundId: ${roundId}`);

    // Update the final_score in submission table
    await ctx.query(QUERY_UPDATE_SUBMISSION_FINAL_REVIEW_SCORE, {
      submissionId,
      finalScore,
    });

    // Get initial_score from submission table
    let result = await ctx.query(QUERY_GET_SUBMISSION_INITIAL_SCORE, {
      submissionId,
    });
    const [initialScore] = result[0];

    // Check whether user result exists in informixoltp:long_comp_result
    result = await ctx.query(QUERY_CHECK_COMP_RESULT_EXISTS, {
      roundId,
      userId,
    });
    const [resultExists] = result[0];

    ratedInd = ratedInd ? 1 : 0;
    const params = {
      roundId,
      userId,
      initialScore: _.isFinite(initialScore)
        ? initialScore
        : {
            replace: 0,
          },
      finalScore,
      ratedInd,
    };

    let userLastCompResult;
    if (ratedInd) {
      logger.debug('Rated Match - Get previous Rating and Vol');

      // Find user's last entry from informixoltp:long_comp_result
      const userLastCompResultArr = await ctx.query(
        QUERY_GET_LAST_COMP_RESULT,
        params
      );

      if (_.isArray(userLastCompResultArr) && userLastCompResultArr.length) {
        userLastCompResult = userLastCompResultArr[0];
      }

      logger.debug(`Old Rating and Vol values ${userLastCompResult}`);
    }

    if (userLastCompResult) {
      params.oldRating = _.isFinite(userLastCompResult[0])
        ? userLastCompResult[0]
        : {
            replace: 'null',
          };
      params.oldVol = _.isFinite(userLastCompResult[1])
        ? userLastCompResult[1]
        : {
            replace: 'null',
          };
    } else {
      params.oldRating = {
        replace: 'null',
      };
      params.oldVol = {
        replace: 'null',
      };
    }

    if (resultExists) {
      // Update the long_comp_result table
      logger.debug(
        `Update long_comp_result with params: ${JSON.stringify(params)}`
      );
      await ctx.query(QUERY_UPDATE_COMP_RESULT_SCORE, params);
    } else {
      // Add entry in long_comp_result table
      logger.debug(
        `Insert into long_comp_result with params: ${JSON.stringify(params)}`
      );
      await ctx.query(QUERY_INSERT_COMP_RESULT, params);
    }

    // Update placed
    result = await ctx.query(QUERY_GET_COMP_RESULT, params);
    for (let i = 1; i <= result.length; i++) {
      const r = result[i - 1];
      if (i !== r[1]) {
        await ctx.query(QUERY_UPDATE_COMP_RESULT_PLACE, {
          placed: i,
          roundId,
          userId: r[0],
        });
      }
    }

    await ctx.commit();
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
  }
}

/**
 * Update upload url of latest user submission
 * If submission id is provided then it will be used
 *
 * @param {Number} challengeId challenge id
 * @param {Number} userId user id
 * @param {Number} phaseId phase id
 * @param {String} url new url value
 * @param {String} submissionType submission type
 * @param {Number} submissionId submission id
 */
async function updateUpload(
  challengeId,
  userId,
  phaseId,
  url,
  submissionType,
  submissionId
) {
  const ctx = new InformixContext(dbOpts);

  try {
    await ctx.begin();
    let sql;
    let params;

    if (submissionId > 0) {
      sql = QUERY_UPDATE_UPLOAD_BY_SUBMISSION_ID;
      params = {
        url,
        submissionId,
      };
    } else {
      logger.warn('no valid submission id');
      const [resourceId] = await getChallengeProperties(
        ctx,
        challengeId,
        userId,
        constant.SUBMISSION_TYPE[submissionType].roleId,
        phaseId
      );
      sql = QUERY_UPDATE_UPLOAD;
      params = {
        url,
        challengeId,
        phaseId,
        resourceId,
      };
    }
    logger.debug(
      `update upload with sql ${sql} and params ${JSON.stringify(params)}`
    );
    await ctx.query(sql, params);
    await ctx.commit();
    return;
  } catch (e) {
    await ctx.rollback();
    throw e;
  } finally {
    await ctx.end();
  }
}

/**
 * Get the Axios instance to make calls to the Submission API
 * @returns {Object} Axios instance to make calls to the Submission API
 * @private
 */
async function getSubmissionApi() {
  const options = await getAxioOptions();
  options.baseURL = config.SUBMISSION_API_URL;
  options.timeout = config.SUBMISSION_TIMEOUT;

  return Axios.create(options);
}

/**
 * Get Axio options.
 * @returns {Object} Axio options
 * @private
 */
async function getAxioOptions() {
  const options = {};
  if (
    process.env.NODE_ENV !== 'test' &&
    process.env.NODE_ENV !== 'development'
  ) {
    // For test/development will use mock api, no need m2m token
    const token = await m2m.getMachineToken(
      config.AUTH0_CLIENT_ID,
      config.AUTH0_CLIENT_SECRET
    );
    options.headers = {
      Authorization: `Bearer ${token}`,
    };
  }

  return options;
}

/**
 * Handle Axios error.
 * @param err the Axios error occurred
 * @param apiName the api name
 * @private
 */
function handleAxiosError(err, apiName) {
  if (err.response) {
    // non-2xx response received
    logger.error(
      `${apiName} Error: ${Flatted.stringify(
        {
          data: err.response.data,
          status: err.response.status,
          headers: err.response.headers,
        },
        null,
        2
      )}`
    );
  } else if (err.request) {
    // request sent, no response received
    logger.error(
      `${apiName} Error (request sent, no response): ${Flatted.stringify(
        err.request,
        null,
        2
      )}`
    );
  } else {
    logger.error(util.inspect(err));
  }
  throw err;
}

/**
 * Get the submission from submission API
 * @param {String} submissionId submission id
 * @returns {Object} submission object
 */
async function getSubmission(submissionId) {
  try {
    const api = await getSubmissionApi();

    logger.debug(`Fetching submission for ${submissionId}`);
    const sub = await api.get(`/submissions/${submissionId}`);
    logger.debug(`Fetched submission for ${submissionId}`);

    return sub.data;
  } catch (err) {
    handleAxiosError(err, 'Submission API');
  }
}

/**
 * Get the subtrack for a challenge.
 * @param {Number} challengeId The id of the challenge.
 * @returns {String} The subtrack type of the challenge.
 */
async function getSubTrack(challengeId) {
  try {
    const options = await getAxioOptions();

    let challengeURL = config.CHALLENGE_INFO_API.replace('{cid}', challengeId);
    logger.debug(
      `fetching challenge details for ${challengeId} using ${challengeURL}`
    );

    // attempt to fetch the subtrack
    const result = await Axios.get(challengeURL, options);

    // use _.get to avoid access with undefined object
    return _.get(result.data[0], 'legacy.subTrack');
  } catch (err) {
    handleAxiosError(err, 'Challenge Details API');
  }
}

module.exports = {
  addSubmission,
  deleteSubmission,
  addMMSubmission,
  updateUpload,
  updateProvisionalScore,
  updateFinalScore,
  patchSubmission,
  getSubmission,
  getSubTrack,
};
