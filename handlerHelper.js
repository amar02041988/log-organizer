'use strict';

const { DeleteMessageCommand, SQSClient } = require("@aws-sdk/client-sqs");
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const retry = require('@lifeomic/attempt').retry;
const ConsoleLogger = require('./ConsoleLogger');
const logger = new ConsoleLogger();
const crypto = require('crypto');

// Environment variables configuration
const env = {
    S3_KEY_BASE_PATH: process.env.S3_KEY_BASE_PATH,
    DEFAULT_DELAY_IN_MILLIS: process.env.DEFAULT_DELAY_IN_MILLIS,
    DEFAULT_MIN_DELAY_IN_MILLIS: process.env.DEFAULT_MIN_DELAY_IN_MILLIS,
    DEFAULT_MAX_DELAY_IN_MILLIS: process.env.DEFAULT_MAX_DELAY_IN_MILLIS,
    DEFAULT_MAX_ATTEMPTS: process.env.DEFAULT_MAX_ATTEMPTS,
    DEFAULT_IS_JITTER: process.env.DEFAULT_IS_JITTER
};

// Initialize AWS clients
const sqsClient = new SQSClient({});
const s3Client = new S3Client({});

// Retry configuration for AWS ids based on id passed.
const getRetryOptions = (id = "DEFAULT") => {
    return {
        delay: parseInt(env[`${id}_DELAY_IN_MILLIS`]),
        minDelay: parseInt(env[`${id}_MIN_DELAY_IN_MILLIS`]),
        maxDelay: parseInt(env[`${id}_MAX_DELAY_IN_MILLIS`]),
        maxAttempts: parseInt(env[`${id}_MAX_ATTEMPTS`]),
        jitter: env[`${id}_IS_JITTER`] === "true",
        handleError(err, context) {
            logger.info(`Retrying ${id} ${context.attemptNum}, date time: ${new Date().toISOString()}`);
        }
    }
};

/**
 * Validates required environment variables
 * @param {string[]} required - Array of required environment variable names
 * @throws {Error} If any required variable is missing
 */
const validateEnvironment = (required) => {
    const missing = required.filter(name => !process.env[name]);
    if (missing.length > 0) {
        throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
    }
};

/**
 * Validates if all required fields are present in a record
 * @param {Object} record - The record to validate
 * @param {string[]} requiredFields - Array of required field names
 * @returns {string[]} Array of missing field names, empty if all fields present
 */
const validateRequiredFields = (record, requiredFields = []) => {
    logger.debug(`Validating record with type (${typeof record}): ${JSON.stringify(record)}`);
    if (!record || typeof record !== 'object') {
        return requiredFields; // All fields are missing if record is invalid
    }

    return requiredFields.filter(field => !record[field]);
}

/**
 * Explicitly deletes a message from SQS queue to avoid duplicate processing
 * @param {Object} event - The event containing queue URL and message details
 * @throws {Error} If deletion fails after all retries
 */
const deleteSQSMessage = async (event) => {
    const queueUrl = event.queueUrl;
    try {
        logger.debug(`DELETING... the message from sqs queue ${queueUrl} with receipt handle: ${event.receiptHandle}, message Id: ${event.messageId}`);

        await retry(async () => {
            await sqsClient.send(
                new DeleteMessageCommand({
                    QueueUrl: queueUrl,
                    ReceiptHandle: event.receiptHandle
                })
            );
        }, getRetryOptions());

        logger.debug(`DELETED the message from sqs queue ${queueUrl} with receipt handle: ${event.receiptHandle}, message Id: ${event.messageId}`);
    } catch (err) {
        const errorMsg = `Error in deleting the message from sqs queue ${queueUrl} with receipt handle: ${event.receiptHandle}, message Id: ${event.messageId}`;
        logger.error(`${errorMsg}: ${err}`);
        throw new Error(errorMsg, { cause: err });
    }
};

/**
 * Extracts queue URL from an ARN
 * @param {string} arn - The ARN to parse
 * @returns {string} The queue URL
 */
const getQueueUrlFromArn = (arn) => {
    const parts = arn.split(':');
    const [service, region, accountId, queueName] = parts.slice(2);
    return `https://${service}.${region}.amazonaws.com/${accountId}/${queueName}`;
};

/**
 * Groups records by their partitioning attributes
 * @param {Array} records - Array of parsed log records
 * @returns {Object} Records grouped by their partition key
 */
// const MANDATORY_FIELDS = ['messageType', 'projectCode', 'partner', 'customer', 'region', 'country', 'component'];

// const validateRecord = (record) => {
//     const missingFields = MANDATORY_FIELDS.filter(field => !record[field]);
//     return missingFields.length === 0 ? null : missingFields;
// };

const hashedApiKeysMap = {};
const hashApiKey = (apiKey) => {
    if (!apiKey) {
        return "undefined";
    }

    let hashedApiKey = hashedApiKeysMap[apiKey];
    if (hashedApiKey) {
        return hashedApiKey;
    }
    hashedApiKey = crypto.createHash('sha256').update(apiKey, 'utf8').digest('hex');
    hashedApiKeysMap[apiKey] = hashedApiKey;
    return hashedApiKey;
}

const groupRecordsByPartition = (records) => {
    const groups = new Map();
    let skippedRecords = 0;

    for (const record of records) {
        const dataRecord = record.dataRecord;
        // const missingFields = validateRecord(dataRecord);
        // if (missingFields) {
        //     logger.error(`Skipping record due to missing mandatory fields [${missingFields.join(', ')}]: ${JSON.stringify(dataRecord)}`);
        //     skippedRecords++;
        //     continue;
        // }

        const timestamp = new Date(dataRecord.dateTime);
        let partitionKey = [
            `message_type=${dataRecord.messageType}`,
            `project_code=${dataRecord.projectCode}`,
            `partner=${dataRecord.partner}`,
            `customer=${dataRecord.customer}`,
            `client_id=${dataRecord.clientId}`,
            `hashed_api_key=${hashApiKey(dataRecord.apiKeyId)}`,
            `region=${dataRecord.region}`,
            `country=${dataRecord.country}`,
            `component=${dataRecord.component}`,
            `year=${timestamp.getUTCFullYear()}`,
            `month=${String(timestamp.getUTCMonth() + 1).padStart(2, '0')}`,
            `day=${String(timestamp.getUTCDate()).padStart(2, '0')}`,
            `hour=${String(timestamp.getUTCHours()).padStart(2, '0')}`
        ].filter(Boolean).join('/');

        if(dataRecord.env){
            partitionKey = `env=${dataRecord.env}/`+ partitionKey;
        }

        if (!groups.has(partitionKey)) {
            groups.set(partitionKey, {
                key: partitionKey,
                records: []
            });
        }

        const group = groups.get(partitionKey);
        group.records.push(record);
    }

    return Array.from(groups.values());
};

/**
 * Saves a group of records to S3 in a single file
 * @param {string} bucketName - The S3 bucket name
 * @param {Object} group - Group of records with the same partition key
 * @returns {string} The S3 key where records were saved
 */
const saveRecordsGroupToS3 = async (bucketName, group) => {
    try {
        const key = `${group.key}/${uuidv4()}.json`;

        // Convert records to JSONL format (one JSON object per line)
        const jsonlContent = group.records.map(record => JSON.stringify(record.dataRecord)).join('\n');

        await retry(async () => {
            await s3Client.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: `${env.S3_KEY_BASE_PATH}/${key}`,
                Body: jsonlContent,
                ContentType: 'application/x-ndjson'
            }));
        }, getRetryOptions());

        logger.debug(`Successfully saved ${group.records.length} records to S3 in JSONL format: ${bucketName}/${key}`);
        return key;
    } catch (err) {
        const errorMsg = `Failed to save record group to S3 with key ${group.key}`;
        logger.error(`${errorMsg}: ${err.message}`);
        throw new Error(errorMsg, { cause: err });
    }
};

module.exports = {
    validateEnvironment,
    validateRequiredFields,
    deleteSQSMessage,
    getQueueUrlFromArn,
    groupRecordsByPartition,
    saveRecordsGroupToS3
};