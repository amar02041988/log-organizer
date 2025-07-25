const { validateEnvironment, validateRequiredFields, saveRecordsGroupToS3, deleteSQSMessage, getQueueUrlFromArn, groupRecordsByPartition } = require('./handlerHelper.js');
const ConsoleLogger = require('./ConsoleLogger.js');
const logger = new ConsoleLogger();

const REQUIRED_ENV_VARS = ['STAGE', 'PM_BUCKET_NAME', 'S3_KEY_BASE_PATH'];
const REQUIRED_FIELDS = ['messageType', 'projectCode', 'component'];

/**
 * Processes SQS messages and stores them in S3 using Hive-style partitioning
 * @param {Object} event - The Lambda event containing SQS records
 * @param {Object} context - The Lambda context
 * @returns {Object} Lambda response with statusCode and body
 * @throws {Error} If processing fails for any record
 */
module.exports.handle = async function (event, context) {

    logger.debug(`Received event: ${JSON.stringify(event)}`);
    // Process each group
    const successfulRecords = [];
    const failedRecords = [];
    let groupsProcessed = 0;

    try {
        // Validate environment variables first
        validateEnvironment(REQUIRED_ENV_VARS);

        // Parse all records and attach SQS message details
        const parsedRecords = [];
        for (const sqsRecord of event.Records) {
            try {
                let dataRecord = undefined;

                // Parse the SQS message body
                try {
                    dataRecord = JSON.parse(sqsRecord.body);
                } catch (err) {
                    logger.error(`Skipping record ${sqsRecord.messageId} due to parsing failure: ${err.message}`);
                    failedRecords.push({ messageId: sqsRecord.messageId, error: `Failed to parse JSON body: ${err.message}` });
                    continue;
                }

                // Validate required fields in the record
                const missingFields = validateRequiredFields(dataRecord, REQUIRED_FIELDS);
                logger.debug(`Record ${sqsRecord.messageId} missing fields: ${missingFields}`);
                if (missingFields.length > 0) {
                    logger.error(`Skipping record ${sqsRecord.messageId} due to missing mandatory fields [${missingFields.join(', ')}]`);
                    failedRecords.push({ messageId: sqsRecord.messageId, error: `Missing mandatory fields: ${missingFields.join(', ')}` });
                    continue;
                }

                const record = {
                    dataRecord: dataRecord,
                    // Attach SQS message details to the record for later deletion
                    sqsConfig: {
                        receiptHandle: sqsRecord.receiptHandle,
                        messageId: sqsRecord.messageId,
                        queueUrl: getQueueUrlFromArn(sqsRecord.eventSourceARN)
                    }
                };
                parsedRecords.push(record);
            } catch (err) {
                logger.error(`Failed to parse record ${sqsRecord.messageId}: ${err.message}`);
                // Continue processing other records
            }
        }

        // Group records by partition attributes
        const groups = groupRecordsByPartition(parsedRecords);

        for (const group of groups) {
            try {
                // Save group to S3
                const s3Key = await saveRecordsGroupToS3(process.env.PM_BUCKET_NAME, group);

                // Delete SQS messages for this group after successful S3 upload
                for (const record of group.records) {
                    const sqsDetails = record.sqsConfig;
                    try {
                        await deleteSQSMessage({
                            receiptHandle: sqsDetails.receiptHandle,
                            messageId: sqsDetails.messageId,
                            queueUrl: sqsDetails.queueUrl
                        });
                        successfulRecords.push({ messageId: sqsDetails.messageId, s3Key });
                    } catch (err) {
                        logger.error(`Failed to delete SQS message ${sqsDetails.messageId}: ${err.message}`);
                        failedRecords.push({ messageId: sqsDetails.messageId, error: err.message });
                        // Continue processing other messages even if one fails
                    }
                }
                groupsProcessed++;
            } catch (err) {
                logger.error(`Failed to process group ${group.key}: ${err.message}`);
                // Add all records in the failed group to failedRecords
                for (const record of group.records) {
                    failedRecords.push({ messageId: record.sqsConfig.messageId, error: err.message });
                }
                logger.debug(`Error in group ${group.key}: ${JSON.stringify(failedRecords)}`);
                // Continue processing other groups
            }
        }

        // Return summary of processing results
        return {
            statusCode: 200,
            body: {
                totalRecords: event.Records.length,
                successfulRecords: successfulRecords.length,
                failedRecords: failedRecords.length,
                groupsProcessed: groupsProcessed
            }
        };
    } catch (err) {
        logger.error(`Fatal error in handler: ${err.message}`);
        throw err;
    }
};
