const handler = require('../handler');
const { deleteSQSMessage } = require('../handlerHelper');

// Only mock the SQS delete message function
jest.mock('../handlerHelper', () => {
    const actual = jest.requireActual('../handlerHelper');
    return {
        ...actual,
        deleteSQSMessage: jest.fn()
    };
});

describe('Log Organizer Handler Integration Tests', () => {
    const mockSQSEvent = {
        Records: [
            {
                messageId: 'integration-test-msg-1',
                receiptHandle: 'integration-test-receipt-1',
                eventSourceARN: 'arn:aws:sqs:eu-west-1:123456789012:test-queue',
                body: JSON.stringify({
                    "messageType": "audit_record",
                    "projectCode": "intel",
                    // "region": "eu",
                    // "country": "it",
                    "dateTime": "2025-05-17T19:50:16.306Z",
                    "correlationId": "151e6592-3e40-4b45-a747-e4a2f8155c88",
                    "component": "test-ip-intel",
                    "filename": "test-sample",
                    "customer": "test-customer",
                    "partner": "test-partner",

                    "stepCategory": "invocation",
                    "stepStatus": "success",
                    "entity": "test-ip",
                    "workflowInfo": "Request Completed",
                    "recordAuditFlag": true
                })
            },
            {
                messageId: 'integration-test-msg-2',
                receiptHandle: 'integration-test-receipt-2',
                eventSourceARN: 'arn:aws:sqs:eu-west-1:123456789012:test-queue',
                body: JSON.stringify({
                    "messageType": "audit_record",
                    "projectCode": "intel",
                    // "region": "eu",
                    // "country": "it",
                    "dateTime": "2025-05-17T19:50:16.306Z",
                    "correlationId": "16666592-3e40-4b45-a747-e4a2f8155c89",
                    "component": "test-ip-intel",
                    "filename": "test-sample",
                    "customer": "test-customer",
                    "partner": "test-partner",
                    "stepCategory": "invocation",
                    "stepStatus": "success",
                    "entity": "test-ip",
                    "workflowInfo": "Request Completed",
                    "recordAuditFlag": true
                })
            }
        ]
    };

    beforeEach(() => {
        jest.clearAllMocks();
        deleteSQSMessage.mockResolvedValue({});
    });

    test('Successfully processes and stores log records in S3', async () => {
        const result = await handler.handle(mockSQSEvent, {});

        console.log(`------------------ ${JSON.stringify(result)}`);

        // Verify successful execution
        expect(result.statusCode).toBe(200);
        expect(result.body.totalRecords).toBe(2);
        expect(result.body.successfulRecords).toBe(2);
        expect(result.body.failedRecords).toBe(0);
        expect(result.body.groupsProcessed).toBe(1); // Both records should be in the same group

        // Verify SQS delete was called for each message
        expect(deleteSQSMessage).toHaveBeenCalledTimes(2);
        expect(deleteSQSMessage).toHaveBeenCalledWith({
            receiptHandle: 'integration-test-receipt-1',
            messageId: 'integration-test-msg-1',
            queueUrl: expect.stringContaining('sqs.eu-west-1.amazonaws.com')
        });
        expect(deleteSQSMessage).toHaveBeenCalledWith({
            receiptHandle: 'integration-test-receipt-2',
            messageId: 'integration-test-msg-2',
            queueUrl: expect.stringContaining('sqs.eu-west-1.amazonaws.com')
        });


    });


    test('Processes records with different customer separately', async () => {
        // Create event with different customer
        const mixedTypeEvent = {
            Records: [
                {
                    ...mockSQSEvent.Records[0],
                    messageId: 'mixed-type-1',
                    receiptHandle: 'mixed-receipt-1',
                    body: JSON.stringify({
                        ...JSON.parse(mockSQSEvent.Records[0].body),
                        customer: 'test-cust-1'
                    })
                },
                {
                    ...mockSQSEvent.Records[1],
                    messageId: 'mixed-type-2',
                    receiptHandle: 'mixed-receipt-2',
                    body: JSON.stringify({
                        ...JSON.parse(mockSQSEvent.Records[1].body),
                        customer: 'test-cust-2'
                    })
                }
            ]
        };

        const result = await handler.handle(mixedTypeEvent, {});

        // Verify successful execution
        expect(result.statusCode).toBe(200);
        expect(result.body.totalRecords).toBe(2);
        expect(result.body.successfulRecords).toBe(2);
        expect(result.body.failedRecords).toBe(0);
        expect(result.body.groupsProcessed).toBe(2); // Should be in different groups due to different customer

        // Verify SQS delete was called for each message
        expect(deleteSQSMessage).toHaveBeenCalledTimes(2);
    });

});
