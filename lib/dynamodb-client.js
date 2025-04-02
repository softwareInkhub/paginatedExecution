import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import dotenv from 'dotenv';

dotenv.config();

const config = {
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
};

console.log('[DynamoDB] Using configuration:', {
  region: config.region,
  hasCredentials: !!config.credentials.accessKeyId && !!config.credentials.secretAccessKey
});

// Create the DynamoDB client instance
const client = new DynamoDBClient(config);

// Create the DynamoDB Document client with custom marshalling options
const marshallOptions = {
  convertEmptyValues: false,
  removeUndefinedValues: true,
  convertClassInstanceToMap: false
};

const unmarshallOptions = {
  wrapNumbers: false
};

const translateConfig = { marshallOptions, unmarshallOptions };

// Create Document Client
const docClient = DynamoDBDocumentClient.from(client, translateConfig);

export { client, docClient }; 