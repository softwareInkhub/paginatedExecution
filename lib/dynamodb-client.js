import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import dotenv from 'dotenv';

dotenv.config();

// Get credentials based on environment
const getCredentials = () => {
  // In Lambda, use the default AWS credentials
  if (process.env.AWS_LAMBDA_FUNCTION_VERSION) {
    return undefined; // AWS SDK will automatically use Lambda's credentials
  }
  
  // For local development, use the LOCAL_ prefixed credentials
  return {
    accessKeyId: process.env.LOCAL_AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.LOCAL_AWS_SECRET_ACCESS_KEY
  };
};

const config = {
  region: process.env.AWS_LAMBDA_FUNCTION_VERSION 
    ? process.env.AWS_REGION 
    : process.env.LOCAL_AWS_REGION || "us-east-1",
  ...(getCredentials() ? { credentials: getCredentials() } : {})
};

console.log('[DynamoDB] Using configuration:', {
  region: config.region,
  hasCredentials: !!config.credentials?.accessKeyId && !!config.credentials?.secretAccessKey,
  isLambda: !!process.env.AWS_LAMBDA_FUNCTION_VERSION
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