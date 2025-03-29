import express from 'express';
import { OpenAPIBackend } from 'openapi-backend';
import swaggerUi from 'swagger-ui-express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import yaml from 'js-yaml';
import { v4 as uuidv4 } from 'uuid';
import cors from 'cors'
 import { handlers as dynamodbHandlers } from './lib/dynamodb-handlers.js';
import dotenv from 'dotenv';
import { savePaginatedExecutionLogs } from './executionHandler.js';
import serverless from 'serverless-http';


dotenv.config();  

const app = express();
app.use(express.json());
app.use(cors());
// File storage configuration
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);



app.get("/test",(req,res)=>{res.send("hello! world test");
})

// Initialize main OpenAPI backend
const mainApi = new OpenAPIBackend({
  definition: './openapi.yaml',
  quick: true,
  handlers: {
    validationFail: async (c, req, res) => ({
      statusCode: 400,
      error: c.validation.errors
    }),
    notFound: async (c, req, res) => ({
      statusCode: 404,
      error: 'Not Found'
    }),

     executeNamespacePaginatedRequest: async (c, req, res) => {
      console.log('\n=== PAGINATED REQUEST START ===');
      console.log('Request details:', {
        method: c.request.requestBody.method,
        url: c.request.requestBody.url,
        maxIterations: c.request.requestBody.maxIterations || 10,
        queryParams: c.request.requestBody.queryParams,
        headers: c.request.requestBody.headers,
        tableName: c.request.requestBody.tableName,
        saveData: c.request.requestBody.saveData
      });

      const { 
        method, 
        url, 
        maxIterations = 10,
        queryParams = {}, 
        headers = {}, 
        body = null,
        tableName,
        saveData
      } = c.request.requestBody;

      let currentUrl = url;
      let lastError = null;
      const execId = uuidv4();
      let executionLogs;

      try {
        // Initialize execution logs
        executionLogs = await savePaginatedExecutionLogs({
          execId,
          method,
          url,
          queryParams,
          headers,
          maxIterations,
          tableName,
          saveData
        });

        if (!executionLogs) {
          throw new Error('Failed to initialize execution logs');
        }

        // Return immediately with execution ID and initial status
        const initialResponse = {
          statusCode: 200,
          body: {
            status: 200,
            data: {
              executionId: execId,
              status: 'initialized',
              method,
              url,
              maxIterations,
              timestamp: new Date().toISOString()
            }
          }
        };

        // Start processing in the background
        (async () => {
          try {
            const pages = [];
            let pageCount = 1;
            let hasMorePages = true;
            let detectedPaginationType = null;
            let totalItemsProcessed = 0;

            // Update parent execution status to inProgress
            await executionLogs.updateParentStatus('inProgress', false);

            // Function to save items to DynamoDB
            const saveItemsToDynamoDB = async (items, pageData) => {
              if (!saveData || !tableName || items.length === 0) return [];

              console.log(`\nSaving ${items.length} items to DynamoDB table: ${tableName}`);
              
              const timestamp = new Date().toISOString();
              const baseRequestDetails = {
                method,
                url: pageData.url,
                queryParams,
                headers,
                body
              };

              const BATCH_SIZE = 5;
              const batches = [];
              const savedItemIds = [];
              
              for (let i = 0; i < items.length; i += BATCH_SIZE) {
                batches.push(items.slice(i, i + BATCH_SIZE));
              }

              for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
                const batch = batches[batchIndex];
                console.log(`Processing batch ${batchIndex + 1}/${batches.length} (${batch.length} items)`);

                const savePromises = batch.map(async (item, index) => {
                  // Create a clean copy of the item
                  const cleanedItem = { ...item };
                  
                  // Ensure id is a string
                  if (typeof cleanedItem.id === 'number') {
                    cleanedItem.id = cleanedItem.id.toString();
                  }

                  // Remove bookmark and url fields from the item
                  const { bookmark, url, ...itemWithoutBookmark } = cleanedItem;

                  // Keep only essential fields and primitive values
                  const simplifiedItem = Object.entries(itemWithoutBookmark).reduce((acc, [key, value]) => {
                    if (typeof value === 'string' || 
                        typeof value === 'number' || 
                        typeof value === 'boolean' ||
                        value === null ||
                        Array.isArray(value) ||
                        (typeof value === 'object' && value !== null)) {
                      acc[key] = value;
                    }
                    return acc;
                  }, {});
                  
                  const itemId = cleanedItem.id || `item_${timestamp}_${batchIndex}_${index}`;
                  const itemData = {
                    id: itemId,
                    Item: simplifiedItem,
                    timestamp,
                    _metadata: {
                      requestDetails: baseRequestDetails,
                      status: pageData.status,
                      itemIndex: batchIndex * BATCH_SIZE + index,
                      totalItems: items.length,
                      originalId: item.id
                    }
                  };

                  try {
                    const dbResponse = await dynamodbHandlers.createItem({
                      request: {
                        params: {
                          tableName
                        },
                        requestBody: itemData
                      }
                    });

                    if (!dbResponse.ok) {
                      console.error('Failed to save item:', dbResponse);
                      return null;
                    }

                    console.log(`Successfully saved item ${batchIndex * BATCH_SIZE + index + 1}/${items.length} with ID: ${itemId}`);
                    savedItemIds.push(itemId);
                    return itemId;
                  } catch (error) {
                    console.error(`Error saving item ${batchIndex * BATCH_SIZE + index + 1}:`, error);
                    return null;
                  }
                });

                await Promise.all(savePromises);
                console.log(`Completed batch ${batchIndex + 1}/${batches.length}`);
              }

              console.log(`Completed saving ${items.length} items to DynamoDB. Saved IDs:`, savedItemIds);
              return savedItemIds;
            };

            // Function to detect pagination type from response
            const detectPaginationType = (response) => {
              // Check for Link header pagination (Shopify style)
              if (response.headers.link && response.headers.link.includes('rel="next"')) {
                return 'link';
              }
              
              // Check for bookmark pagination (Pinterest style)
              if (response.data && response.data.bookmark) {
                return 'bookmark';
              }

              // Check for cursor-based pagination
              if (response.data && (response.data.next_cursor || response.data.cursor)) {
                return 'cursor';
              }

              // Check for offset/limit pagination
              if (response.data && (response.data.total_count !== undefined || response.data.total !== undefined)) {
                return 'offset';
              }

              return null;
            };

            // Extract next URL from Link header (Shopify)
            const extractNextUrl = (linkHeader) => {
              if (!linkHeader) return null;
              const matches = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
              return matches ? matches[1] : null;
            };

            // Extract bookmark from response (Pinterest)
            const extractBookmark = (responseData) => {
              if (!responseData) return null;
              return responseData.bookmark || null;
            };

            // Extract cursor from response
            const extractCursor = (responseData) => {
              if (!responseData) return null;
              return responseData.next_cursor || responseData.cursor || null;
            };

            while (hasMorePages && pageCount <= maxIterations) {
              console.log(`\n=== PAGE ${pageCount} START ===`);
              
              // Build URL with query parameters
              const urlObj = new URL(currentUrl);
              
              // Only add query parameters if they're not already in the URL and it's the first page
              if (pageCount === 1) {
                Object.entries(queryParams).forEach(([key, value]) => {
                  if (value && !urlObj.searchParams.has(key)) {
                    urlObj.searchParams.append(key, value);
                  }
                });
              }

              // Make request
              console.log('Making request to:', urlObj.toString());
              const response = await axios({
                method: method.toUpperCase(),
                url: urlObj.toString(),
                headers: headers,
                data: !['GET', 'HEAD'].includes(method.toUpperCase()) ? body : undefined,
                validateStatus: () => true
              });

              console.log('Response received:', {
                status: response.status,
                statusText: response.statusText,
                headers: response.headers,
                dataLength: response.data ? JSON.stringify(response.data).length : 0,
                data: response.data
              });

              // Handle API errors
              if (response.status >= 400) {
                lastError = {
                  status: response.status,
                  statusText: response.statusText,
                  data: response.data,
                  url: urlObj.toString()
                };
                console.error(`\nAPI Error on page ${pageCount}:`, lastError);
                
                // For Shopify API, check if it's a rate limit error
                if (response.status === 429 || 
                    (response.data && 
                     response.data.errors && 
                     (Array.isArray(response.data.errors) ? 
                       response.data.errors.some(err => err.includes('rate limit')) :
                       response.data.errors.toString().includes('rate limit')))) {
                  console.log('Rate limit detected, waiting before retry...');
                  await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
                  continue; // Retry the same page
                }
                
                // For other errors, stop pagination
                hasMorePages = false;
                break;
              }

              // Detect pagination type on first request if not specified
              if (pageCount === 1) {
                detectedPaginationType = detectPaginationType(response);
                console.log('Detected pagination type:', detectedPaginationType);
              }

              // Process response data
              let currentPageItems = [];
              if (response.data) {
                // Handle different response structures
                if (Array.isArray(response.data)) {
                  currentPageItems = response.data;
                } else if (response.data.data && Array.isArray(response.data.data)) {
                  currentPageItems = response.data.data;
                } else if (response.data.items && Array.isArray(response.data.items)) {
                  currentPageItems = response.data.items;
                } else if (response.data.orders && Array.isArray(response.data.orders)) {
                  currentPageItems = response.data.orders;
                } else {
                  currentPageItems = [response.data];
                }

                console.log('Extracted current page items:', {
                  count: currentPageItems.length,
                  firstItem: currentPageItems[0]
                });
              }

              // Extract IDs from items
              const itemIds = currentPageItems.map(item => {
                const id = item.id || item.Id || item.ID || item._id || 
                          item.pin_id || item.board_id || 
                          item.order_id || item.product_id ||
                          `generated_${uuidv4()}`;
                return id.toString();
              });

              console.log('Extracted item IDs:', {
                count: itemIds.length,
                sampleIds: itemIds.slice(0, 5)
              });

              // After processing each page's items
              if (currentPageItems.length > 0) {
                // Save child execution log with the item IDs
                await executionLogs.saveChildExecution({
                  pageNumber: pageCount,
                  totalItemsProcessed,
                  itemsInCurrentPage: currentPageItems.length,
                  url: urlObj.toString(),
                  status: response.status,
                  paginationType: detectedPaginationType || 'none',
                  isLast: !hasMorePages || pageCount === maxIterations,
                  itemIds: itemIds // Pass the extracted item IDs directly
                });
              }

              // Check for next page based on detected pagination type
              if (detectedPaginationType === 'link') {
                const nextUrl = extractNextUrl(response.headers.link);
                if (!nextUrl) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Link header):', `Page ${pageCount} is the last page`);
                } else {
                  // For Shopify, we need to handle page_info parameter correctly
                  const nextUrlObj = new URL(nextUrl);
                  // Only remove status parameter, keep limit
                  nextUrlObj.searchParams.delete('status');
                  // Add limit parameter if it's not already present
                  if (!nextUrlObj.searchParams.has('limit') && queryParams.limit) {
                    nextUrlObj.searchParams.append('limit', queryParams.limit);
                  }
                  currentUrl = nextUrlObj.toString();
                  console.log('\nNext page URL:', currentUrl);
                }
              } else if (detectedPaginationType === 'bookmark') {
                const bookmark = extractBookmark(response.data);
                if (!bookmark) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Bookmark):', `Page ${pageCount} is the last page`);
                } else {
                  urlObj.searchParams.set('bookmark', bookmark);
                  currentUrl = urlObj.toString();
                  console.log('\nNext page bookmark:', bookmark);
                }
              } else if (detectedPaginationType === 'cursor') {
                const cursor = extractCursor(response.data);
                if (!cursor) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Cursor):', `Page ${pageCount} is the last page`);
                } else {
                  urlObj.searchParams.set('cursor', cursor);
                  currentUrl = urlObj.toString();
                  console.log('\nNext page cursor:', cursor);
                }
              } else if (detectedPaginationType === 'offset') {
                const totalCount = response.data.total_count || response.data.total;
                const currentOffset = parseInt(urlObj.searchParams.get('offset') || '0');
                const limit = parseInt(urlObj.searchParams.get('limit') || '10');
                
                if (currentOffset + limit >= totalCount) {
                  hasMorePages = false;
                  console.log('\nNo more pages (Offset):', `Page ${pageCount} is the last page`);
                } else {
                  urlObj.searchParams.set('offset', (currentOffset + limit).toString());
                  currentUrl = urlObj.toString();
                  console.log('\nNext page offset:', currentOffset + limit);
                }
              } else {
                hasMorePages = false;
                console.log('\nNo pagination detected:', `Page ${pageCount} is the last page`);
              }

              console.log(`\n=== PAGE ${pageCount} SUMMARY ===`);
              console.log({
                status: response.status,
                hasMorePages,
                totalItemsProcessed,
                currentPageItems: currentPageItems.length,
                nextUrl: currentUrl,
                paginationType: detectedPaginationType,
                responseData: response.data
              });

              pageCount++;
            }

            // Update parent execution status to completed
            await executionLogs.updateParentStatus('completed', true);

            // Log final summary
            console.log('\n=== PAGINATED REQUEST COMPLETED ===');
            console.log({
              totalPages: pageCount - 1,
              totalItems: totalItemsProcessed,
              executionId: execId,
              paginationType: detectedPaginationType || 'none',
              finalUrl: currentUrl,
              lastError: lastError
            });

          } catch (error) {
            console.error('Background processing error:', error);
            if (executionLogs) {
              await executionLogs.updateParentStatus('error', true);
            }
          }
        })();

        return initialResponse;

      } catch (error) {
        console.error('\n=== PAGINATED REQUEST FAILED ===');
        console.error({
          message: error.message,
          code: error.code,
          stack: error.stack,
          request: {
            url: currentUrl,
            method,
            headers
          }
        });

        return {
          statusCode: 500,
          body: { 
            error: 'Failed to execute paginated request',
            details: error.message,
            code: error.code,
            lastError: lastError
          }
        };
      }
    }
  }
});

// Initialize AWS DynamoDB OpenAPI backend
const awsApi = new OpenAPIBackend({
  definition: './swagger/aws-dynamodb.yaml',
  quick: true,
  handlers: {
    validationFail: async (c, req, res) => ({
      statusCode: 400,
      error: c.validation.errors
    }),
    notFound: async (c, req, res) => ({
      statusCode: 404,
      error: 'Not Found'
    }),
    // Table Operations 
    createTable: dynamodbHandlers.createTable,
   
    createItem: dynamodbHandlers.createItem,
   
  }
});

// Initialize Pinterest OpenAPI backend



// Initialize AWS Messaging OpenAPI backend


// Initialize all APIs
await Promise.all([
  mainApi.init(),
  awsApi.init(),
 
]);

// Helper function to handle requests


// Serve Swagger UI for all APIs
const mainOpenapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, 'openapi.yaml'), 'utf8'));
const awsOpenapiSpec = yaml.load(fs.readFileSync(path.join(__dirname, 'swagger/aws-dynamodb.yaml'), 'utf8'));

// Configure route handlers for API documentation
app.get('/api-docs/swagger.json', (req, res) => {
  res.json(mainOpenapiSpec);
});



// Serve main API docs
app.use('/api-docs', swaggerUi.serve);
app.get('/api-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(mainOpenapiSpec, {
      customSiteTitle: "Main API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/api-docs/swagger.json"
    })
  );
});

// Serve AWS API docs
app.use('/aws-api-docs', swaggerUi.serve);
app.get('/aws-api-docs', (req, res) => {
  res.send(
    swaggerUi.generateHTML(awsOpenapiSpec, {
      customSiteTitle: "AWS DynamoDB API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/aws-api-docs/swagger.json"
    })
  );
});

// Serve AWS API docs at the DynamoDB base URL
app.use('/api/dynamodb', swaggerUi.serve);
app.get('/api/dynamodb', (req, res) => {
  res.send(
    swaggerUi.generateHTML(awsOpenapiSpec, {
      customSiteTitle: "AWS DynamoDB API Documentation",
      customfavIcon: "/favicon.ico",
      customCss: '.swagger-ui .topbar { display: none }',
      swaggerUrl: "/api/dynamodb/swagger.json"
    })
  );
});

// Serve DynamoDB OpenAPI specification
app.get('/api/dynamodb/swagger.json', (req, res) => {
  res.json(awsOpenapiSpec);
});







app.post('/execute/paginated', async (req, res) => {
  try {
    const response = await mainApi.handleRequest(
      {
        method: 'POST',
        path: '/execute/paginated',
        body: req.body,
        headers: req.headers
      },
      req,
      res
    );
    res.status(response.statusCode).json(response.body);
  } catch (error) {
    console.error('Paginated execution error:', error);
    res.status(500).json({ error: 'Failed to execute paginated request' });
  }
});

// const PORT = process.env.PORT || 5000;
// app.listen(PORT, '0.0.0.0', () => {
//   console.log(`Server listening on port ${PORT}`);
 
// });


export const handler = serverless(app);
