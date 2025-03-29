import { v4 as uuidv4 } from 'uuid';
import { handlers as dynamodbHandlers } from './lib/dynamodb-handlers.js';

// Execution status constants
const EXECUTION_STATUS = {
  STARTED: 'started',
  IN_PROGRESS: 'inProgress',
  COMPLETED: 'completed',
  ERROR: 'error'
};

// Handler to save execution logs
export const saveExecutionLog = async ({
  execId,
  childExecId,
  data,
  isParent = false
}) => {
  try {
    const timestamp = new Date().toISOString();
    
    // Format item IDs as DynamoDB list type
    const formattedItemIds = (data.itemIds || []).map(id => ({
      S: id.toString() // Convert each ID to string type for DynamoDB
    }));

    const logItem = {
      'exec-id': execId,
      'child-exec-id': childExecId,
      data: {
        'execution-id': execId,
        'iteration-no': data.iterationNo || 0,
        'total-items-processed': data.totalItemsProcessed || 0,
        'items-in-current-page': data.itemsInCurrentPage || 0,
        'request-url': data.requestUrl,
        'response-status': data.responseStatus,
        'pagination-type': data.paginationType || 'none',
        'timestamp': timestamp,
        'is-last': data.isLast || false,
        'item-ids': {
          L: formattedItemIds // Use DynamoDB list type for item IDs
        }
      }
    };

    // Only add status field for parent execution logs
    if (isParent) {
      logItem.data.status = data.status || EXECUTION_STATUS.STARTED;
    }

    console.log('Saving execution log with item IDs:', {
      execId,
      childExecId,
      itemIdsCount: formattedItemIds.length,
      itemIds: formattedItemIds
    });

    const response = await dynamodbHandlers.createItem({
      request: {
        params: {
          tableName: 'executions'
        },
        requestBody: logItem
      }
    });

    if (!response.ok) {
      console.error('Failed to save execution log:', response);
      return null;
    }

    return logItem;
  } catch (error) {
    console.error('Error saving execution log:', error);
    return null;
  }
};

 // Handler to update parent execution status
export const updateParentExecutionStatus = async ({
  execId,
  status,
  isLast = false
}) => {
  try {
    const updateExpression = {
      UpdateExpression: "SET #data.#status = :status, #data.#isLast = :isLast",
      ExpressionAttributeNames: {
        "#data": "data",
        "#status": "status",
        "#isLast": "is-last"
      },
      ExpressionAttributeValues: {
        ":status": status,
        ":isLast": isLast
      }
    };

    const response = await dynamodbHandlers.updateItemsByPk({
      request: {
        params: {
          tableName: 'executions',
          id: execId
        },
        query: {
          sortKey: execId // Pass sortKey as a query parameter
        },
        requestBody: updateExpression
      }
    });

    if (!response.ok) {
      console.error('Failed to update parent execution status:', response);
      return null;
    }

    return response.body;
  } catch (error) {
    console.error('Error updating parent execution status:', error);
    return null;
  }
};

// Handler to save paginated execution logs
export const savePaginatedExecutionLogs = async ({
  execId,
  method,
  url,
  queryParams,
  headers,
  maxIterations,
  tableName,
  saveData
}) => {
  try {
    // Save parent execution log
    const parentExecId = execId;
    await saveExecutionLog({
      execId: parentExecId,
      childExecId: parentExecId, // Same as execId for parent
      data: {
        requestUrl: url,
        responseStatus: 0, // Will be updated later
        paginationType: 'paginated',
        status: EXECUTION_STATUS.STARTED,
        isLast: false
      },
      isParent: true
    });

    return {
      parentExecId,
      updateParentStatus: async (status, isLast) => {
        return await updateParentExecutionStatus({
          execId: parentExecId,
          status,
          isLast
        });
      },
      saveChildExecution: async (pageData) => {
        const childExecId = uuidv4();
        return await saveExecutionLog({
          execId: parentExecId,
          childExecId,
          data: {
            iterationNo: pageData.pageNumber,
            totalItemsProcessed: pageData.totalItemsProcessed,
            itemsInCurrentPage: pageData.itemsInCurrentPage,
            requestUrl: pageData.url,
            responseStatus: pageData.status,
            paginationType: pageData.paginationType,
            isLast: pageData.isLast,
            itemIds: pageData.itemIds || []
          }
        });
      }
    };
  } catch (error) {
    console.error('Error saving paginated execution logs:', error);
    return null;
  }
};