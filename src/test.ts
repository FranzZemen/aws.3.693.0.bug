/*
Created by Franz Zemen 06/16/2024
License Type: UNLICENSED
*/


import {DynamoDBDocument} from '@aws-sdk/lib-dynamodb';
import {DynamoDBClient} from '@aws-sdk/client-dynamodb';


/*
export type ProgressCallback = (status: string, percent?: number) => void;
export const databaseManagementTimeout = 15 * 60 * 1000;

export function isResourceNotFoundException(err: any | ResourceNotFoundException): err is ResourceNotFoundException {
  if ('name' in err) {
    if (err.name === 'ResourceNotFoundException') {
      return true;
    }
  }
  return false;
}

export function isResourceInUseException(err: any | ResourceNotFoundException): err is ResourceNotFoundException {
  if ('name' in err) {
    if (err.name === 'ResourceInUseException') {
      return true;
    }
  }
  return false;
}

export function getCreateTableCommandInputShell(tableName: string, partitionKeyName: string, sortKeyName: string): CreateTableCommandInput {
  const AttributeDefinitions: AttributeDefinition[] = [];
  AttributeDefinitions.push({AttributeName: partitionKeyName, AttributeType: 'S'});
  if (sortKeyName) {
    AttributeDefinitions.push({AttributeName: sortKeyName, AttributeType: 'S'});
  }
  const KeySchema: KeySchemaElement[] = [];
  KeySchema.push({AttributeName: partitionKeyName, KeyType: 'HASH'});
  if (sortKeyName) {
    KeySchema.push({AttributeName: sortKeyName, KeyType: 'RANGE'});
  }
  return {
    TableName: tableName,
    KeySchema,
    AttributeDefinitions,
    BillingMode: 'PAY_PER_REQUEST',
    TableClass: 'STANDARD'
  }
}
*/

/**
 * Low level Dynamo Client with some basic operations and dynamo specific logic, including backoff algorithms, basic exception logging and dynamoDB encpasulation
 */
export class DynamoClient {
  private documentClient: DynamoDBDocument;
  private dbClient: DynamoDBClient;

  constructor() {

    this.dbClient = new DynamoDBClient();
    this.documentClient = DynamoDBDocument.from(this.dbClient, {
      marshallOptions: {
        removeUndefinedValues: true
      }
    });
  }
}

/*
async _scan<RECORD_TYPE>(table: string, FilterExpression?: string,
                         filterExpressionAttributeNames?: Record<string, string>,
                         filterExpressionAttributeValues?: Record<string, string | number>, ProjectionExpression?: string): Promise<RECORD_TYPE[]> {
  const args: any = {TableName: table};
  if (FilterExpression) {
    if (!filterExpressionAttributeValues || !filterExpressionAttributeNames) {
      throw new Error('FilterExpression, filterExpressionAttributeNames, and filterExpressionAttributeValues must all be set');
    }
    args['FilterExpression'] = FilterExpression;
    args['ExpressionAttributeNames'] = filterExpressionAttributeNames;
    args['ExpressionAttributeValues'] = filterExpressionAttributeValues;
  }

  const allRecords: RECORD_TYPE[] = [];
  let {records, LastEvaluatedKey} = await this.__scan<RECORD_TYPE>(args);
  allRecords.push(...records);
  while (LastEvaluatedKey !== undefined) {
    args.ExclusiveStartKey = LastEvaluatedKey;
    const nextResult = await this.__scan<RECORD_TYPE>(args);
    if (nextResult.records.length > 0) {
      allRecords.push(...nextResult.records);
    } else {
      break;
    }
    LastEvaluatedKey = nextResult.LastEvaluatedKey;
  }
  return allRecords;
}


async _put<RECORD_TYPE>(table: string, record: RECORD_TYPE): Promise<RECORD_TYPE> {
  try {
    await this.documentClient.put({TableName: table, Item: record as Record<string, any>})
    return record;
  } catch (err) {
    throw err;
  }
}

async _batchPut(recordsHashedByTableName: MappedKeyedRecords<string, any>): Promise<boolean> {
  try {
    const flattened = this.flatten(recordsHashedByTableName);
    //const result = await new Promise<boolean>(async (resolve, reject) => {
    //const interval = setInterval(async () => {
    while (flattened.length > 0) {
      const spliceCount = Math.min(25, flattened.length);
      const spliced = flattened.splice(0, spliceCount);
      const args: BatchWriteCommandInput = {
        RequestItems: {}
      };
      spliced.forEach((record: any) => {
        const tableName = Object.keys(record)[0] as string;
        if (args.RequestItems) {
          let tableRecords = args.RequestItems[tableName];
          if (!tableRecords) {
            tableRecords = [];
            args.RequestItems[tableName] = tableRecords;
          }
          tableRecords.push({PutRequest: {Item: record[tableName]}});
        } else {
          throw new Error('Unreachable code.  RequestItems is undefined.');
        }
      });
      await this.__batchWrite(args);
    }
    return true;
  } catch (err) {
    this.#log.setMethod('_batchPut');
    throw logAndEnhanceError(this.#log, err);
  }
}

async _batchDelete<KEYS extends string>(deletionRequests: MappedKeys<KEYS>): Promise<boolean> {
  try {
    for (const table in deletionRequests) {
      const records = deletionRequests[table];
      if (records) {
        while (records.length > 0) {
          const spliceCount = Math.min(25, records.length);
          const recordsToDelete = records.splice(0, spliceCount);
          const batchWriteCommandInput: BatchWriteCommandInput = {RequestItems: {[table]: recordsToDelete.map(record => ({DeleteRequest: {Key: record}}))}}
          await this.__batchWrite(batchWriteCommandInput);
          if (records.length === 0) {
            delete deletionRequests[table];
          }
        }
      } else {
        delete deletionRequests[table];
      }
    }
    return true;
  } catch (err) {
    this.#log.setMethod('_batchDelete');
    throw logAndEnhanceError(this.#log, err);
  }
}


async _delete<KEYS extends string>(TableName: string, Key: RecordKey<KEYS>): Promise<boolean> {
  try {
    const args: DeleteCommandInput = {TableName, Key};
    await this.documentClient.delete(args);
    return true;
  } catch (err) {
    this.#log.setMethod('_delete');
    throw logAndEnhanceError(this.#log, err);
  }
}

async _query<RECORD_TYPE>(TableName: string,
                          indexQuery: boolean | string,
                          keyStructuredExpression: KeyStructuredExpression,
                          FilterExpression?: string,
                          filterExpressionAttributeNames?: Record<string, string>,
                          filterExpressionAttributeValues?: Record<string, string | number>, OriginalProjectionExpression?: string): Promise<RECORD_TYPE[]> {

  let args: QueryCommandInput = {TableName};
  if (indexQuery === true) {
    const IndexName = `${keyStructuredExpression.partitionFieldName}${keyStructuredExpression.sortFieldName ? `-${keyStructuredExpression.sortFieldName}` : ''}-index`;
    args['IndexName'] = IndexName;
  } else if (typeof indexQuery === 'string') {
    args['IndexName'] = indexQuery;
  }
  let {KeyConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues, ProjectionExpression} = buildKeyConditionSet(keyStructuredExpression, OriginalProjectionExpression);
  args['KeyConditionExpression'] = KeyConditionExpression;
  if (FilterExpression) {
    if (!filterExpressionAttributeValues || !filterExpressionAttributeNames) {
      throw new Error('FilterExpression, filterExpressionAttributeNames, and filterExpressionAttributeValues must all be set');
    }
    ExpressionAttributeNames = {...ExpressionAttributeNames, ...filterExpressionAttributeNames};
    ExpressionAttributeValues = {...ExpressionAttributeValues, ...filterExpressionAttributeValues};
    args['FilterExpression'] = FilterExpression;
  }
  args['ExpressionAttributeNames'] = ExpressionAttributeNames;
  args['ExpressionAttributeValues'] = ExpressionAttributeValues;
  if (ProjectionExpression) {
    args['ProjectionExpression'] = ProjectionExpression;
  }
  const allRecords: RECORD_TYPE[] = [];
  let {records, LastEvaluatedKey} = await this.___query<RECORD_TYPE>(args);
  allRecords.push(...records);
  while (LastEvaluatedKey !== undefined) {
    args.ExclusiveStartKey = LastEvaluatedKey;
    const nextResult = await this.___query<RECORD_TYPE>(args);
    if (nextResult.records.length > 0) {
      allRecords.push(...nextResult.records);
    } else {
      break;
    }
    LastEvaluatedKey = nextResult.LastEvaluatedKey;
  }
  return allRecords;
}

async ___query<RECORD_TYPE>(args: QueryCommandInput): Promise<{ records: RECORD_TYPE[], LastEvaluatedKey: any }> {
  try {
    const data = await this.documentClient.query(args);
    return {records: data.Items as RECORD_TYPE[], LastEvaluatedKey: data.LastEvaluatedKey};
  } catch (err) {
    this.#log.setMethod('__query');
    throw logAndEnhanceError(this.#log, err);
  }
}

async _batchGet<KEYS extends string, RECORD_TYPE>(args: BatchGetCommandInput): Promise<MappedKeyedRecords<KEYS, RECORD_TYPE>> {
  try {
    let results: MappedKeyedRecords<KEYS, RECORD_TYPE>;
    const data = await this.documentClient.batchGet(args);

    results = data.Responses as MappedKeyedRecords<KEYS, RECORD_TYPE>;
    let unprocessedItems = data.UnprocessedKeys;
    let timeout = 1;
    let emptyUnprocessedItems = true;
    if (unprocessedItems) {
      const remainingTables = Object.getOwnPropertyNames(unprocessedItems);
      if (remainingTables.length === 0) {
        emptyUnprocessedItems = true;
      } else {
        emptyUnprocessedItems = false;
      }
    }
    while (!emptyUnprocessedItems && timeout <= 1280) {
      let nextResult = await this.documentClient.batchGet({RequestItems: unprocessedItems});
      merge(results, nextResult.Responses);
      unprocessedItems = nextResult.UnprocessedKeys;
      timeout = timeout * 2;
    }
    if (timeout > 1280) {
      const err = new Error('Exponential backoff algorithm exceeded');
      this.#log.setMethod('_getMany').error(err);
      throw err;
    }
    return results;
  } catch (err) {
    this.#log.setMethod('_batchGet');
    throw logAndEnhanceError(this.#log, err);
  }
}

async _get<RECORD_TYPE, KEYS extends string = string>(Key: RecordKey<KEYS>, TableName: string): Promise<RECORD_TYPE | undefined> {
  try {
    const data = await this.documentClient.get({TableName, Key})
    this.#log.setMethod('findByKey').debug(data, 'findByKey result');
    if (data.Item) {
      return data.Item as RECORD_TYPE;
    } else {
      return undefined;
    }
  } catch (err) {
    this.#log.setMethod('_get');
    throw logAndEnhanceError(this.#log, err);
  }
}

async updateTable(update: UpdateTableCommandInput, progressCallBack?: ProgressCallback): Promise<TableDescription> {
  let indexName: string = '';
  let creating = true;
  const TableName = update.TableName;
  if (!TableName) {
    throw new Error('Table name is required');
  }
  const createIndex = update.GlobalSecondaryIndexUpdates?.find(indexUpdate => indexUpdate.Create?.IndexName !== undefined);
  if (createIndex?.Create?.IndexName) {
    indexName = createIndex.Create.IndexName;
  } else {
    const deleteIndex = update.GlobalSecondaryIndexUpdates?.find(indexUpdate => indexUpdate.Delete?.IndexName !== undefined);
    if (deleteIndex?.Delete?.IndexName) {
      creating = false;
      indexName = deleteIndex.Delete.IndexName;
    }
  }
  let priorDescription = await this.describeTable(TableName);
  if (priorDescription.GlobalSecondaryIndexes) {
    if (priorDescription.GlobalSecondaryIndexes.find(index => index.IndexName === indexName)) {
      if (progressCallBack) {
        progressCallBack(`Index ${indexName} already exists on table ${TableName}`, 100);
      }
      return priorDescription;
    }
  }
  if (progressCallBack) {
    progressCallBack(`Updating table ${update.TableName}, ${creating ? 'creating' : 'deleting'} index ${indexName}`, 0);
  }
  const result = await this.dbClient.send(new UpdateTableCommand(update));
  let tableDescription = result.TableDescription;
  let status = (tableDescription?.GlobalSecondaryIndexes?.find(index => index.IndexName === indexName))?.IndexStatus || 'UPDATING';
  let totalTimeout = 0;
  while (status !== 'ACTIVE') {
    if (progressCallBack) {
      progressCallBack(status, totalTimeout / databaseManagementTimeout);
    }
    await new Promise<void>((resolve, reject) => setTimeout(() => resolve(), 5000));
    const describeCommand = new DescribeTableCommand({TableName: update.TableName});
    const describeResult = await this.dbClient.send(describeCommand);
    tableDescription = describeResult.Table;
    status = (tableDescription?.GlobalSecondaryIndexes?.find(index => index.IndexName === indexName))?.IndexStatus || 'UPDATING';
    totalTimeout += 5000;
    if (totalTimeout > databaseManagementTimeout) {
      throw new Error(`Table update timeout exceeded for table ${update.TableName}`);
    }
  }
  if (!tableDescription) {
    throw new Error(`Table update failed for table ${update.TableName}`);
  } else {
    if (progressCallBack) {
      progressCallBack(`Table ${update.TableName} updated, ${creating ? 'created' : 'deleted'} index ${indexName}`, 100);
    }
    return tableDescription;
  }
}

async createTable(input: CreateTableCommandInput, progressCallBack?: ProgressCallback): Promise<TableDescription> {
  if (!input.TableName) {
    throw new Error('Table name is required');
  }
  if (progressCallBack) {
    progressCallBack(`Creating table ${input.TableName}`, 0);
  }
  const command = new CreateTableCommand(input);
  const createResult = await this.dbClient.send(command);
  let tableDescription: TableDescription | undefined = createResult.TableDescription;
  let status = tableDescription?.TableStatus || 'CREATING';
  let totalTimeout = 0;
  while (status === 'CREATING') {
    if (progressCallBack) {
      progressCallBack(status, totalTimeout / databaseManagementTimeout);
    }
    await new Promise<void>((resolve, reject) => setTimeout(() => resolve(), 5000));
    const describeCommand = new DescribeTableCommand({TableName: input.TableName});
    const describeResult = await this.dbClient.send(describeCommand);
    tableDescription = describeResult.Table;
    status = tableDescription?.TableStatus ?? 'CREATING';
    totalTimeout += 5000;
    if (totalTimeout > databaseManagementTimeout) {
      throw new Error(`Table creation timeout exceeded for table ${input.TableName}`);
    }
  }
  if (!tableDescription) {
    throw new Error(`Table creation failed for table ${input.TableName}`);
  } else {
    if (progressCallBack) {
      progressCallBack(`Table ${input.TableName} created`, 100);
    }
    return tableDescription;
  }
}

async deleteTable(TableName: string, progressCallBack?: ProgressCallback): Promise<void> {
  const result = await this.dbClient.send(new DeleteTableCommand({TableName}));
  let tableDescription: TableDescription | undefined = result.TableDescription;
  let status = tableDescription?.TableStatus || 'DELETING';
  let totalTimeout = 0;
  try {
    while (status === 'DELETING') {
      if (progressCallBack) {
        progressCallBack(status, totalTimeout / 300000 * 100);
      }
      await new Promise<void>((resolve, reject) => setTimeout(() => resolve(), 5000));
      const describeCommand = new DescribeTableCommand({TableName});
      const describeResult = await this.dbClient.send(describeCommand);
      tableDescription = describeResult.Table;
      status = tableDescription?.TableStatus ?? 'DELETING';
      totalTimeout += 5000;
      if (totalTimeout > databaseManagementTimeout) {
        throw new Error(`Table deletion timeout exceeded for table ${TableName}`);
      }
    }
  } catch (err) {
    if (isResourceNotFoundException(err)) {
      return;
    } else {
      this.#log.error(err);
      throw err;
    }
  }
  if (!tableDescription) {
    throw new Error(`Table deletion failed for table ${TableName}`);
  } else {
    return;
  }
}

async describeTable(TableName: string): Promise<TableDescription> {
  const result: DescribeTableCommandOutput = await this.dbClient.send(new DescribeTableCommand({TableName}));
  return result.Table as TableDescription;
}

private async __scan<RECORD_TYPE>(args: ScanCommandInput): Promise<{ records: RECORD_TYPE[], LastEvaluatedKey: any }> {
  return this.documentClient.scan(args)
    .then(data => {
      return {records: data.Items as RECORD_TYPE[], LastEvaluatedKey: data.LastEvaluatedKey};
    })
    .catch(err => {
      this.#log.error(new Error(`Error scanning table ${args.TableName}`, err));
      throw err;
    });
}

private async __batchWrite(args: BatchWriteCommandInput): Promise<boolean> { // private async __batchWrite(args: BatchWriteCommandInput, table: string): Promise<boolean> {
  try {
    const writeOutput = await this.documentClient.batchWrite(args);
    let unprocessedItems = writeOutput.UnprocessedItems;
    let timeout = 1;
    let emptyUnprocessedItems = unprocessedItems === undefined;
    if (unprocessedItems) {
      const remainingTables = Object.getOwnPropertyNames(unprocessedItems);
      if (remainingTables.length === 0) {
        emptyUnprocessedItems = true;
      }
    }
    while (!emptyUnprocessedItems && timeout <= 1280) { // while (unprocessedItems && unprocessedItems[table] && timeout <= 1280) {
      let unprocessedData = await this.documentClient.batchWrite({RequestItems: unprocessedItems});
      unprocessedItems = unprocessedData.UnprocessedItems;
      timeout = timeout * 2;
    }
    if (timeout > 1280) {
      const err = new Error('Exponential backoff algorithm exceeded');
      this.#log.setMethod('_insertMany').error(err);
      throw err;
    }
    return true;
  } catch (err) {
    if (isResourceNotFoundException(err)) {
      this.#log.warn(`Amazon ResourceNotFoundException - might table name be incorrect?`);
    }
    this.#log.setMethod('__batchWrite');
    throw logAndEnhanceError(this.#log, err);
  }
}

private flatten(hash: MappedKeyedRecords<string, any>): { [key: string]: any }[] {
  const records: { [key: string]: any }[] = [];
  for (const table in hash) {
    const tableRecords = hash[table];
    if (tableRecords) {
      tableRecords.forEach(record => {
        records.push({[table]: record});
      });
    }
  }
  return records;
}


}



 */
