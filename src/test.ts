import {DynamoDBDocument} from '@aws-sdk/lib-dynamodb';
import {DynamoDBClient} from '@aws-sdk/client-dynamodb';


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
