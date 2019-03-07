import dynamodb = require('@aws-cdk/aws-dynamodb');
import cdk = require('@aws-cdk/cdk');
import { AttributeType, BillingMode, ProjectionType } from '@aws-cdk/aws-dynamodb';

export class BudgetStack extends cdk.Stack {
  constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const db = new dynamodb.Table(this, 'BudgetTable', {
      partitionKey: { name: 'pk', type: AttributeType.String },
      sortKey: { name: 'sk', type: AttributeType.String },
      billingMode: BillingMode.Provisioned,
      readCapacity: 5,
      writeCapacity: 5
    });

    db.addLocalSecondaryIndex({
      indexName: 'BudgetTable-LSI1',
      projectionType: ProjectionType.All,
      sortKey: { name: 'tk', type: AttributeType.String }
    });
    db.addLocalSecondaryIndex({
      indexName: 'BudgetTable-LSI2',
      projectionType: ProjectionType.All,
      sortKey: { name: 'qk', type: AttributeType.String }
    });

    console.error(db.tableName);
  }
}
