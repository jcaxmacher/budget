import os
from typing import List, Tuple
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date, datetime
from enum import Enum
import uuid

import pytz
import boto3
from boto3.dynamodb.conditions import Key, Attr


TABLE_NAME = os.environ['BUDGET_TABLE']
LSI_1 = os.environ['LSI_1']
LSI_2 = os.environ['LSI_2']
table = boto3.resource('dynamodb').Table(TABLE_NAME)


class TransactionType(Enum):
    SPEND = 1
    FUND = 2
    INFLOW = 3


@dataclass
class Ledger(object):
    account: str
    category: str
    transaction_type: TransactionType
    month: str
    amount: Decimal
    description: str
    date: datetime
    existing: bool = field(default=False)
    old_sk: str = field(default='')
    _wire_attrs: Tuple[str] = (
        'category', 'transaction_type', 'date', 'amount', 'description', 'sk'
    )
    _serialize_attrs: Tuple[str] = (
        'pk', 'sk', 'tk', 'qk', 'amount', 'description', 'date'
    )
    uuid: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    @property
    def transaction_type_name(self):
        return self.transaction_type.name

    @property
    def pk(self):
        return self.account

    @property
    def sk(self):
        return f'{self.category}||{self.month}||{self.transaction_type_name}||{self.uuid}'

    @property
    def tk(self):
        return f'{self.category}||{self.transaction_type_name}||{self.month}'

    @property
    def qk(self):
        return f'{self.transaction_type_name}||{self.month}'

    def save(self):
        data = {}
        for key in self._serialize_attrs:
            value = getattr(self, key)
            if isinstance(value, (date, datetime)):
                data[key] = value.isoformat()
            elif isinstance(value, TransactionType):
                data[key] = value.name
            else:
                data[key] = value
        # Composite key has changed, so we have to delete the old item
        if self.old_sk and self.old_sk != self.sk:
            response = table.delete_item(Key={
                'pk': self.pk,
                'sk': self.old_sk
            }, ReturnConsumedCapacity='TOTAL')
            print(response)
            self.existing = False
            self.old_sk = ''
        # If we have an existing item (which must not have changed the
        # composite key), update the fields
        if self.existing:
            data.pop('pk')
            data.pop('sk')
            update_expression = 'SET {}'.format(','.join(f'#{k}=:{k}' for k in data))
            expression_attribute_values = {f':{k}': v for k, v in data.items()}
            expression_attribute_names = {f'#{k}': k for k in data}
            response = table.update_item(
                Key={
                    'pk': self.pk,
                    'sk': self.sk
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
                ReturnValues='UPDATED_NEW',
                ReturnConsumedCapacity='TOTAL'
            )
            return response
        else:
            response = table.put_item(Item=data, ReturnConsumedCapacity='TOTAL')
            self.existing = True
            return response
        
    def to_wire(self):
        data = {}
        for key in self._wire_attrs:
            value = getattr(self, key)
            if isinstance(value, (date, datetime)):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = str(value)
            elif isinstance(value, TransactionType):
                value = value.name
            data[key] = value
        return data

    @classmethod
    def from_wire(cls, account, data):
        data['account'] = account
        data['date'] = datetime.fromisoformat(data['date'])
        data['amount'] = Decimal(data['amount'])
        data['transaction_type'] = TransactionType[data['transaction_type']]
        data['month'] = data['date'].strftime('%Y-%m')
        if 'sk' in data:
            data['existing'] = True
            data['old_sk'] = data['sk']
            del data['sk']
        return cls(**data)


    @classmethod
    def deserialize(cls, data):
        # Store key data and remove all keys
        secondary_key = data['sk']
        account = data['pk']
        del data['pk'], data['sk'], data['tk'], data['qk']
        # Slice secondary key into consituent parts
        category, month, transaction_type, uuid = secondary_key.split('||')
        data['account'] = account
        data['category'] = category
        data['transaction_type'] = TransactionType[transaction_type]
        data['month'] = month
        data['uuid'] = uuid
        data['date'] = datetime.fromisoformat(data['date'])
        data['existing'] = True
        return cls(**data)

    @classmethod
    def get_by_category(cls, account, category):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('sk').begins_with(f'{category}||'),
            ReturnConsumedCapacity='TOTAL'
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

    @classmethod
    def get_by_category_month(cls, account, category, month):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('sk').begins_with(f'{category}||{month}'),
            ReturnConsumedCapacity='TOTAL'
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

    @classmethod
    def get_by_category_transaction_type(cls, account, category, transaction_type):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('tk').begins_with(f'{category}||{transaction_type.name}||'),
            IndexName=LSI_1,
            ReturnConsumedCapacity='TOTAL'
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

    @classmethod
    def get_by_category_month_transaction_type(cls, account, category, month, transaction_type):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('sk').begins_with(f'{category}||{month}||{transaction_type.name}||'),
            ReturnConsumedCapacity='TOTAL'
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response
    
    @classmethod
    def get_by_transaction_type(cls, account, transaction_type):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('qk').begins_with(f'{transaction_type.name}||'),
            IndexName=LSI_2,
            ReturnConsumedCapacity='TOTAL'
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

class Budget(object):
    pass