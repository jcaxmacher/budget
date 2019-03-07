import os
from typing import List, Tuple
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date, datetime
import uuid

import boto3
from boto3.dynamodb.conditions import Key, Attr


TABLE_NAME = os.environ['BUDGET_TABLE']
LSI_1 = os.environ['LSI_1']
LSI_2 = os.environ['LSI_2']
table = boto3.resource('dynamodb').Table(TABLE_NAME)


@dataclass
class Ledger(object):
    account: str
    category: str
    transaction_type: str
    month: str
    amount: Decimal
    description: str
    date: date
    existing: bool = field(default=False)
    _outbound_attrs: Tuple[str] = (
        'category', 'transaction_type', 'date', 'amount', 'description', 'uuid'
    )
    _serialize_attrs: Tuple[str] = (
        'pk', 'sk', 'tk', 'qk', 'amount', 'description', 'date'
    )
    uuid: str = field(default_factory=lambda: str(uuid.uuid4())[:6])

    @property
    def pk(self):
        return self.account

    @property
    def sk(self):
        return f'{self.category}||{self.month}||{self.transaction_type}||{self.uuid}'

    @property
    def tk(self):
        return f'{self.category}||{self.transaction_type}||{self.month}'

    @property
    def qk(self):
        return f'{self.transaction_type}||{self.month}'

    def save(self):
        data = {}
        for key in self._serialize_attrs:
            value = getattr(self, key)
            if isinstance(value, (date, datetime)):
                data[key] = value.isoformat()
            else:
                data[key] = value
        if self.existing:
            return table.update_item(Item=data)
        else:
            self.existing = True
            return table.put_item(Item=data)
        
    def outbound(self):
        data = {}
        for key in self._outbound_attrs:
            value = getattr(self, key)
            if isinstance(value, (date, datetime)):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = str(value)
            data[key] = value
        return data

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
        data['transaction_type'] = transaction_type
        data['month'] = month
        data['uuid'] = uuid
        data['date'] = date.fromisoformat(data['date'])
        data['existing'] = True
        return cls(**data)

    @classmethod
    def get_by_category(cls, account, category):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('sk').begins_with(category)
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

    @classmethod
    def get_by_category_month(cls, account, category, month):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('sk').begins_with(f'{category}||{month}')
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

    @classmethod
    def get_by_category_transaction_type(cls, account, category, transaction_type):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('tk').begins_with(f'{category}||{transaction_type}||'),
            IndexName=LSI_1
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

    @classmethod
    def get_by_category_month_transaction_type(cls, account, category, month, transaction_type):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('sk').eq(f'{category}||{month}||{transaction_type}||')
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response
    
    @classmethod
    def get_by_transaction_type(cls, account, transaction_type):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(account) & Key('qk').begins_with(f'{transaction_type}||'),
            IndexName=LSI_2
        )
        items = [cls.deserialize(item) for item in response['Items']]
        response['Items'] = items
        return response

class Budget(object):
    pass