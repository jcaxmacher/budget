import models

from boto3.dynamodb.conditions import Key
import pytest
from decimal import Decimal
from datetime import datetime
from unittest.mock import MagicMock


@pytest.fixture(scope='module')
def ledger_item_date():
    return datetime.today()


@pytest.fixture
def ledger_stored_item(ledger_item_date):
    return {
        'pk': '193824345',
        'sk': 'Fun Money||2019-03||SPEND||a73b2f',
        'tk': 'Fun Money||SPEND||2019-03',
        'qk': 'SPEND||2019-03',
        'amount': Decimal('23.94'),
        'description': 'Some fun',
        'date': ledger_item_date.isoformat()
    }


@pytest.fixture
def ledger_item(ledger_item_date):
    return models.Ledger(
        account = '193824345',
        category = 'Fun Money',
        transaction_type = models.TransactionType.SPEND,
        month = '2019-03',
        amount = Decimal('23.94'),
        description = 'Some fun',
        date = ledger_item_date
    )


@pytest.fixture
def ledger_wire_item(ledger_item):
    return {
        'category': ledger_item.category,
        'transaction_type': ledger_item.transaction_type_name,
        'amount': str(ledger_item.amount),
        'description': ledger_item.description,
        'sk': ledger_item.sk,
        'date': ledger_item.date.isoformat()
    }


def test_pk(ledger_item):
    assert ledger_item.pk == ledger_item.account


def test_sk(ledger_item):
    assert ledger_item.sk == (f'{ledger_item.category}||'
                              f'{ledger_item.month}||'
                              f'{ledger_item.transaction_type_name}||'
                              f'{ledger_item.uuid}')

def test_tk(ledger_item):
    assert ledger_item.tk == (f'{ledger_item.category}||'
                              f'{ledger_item.transaction_type_name}||'
                              f'{ledger_item.month}')


def test_qk(ledger_item):
    assert ledger_item.qk == (f'{ledger_item.transaction_type_name}||'
                              f'{ledger_item.month}')


def test_save_new(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    ledger_item.save()
    models.table.put_item.assert_called_with(Item={
        'pk': ledger_item.pk,
        'sk': ledger_item.sk,
        'tk': ledger_item.tk,
        'qk': ledger_item.qk,
        'amount': ledger_item.amount,
        'description': ledger_item.description,
        'date': ledger_item.date.isoformat()
    }, ReturnConsumedCapacity='TOTAL')
    assert ledger_item.existing


def test_save_existing(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    ledger_item.existing = True
    ledger_item.save()
    models.table.update_item.assert_called()
    models.table = old_table


def test_save_existing_changed_key(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    ledger_item.existing = True
    ledger_item.old_sk = 'blah'
    ledger_item.save()
    models.table.delete_item.assert_called()
    models.table.put_item.assert_called()
    models.table = old_table


def test_to_wire(ledger_item):
    assert ledger_item.to_wire() == {
        'category': ledger_item.category,
        'transaction_type': ledger_item.transaction_type_name,
        'amount': str(ledger_item.amount),
        'description': ledger_item.description,
        'date': ledger_item.date.isoformat(),
        'sk': ledger_item.sk
    }


def test_from_wire_new(ledger_item, ledger_wire_item):
    del ledger_wire_item['sk']
    # Add account attribute which will never come from the wire payload
    wire_item = models.Ledger.from_wire(ledger_item.account, ledger_wire_item)
    # Overwrite attribute we know will be different
    ledger_item.uuid = wire_item.uuid
    assert ledger_item == wire_item


def test_from_wire_update(ledger_item, ledger_wire_item):
    # Add account attribute which will never come from the wire payload
    wire_item = models.Ledger.from_wire(ledger_item.account, ledger_wire_item)
    # Overwrite attribute we know will be different
    ledger_item.uuid = wire_item.uuid
    ledger_item.existing = True
    ledger_item.old_sk = wire_item.old_sk
    assert ledger_item == wire_item


def test_deserialization(ledger_item, ledger_stored_item):
    deserialized_item = models.Ledger.deserialize(ledger_stored_item)
    # Override two properties we know will not match from fixtures
    ledger_item.uuid = deserialized_item.uuid
    ledger_item.existing = True
    assert ledger_item == deserialized_item


def test_get_by_category(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    models.Ledger.get_by_category(ledger_item.account, ledger_item.category)
    models.table.query.assert_called_with(
        KeyConditionExpression=Key('pk').eq(ledger_item.pk) & Key('sk').begins_with(f'{ledger_item.category}||'),
        ReturnConsumedCapacity='TOTAL'
    )
    models.table = old_table


def test_get_by_category_month(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    models.Ledger.get_by_category_month(ledger_item.account, ledger_item.category, ledger_item.month)
    models.table.query.assert_called_with(
        KeyConditionExpression=Key('pk').eq(ledger_item.pk) & Key('sk').begins_with(f'{ledger_item.category}||{ledger_item.month}'),
        ReturnConsumedCapacity='TOTAL'
    )
    models.table = old_table


def test_get_by_category_transaction_type(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    models.Ledger.get_by_category_transaction_type(ledger_item.account, ledger_item.category, ledger_item.transaction_type)
    models.table.query.assert_called_with(
        KeyConditionExpression=Key('pk').eq(ledger_item.pk) & Key('tk').begins_with(f'{ledger_item.category}||{ledger_item.transaction_type_name}||'),
        IndexName=models.LSI_1,
        ReturnConsumedCapacity='TOTAL'
    )
    models.table = old_table


def test_get_by_category_month_transaction_type(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    models.Ledger.get_by_category_month_transaction_type(ledger_item.account, ledger_item.category, ledger_item.month, ledger_item.transaction_type)
    models.table.query.assert_called_with(
        KeyConditionExpression=Key('pk').eq(ledger_item.pk) & Key('sk').begins_with(f'{ledger_item.category}||{ledger_item.month}||{ledger_item.transaction_type_name}||'),
        ReturnConsumedCapacity='TOTAL'
    )
    models.table = old_table


def test_get_by_transaction_type(ledger_item):
    old_table = models.table
    models.table = MagicMock()
    models.Ledger.get_by_transaction_type(ledger_item.account, ledger_item.transaction_type)
    models.table.query.assert_called_with(
        KeyConditionExpression=Key('pk').eq(ledger_item.pk) & Key('qk').begins_with(f'{ledger_item.transaction_type_name}||'),
        IndexName=models.LSI_2,
        ReturnConsumedCapacity='TOTAL'
    )
    models.table = old_table