#!/usr/bin/env node
import cdk = require('@aws-cdk/cdk');
import { BudgetStack } from '../lib/budget-stack';

const app = new cdk.App();
new BudgetStack(app, 'BudgetStack');
app.run();
