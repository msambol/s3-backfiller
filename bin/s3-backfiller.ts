#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { S3BackfillerStack } from '../lib/s3-backfiller-stack'

const app = new App()

const environment = app.node.tryGetContext('environment');
const envContext = app.node.tryGetContext(environment);
if (!envContext) { throw new Error(`Invalid environment: ${environment}`) }

console.log(`Deploying to ${environment}..`)

const regionalEnv = {
  env: {
    region: envContext.region,
    account: envContext.account,
  }
}

new S3BackfillerStack(app, `S3BackfillerStack-${environment}`, {
  environment,
  ...regionalEnv
})
