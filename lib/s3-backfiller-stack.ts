import { Stack, StackProps, Duration } from 'aws-cdk-lib'
import { Construct } from 'constructs'
import { aws_iam as iam } from 'aws-cdk-lib'
import { aws_sns as sns } from 'aws-cdk-lib'
import { aws_sqs as sqs } from 'aws-cdk-lib'
import { aws_lambda as lambda } from 'aws-cdk-lib'
import { aws_stepfunctions as sfn } from 'aws-cdk-lib'
import { aws_stepfunctions_tasks as tasks } from 'aws-cdk-lib'
import { aws_sns_subscriptions as subscriptions } from 'aws-cdk-lib'
import { aws_lambda_event_sources as lambdaEventSources } from 'aws-cdk-lib'
import * as lambdaPython from '@aws-cdk/aws-lambda-python-alpha'

import * as path from 'path'

export interface S3BackfillerStackProps extends StackProps {
  readonly environment: string
}

export class S3BackfillerStack extends Stack {
  constructor(scope: Construct, id: string, props: S3BackfillerStackProps) {
    super(scope, id, props)
    const environment = props.environment

    const s3ObjectsDlq = new sqs.Queue(this, 'S3ObjectsDlq', {
      queueName: `s3-objects-dlq-${props.environment}`,
      retentionPeriod: Duration.days(14),
      visibilityTimeout: Duration.minutes(3),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      receiveMessageWaitTime: Duration.seconds(20),
    })

    const s3ObjectsQueue = new sqs.Queue(this, 'S3ObjectsQueue', {
      queueName: `s3-objects-${props.environment}`,
      retentionPeriod: Duration.days(14),
      visibilityTimeout: Duration.minutes(15*6), // recommend to 6 times Lambda timeout
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      receiveMessageWaitTime: Duration.seconds(20),
      deadLetterQueue: {
        queue: s3ObjectsDlq,
        maxReceiveCount: 1,
      }
    })

    const workerLambda = new lambdaPython.PythonFunction(this, 'WorkerLambda', {
      functionName: `s3-object-worker-${environment}`,
      description: 'Polls and processes S3 object pointers from SQS queue',
      entry: path.join(__dirname, '..', 'lambdas'), 
      runtime: lambda.Runtime.PYTHON_3_9, 
      index: 'worker.py',
      handler: 'handler',
      timeout: Duration.minutes(15), 
      memorySize: 256,
      retryAttempts: 0, // set to not retry, but you can change this
      environment: {
        DELETE_ORIGINAL_FILES: 'False',
      },
    })
    s3ObjectsQueue.grantConsumeMessages(workerLambda)
    workerLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: [
        's3:GetObject*',
        's3:PutObject*',
        's3:DeleteObject*',
      ],
    }))
    workerLambda.addEventSource(new lambdaEventSources.SqsEventSource(s3ObjectsQueue, {batchSize: 1}))

    this.createStepFunction(environment, s3ObjectsQueue)
  }

  private createStepFunction(environment: string, s3ObjectsQueue: sqs.Queue) {
    const loaderLambda = new lambdaPython.PythonFunction(this, 'LoaderLambda', {
      functionName: `s3-object-loader-${environment}`,
      description: 'Loads S3 object pointers into SQS queue',
      entry: path.join(__dirname, '..', 'lambdas'), 
      runtime: lambda.Runtime.PYTHON_3_9, 
      index: 'loader.py',
      handler: 'handler',
      timeout: Duration.minutes(15),
      memorySize: 256,
      retryAttempts: 0,
      environment: {
        SQS_QUEUE_URL: s3ObjectsQueue.queueUrl,
      },
    })
    s3ObjectsQueue.grantSendMessages(loaderLambda)
    loaderLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: ['s3:ListBucket*'],
    }))     
    
    const sfnFallbackHandler = this.sfnFailureChain(environment)
    const sfnFallbackProps = { resultPath: '$.error' }
    const sfnSuccess = new sfn.Succeed(this, 'Success')

    const loaderInvoker = new tasks.LambdaInvoke(this, 'Load S3 objects into SQS queue', {
      lambdaFunction: loaderLambda,
      inputPath: '$',
      outputPath: '$.Payload',
    })
    loaderInvoker.addCatch(sfnFallbackHandler, sfnFallbackProps)

    const nextTokenChoice = new sfn.Choice(this, 'Are there more S3 objects to process?')
    nextTokenChoice.when(sfn.Condition.isPresent('$.next_token'), loaderInvoker)
    nextTokenChoice.otherwise(sfnSuccess)
    
    const definition = loaderInvoker.next(nextTokenChoice)
    const s3BackfillerSfn = new sfn.StateMachine(this, 'StepFunction', {
      timeout: Duration.days(30),
      definition: definition,
      stateMachineName: `s3-backfiller-${environment}`,
    })

    return s3BackfillerSfn
  }

  private sfnFailureChain(environment: string): sfn.IChainable {
    const topic = new sns.Topic(this, 'SfnFailures', { 
      topicName: `sfn-failures-${environment}`,
      displayName: `sfn-failures-${environment}`,
    })
    const queue = new sqs.Queue(this, 'SfnFailuresDlq', {
      queueName: `sfn-failures-dlq-${environment}`,
      retentionPeriod: Duration.days(14)
    })
    topic.addSubscription(new subscriptions.SqsSubscription(queue))
    const fallback = new tasks.SnsPublish(this, 'Fallback', {
      topic,
      message: sfn.TaskInput.fromText(sfn.JsonPath.entirePayload),
    })
    return fallback.next(new sfn.Fail(this, 'Failure'))
  }
}
