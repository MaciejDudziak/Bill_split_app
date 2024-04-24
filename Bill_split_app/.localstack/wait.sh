#!/bin/bash

awslocal sqs wait queue-exists --queue-name worker-queue
awslocal s3api wait bucket-exists --bucket debts-bucket
