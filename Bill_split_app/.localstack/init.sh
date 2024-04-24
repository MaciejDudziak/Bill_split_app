#!/bin/bash

awslocal sqs create-queue --queue-name worker-queue
awslocal s3api create-bucket --bucket debts-bucket
