import os
import csv
from io import StringIO
import boto3


def read_csv_from_s3(bucket, key):  # read csv file from s3 bucket
    s3 = boto3.resource('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    obj = s3.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    return list(csv.reader(StringIO(data)))


def write_csv_to_s3(bucket, key, transfers): # write csv file to s3 bucket
    s3 = boto3.resource('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for transfer in transfers:
        writer.writerow(transfer)
    s3.Object(bucket, key).put(Body=csv_buffer.getvalue())


def optimize_transfers(transactions):

    balances = []

    for data in transactions:
        is_in_list = False  # data for lender
        for user in balances:
            if user[0] == data[0]:
                user[1] += int(data[2])
                is_in_list = True
                break
        if not is_in_list:
            balances.append([data[0], int(data[2])])

        is_in_list = False  # data for loaner
        for user in balances:
            if user[0] == data[1]:
                user[1] -= int(data[2])
                is_in_list = True
                break
        if not is_in_list:
            balances.append([data[1], -int(data[2])])

    balances = sorted(balances, key=lambda x: x[1])
    balances = [x for x in balances if x[1] != 0]  # sort list and remove zeros

    solved = []

    settled = 0
    current = 0
    unsettled = len(balances) - 1
    while settled <= len(balances) - 1:  # compare biggest debt with the biggest loan

        if abs(balances[current][1]) > abs(balances[unsettled][1]):  # if negative balance is bigger
            solved.append((balances[current][0], balances[unsettled][0], abs(balances[unsettled][1])))
            balances[current][1] += balances[unsettled][1]
            balances[unsettled][1] = 0
            unsettled -= 1
            settled += 1

        elif abs(balances[current][1]) < abs(balances[unsettled][1]):  # if positive balance is bigger
            solved.append((balances[current][0], balances[unsettled][0], abs(balances[current][1])))
            balances[unsettled][1] += balances[current][1]
            balances[current][1] = 0
            current += 1
            settled += 1

        else:  # if they are equal
            solved.append((balances[current][0], balances[unsettled][0], abs(balances[current][1])))
            balances[current][1] = 0
            balances[unsettled][1] = 0
            current += 1
            unsettled -= 1
            settled += 2

    return solved


def process_debts():
    sqs = boto3.client('sqs', endpoint_url=os.environ.get('SQS_ENDPOINT_URL'))
    queue_url = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/worker-queue"
    s3_bucket = "debts-bucket"

    while True:
        response = sqs.receive_message(  # receive sqs messages
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  # wait up to 20 seconds for a message
        )

        if 'Messages' in response:  # process message if received
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            data_key = message['Body'][13:49]
            transactions = read_csv_from_s3(s3_bucket, data_key)
            transfers = optimize_transfers(transactions)
            result_key = f"{data_key}_results"
            write_csv_to_s3(s3_bucket, result_key, transfers)

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )

        else:  # wait for another message if no message in queue for 20 seconds
            print("No messages in queue, waiting.....")


if __name__ == "__main__":
    process_debts()
