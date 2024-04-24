<h1>Bill_split_app</h1>

<h4>Stack:</h4> Aws: S3 bucket, SQS queue, localhost, docker, docker compose, python

To run application go into project directory in command line and type: <br>docker-compose up --build<br>
When the applications starts go to: <br>http://localhost:8000/docs#/<br> to upload files and fetch results from cloud.

<h2>Use guide:</h2>

<h4>Input:</h4>
Csv file with debts list split by commas, each debt in single line, for example:

John,Mary,20<br>
John,Andrew,50<br>
Mary,Andrew,30<br>

Where first person is the one who paid, second one is the one who got paid for and is in debt and the third field in amount of money paid

<h4>Returns:</h4> ID of our csv data file

<h4>Output:</h4>
Output returned will be a csv file fetched by ID and it can be either:
a: our input file as csv file, same as the one we uploaded
b: our result file, which looks is in the same format as the input file, except it now contains list of transfers, for example:

Andrew,John,70<br>
Andrew,Mary,10<br>

Where first person is the one who is in debt and makes transaction, second one is person who receives money and the third field is amount of money transferred
