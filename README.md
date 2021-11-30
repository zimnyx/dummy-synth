# Tools for data synthesization and evaluation


## Requirements

- Python 3.8

## How to set up the project (Linux/bash or Windows/cmd)

If you're using Windows, for all commands below replace ". env/bin/activate" with ".\env\Scripts\activate"

```
# you may need to use "python" here (no version), if you're using Python 3.8 in Windows cmd shell.
python3 -m venv env
. env/bin/activate
pip install -r requirements.txt

```

## How to run sythesization (Linux/bash)

```
. env/bin/activate

# for local directory
# synthesize
python synthesize_refactored.py synthesize data_dir

# evaluate
python synthesize_refactored.py evaluate data_dir

# both synthesize & evaluate - will be faster than separate commands (less disk IO)
python synthesize_refactored.py synthesize-and-evaluate data_dir

# for S3 bucket directory (S3 running on localstack)
AWS_DEFAULT_REGION=us-east-1 AWS_SECRET_ACCESS_KEY=test AWS_ACCESS_KEY_ID=test python synthesize_refactored.py synthesize-s3 my_bucket data_dir --s3-endpoint-url https://localhost.localstack.cloud:4566

```


## How to start local instance of S3/localstack (Linux/bash)

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test LOCALSTACK_SERVICES=s3 docker run --rm -it -p 4566:4566 -p 4571:4571 localstack/localstack

```
Just connect to this service using with your favourite tools using endpoint-url https://localhost.localstack.cloud:4566 and create my_bucket with directory containing CSV/Parquet files.


## How to set up test environment (Linux/bash)

```
python -m venv env
. env/bin/activate
pip install -r requirements_test.txt
```

## How to run tests (Linux/bash)

```
. env/bin/activate
pytest tests
```

