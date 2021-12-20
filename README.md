# Command line tools for data synthesization and evaluation

## What is it?
This is example of commandline tool that allows to traverse directories on various storages and run synthesize/evaluate command on each file.
Currently local directory and S3 storages are supported.


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

Then create local_config.py using local_config.py.sample as template.


## How to run sythesization (Linux/bash)

```
. env/bin/activate

# check out possible commands
python run.py --help

# for local directory
# synthesize
python run.py synthesize data_dir

# evaluate
python run.py evaluate data_dir

# for S3 bucket directory (S3 running on localstack)
AWS_DEFAULT_REGION=us-east-1 AWS_SECRET_ACCESS_KEY=test AWS_ACCESS_KEY_ID=test python run.py synthesize-s3 my_bucket data_dir --s3-endpoint-url https://localhost.localstack.cloud:4566

```



## How to run tests (Linux/bash)

```
. env/bin/activate
pip install -r requirements_test.txt
pytest tests
```



## How to start local instance of S3/localstack (Linux/bash)

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test LOCALSTACK_SERVICES=s3 docker run --rm -it -p 4566:4566 -p 4571:4571 localstack/localstack

```
Just connect to this service using with your favourite tools using endpoint-url https://localhost.localstack.cloud:4566 and create my_bucket with directory containing CSV/Parquet files.
