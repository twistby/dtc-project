import os

os.system('prefect agent start -q default')
os.system('python deploy_blocks.py')