import os
import click
import subprocess

from typing import Optional, List, Any as any
from environments import set_environment, environment

@click.command()
@click.argument('environment_name', default='')
def run(environment_name: str):
  if environment_name:
    set_environment(identifier=environment_name)
  os.environ['REDISCLI_AUTH'] = environment['redis']['password']
  subprocess.call([
    'redis-cli',
    '-h', environment['redis']['host'],
    '-p', str(environment['redis']['port']),
  ])

if __name__ == '__main__':
  run()
