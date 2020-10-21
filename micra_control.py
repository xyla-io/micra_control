from gevent import monkey
monkey.patch_all()

from typing import Optional
import click

from environments import environment, set_environment
from micra_store.user import run, RunContext
from micra_control import Controller

controller = Controller(config=environment)
run.add_micra_commands(commands=list(filter(lambda c: c.can_run, controller.commands)))

if __name__ == '__main__':
  run()
