import click
import redis
import json
import time

from typing import List, Dict, Callable
from micra_store import Listener, Coordinator, retry
from micra_store.command import Command, StartCommand, QuitCommand, StatusCommand, ListCommand, ViewCommand, MessageCommand, SubprocessCommand, ListenCommand, ForwardCommand
from micra_store import structure
from .service import MicraServiceListener, LoggingServiceListener

class Controller(Coordinator):
  eternal: bool=True

  @property
  def definitions(self) -> List[structure.Element]:
    return [
      structure.json_type, 
      structure.json_object_type, 
      structure.micra_command,
      structure.micra_content_types, 
      structure.micra_structures, 
      structure.micra_definitions,
      structure.micra_structures_with_types,
      structure.micra_commands,
      structure.job_identifier,
      structure.job_version,
      structure.job_instance,
      structure.job_appointment,
      structure.jobs_active,
      structure.jobs_ready,
      structure.jobs_ready_almacen,
      structure.jobs_scored,
    ]

  @property
  def commands(self) -> List[Command]:
    forward_commands = []
    for service in filter(lambda l: isinstance(l, MicraServiceListener), self.listeners.keys()):
      forward_commands.append(ForwardCommand(
        name=f'{service.command[-1]}{"^" * len(list(filter(lambda f: f.name.startswith(service.command[-1]), forward_commands)))}', key=service.list_name,
        context=self
      ))
    return [
      StartCommand(all_names=['controller'], context=self),
      Prepare(context=self),
      StatusCommand(context=self),
      ListCommand(context=self),
      ViewCommand(context=self),
      MessageCommand(context=self),
      QuitCommand(context=self),
      SubprocessCommand(context=self),
      ListenCommand(context=self),
      *forward_commands,
    ]

  @property
  def listener_starters(self) -> Dict[str, Callable[[], None]]:
    return {
      **super().listener_starters,
      'ping': self.start_ping,
      'almacen': lambda : self.start_micra_service(name='micra_manager', role='almacen'),
      'manager': lambda : self.start_micra_service(name='micra_manager', role='manager'),
      'scheduler': lambda : self.start_micra_service(name='micra_scheduler', role='scheduler'),
      'logging': lambda : self.start_listener(LoggingServiceListener(redis=self.redis, pdb_enabled=self.pdb_enabled)),
    }

  def start_micra_service(self, name: str, role: str):
    service = MicraServiceListener(name=name, command=[role], redis=self.redis, eternal=self.eternal, pdb_enabled=self.pdb_enabled)
    self.start_listener(service)

  def start_ping(self):
    @retry(enabled=False, pdb_enabled=self.pdb_enabled, queue=self.queue)
    def ping():
      self.start_subprocess(
        run_args=[
          'ping',
          'example.com',
          '-i', '10',
        ],
        eternal=True
      )
    self.start_listener(Listener(runner=ping))

class Prepare(Command[Controller]):
  @property
  def name(self) -> str:
    return 'prepare'

  @property
  def click_command(self) -> click.Command:
    @click.command()
    def prepare():
      self.prepare()
    return prepare

  def prepare(self):
    r = self.context.redis
    stream_groups_to_create = {
      'almacen_ready_jobs': [ 'almacen_worker_group' ],
    }
    for stream, groups in stream_groups_to_create.items():
      for group in groups:
        self.try_create_group(r, stream, group)
    
  def try_create_group(self, r: any, stream: str, group: str):
    try:
      r.xgroup_create(stream, group, '$', True)
    except redis.exceptions.ResponseError as e:
        print(e, f': {stream}->{group}')

