import os

from redis import Redis
from queue import Queue, Empty
from typing import Callable, Optional, List
from micra_store import Listener, uuid, retry, MicraResurrect
from micra_store.command import Command
from environments import environment
from moda import process

class ServiceListener(Listener):
  id: str
  command: List[str]
  script_path: str
  retry: bool
  eternal: bool
  pdb_enabled: bool
  redis: Redis

  def __init__(self, name: str, command: List[str], redis: Redis, script_path: Optional[str]=None, retry: bool=True, eternal: bool=False, pdb_enabled: bool=False):
    super().__init__(name=name, info={'command': Command.quote_command(command)})
    self.id = uuid()
    self.command = command
    self.script_path = script_path if script_path is not None else os.path.join(environment['service']['services_path'], self.name, f'{self.name}.sh')
    self.retry = retry
    self.eternal = eternal
    self.pdb_enabled = pdb_enabled
    self.redis = redis

  @property
  def runner(self) -> Callable[[], None]:
    script_path = self.script_path
    command = self.command
    eternal = self.eternal
    input_queue = self.input_queue

    @retry(enabled=self.retry, pdb_enabled=self.pdb_enabled)
    def service_runner():
      run_args = [
        script_path,
        *command,
      ]
      process.call_process(run_args=run_args)
      should_resurrect = eternal
      try:
        should_resurrect = input_queue.get_nowait()
      except Empty:
        pass
      if should_resurrect:
        raise MicraResurrect
    
    return service_runner

  def stop(self) -> bool:
    self.input_queue.put(False)
    return True

class MicraServiceListener(ServiceListener):
  def __init__(self, name: str, command: List[str], redis: Redis, script_path: Optional[str]=None, retry: bool=True, eternal: bool=False, pdb_enabled: bool=False):
    super().__init__(name=name, command=command, redis=redis, script_path=script_path, retry=retry, eternal=eternal, pdb_enabled=pdb_enabled)
    self.command = [
      '-I',
      '-c', f'listen start accept {self.list_name}',
      *self.command,
    ]

  @property
  def list_name(self) -> str:
    return f'service_{self.name}_{self.id}'

  def stop(self) -> bool:
    super().stop()
    self.redis.rpush(self.list_name, 'quit')
    return True

  def clean(self):
    self.redis.delete(self.list_name)

class LoggingServiceListener(ServiceListener):
  def __init__(self, redis: Redis, retry: bool=True, eternal: bool=True, pdb_enabled: bool=False):
    super().__init__(name='logging', command=[], redis=redis, script_path='./micra_logger.sh', retry=retry, eternal=eternal, pdb_enabled=pdb_enabled)
