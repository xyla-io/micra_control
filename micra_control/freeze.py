
import pprint
pp = pprint.PrettyPrinter(indent=2).pprint
import json
import redis
from typing import Dict

from .lib.process import Process, RedisProcess, MicraProcess
from micra_store import Coordinator

keyspace = 'micra_health'

class Freeze(Coordinator):
  def __init__(self, config: Dict[str, any], pdb_enabled: bool=False, silent: bool=False):
    super().__init__(config=config, pdb_enabled=pdb_enabled)
    self.silent = silent

  def start(self):
    redis_processes = Process.find_processes(RedisProcess)
    micra_processes = Process.find_processes(MicraProcess)

    if not self.silent:
      pp(list(map(lambda x: x.to_dict(), micra_processes)))
      pp(list(map(lambda x: x.to_dict(), redis_processes)))

    r = redis.Redis(host=self.config['host'], port=self.config['port'], password=self.config['password'])
    pipeline = r.pipeline()
    pipeline.delete(keyspace)

    def put_processes(processes, pipeline):
      for p in processes:
        pipeline.hset(keyspace, f'{p.name()}:{p.pid()}:{p.environment()}', json.dumps(p.to_dict()))

    put_processes(micra_processes, pipeline)
    put_processes(redis_processes, pipeline)
    pipeline.execute()

    super().start()
