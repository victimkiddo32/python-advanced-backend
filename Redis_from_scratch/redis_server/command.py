import time

from .storage import DataStore
from .response import *

class CommandHandler:
    def __init__(self, storage):
        self.storage = storage
        self.command_count=0
        self.commands = {
            "PING": self.ping,
            "ECHO": self.echo,
            "SET": self.set,
            "GET": self.get,
            "DEL": self.delete,
            "EXISTS": self.exists,
            "KEYS": self.keys,
            "FLUSHALL": self.flushall,
            "INFO": self.info,
            "EXPIRE": self.expire,
            "EXPIREAT": self.expire_at,
            "TTL": self.ttl,
            "PTTL": self.pttl,
            "PERSIST": self.persist,
            "TYPE": self.get_type,
            #Persistence commands
            "BGREWRITEAOF": self.bgrewriteaof,
            "CONFIG": self.config_command,
            "DEBUG": self.debug_command
        }

    def execute(self, command, *args): # execute("SET", "mykey", "myvalue"), args = ("mykey", "myvalue")
        self.command_count+=1
        cmd = self.commands.get(command.upper()) # get the command function
        # print(f"Command : {command}")
        # print(f"Args : {args}")
        if cmd:
            return cmd(*args)
        
            # Log write commands to AOF
            if self.persistence_manager:
                self.persistence_manager.log_write_command(command, *args)

            return result
            return cmd(*args)

        return error(f"unknown command '{command}'")

    def ping(self, *args):
        return pong()

    def echo(self, *args):
        return simple_string(" ".join(args)) if args else simple_string("")

    def set(self, *args):
        # SET myvalue 5 EX 30
        if len(args) < 2:
            return error("wrong number of arguments for 'set' command")
        key=args[0]
        value=" ".join(args[1:])

        # Parse optional EX parameter for expiration
        expiry_time=None
        if len(args) > 4 and args[-2].upper() == "EX":
            try:
                seconds = int(args[-1])
                expiry_time= time.time() + seconds
                value=" ".join(args[1:-2])
            
            except ValueError:
                return error("invalid expire time")

        self.storage.set(key, value, expiry_time)
        return ok()

    def get(self, *args):
        if len(args) != 1:
            return error("wrong number of arguments for 'get' command")
        return bulk_string(self.storage.get(args[0]))

    def delete(self, *args):
        if not args:
            return error("wrong number of arguments for 'del' command")
        return integer(self.storage.delete(*args))

    def exists(self, *args):
        if not args:
            return error("wrong number of arguments for 'exists' command")
        return integer(self.storage.exists(*args))

    def keys(self, *args):
        keys = self.storage.keys()
        if not keys:
            return array([])
        return array([bulk_string(key) for key in keys])

    def flushall(self, *args):
        self.storage.flush()
        return ok()
    
    
    def expire(self, *args):
        # EXPIRE myvalue 30
        if len(args)!=2:
            return error("Wrong number of arguments for expire command")
        
        key = args[0]
        try:
            seconds = int (args[1])
            if seconds <=0:
                return integer(0)
            success=self.storage.expire(key,seconds)
            return integer(1) if success else integer(0)
        except ValueError:
            return error("invalid expire time")


    def expire_at(self,*args):
        if len(args) != 2:
            return error("Wrong number of arguments for expireat command")
        
        key = args[0]
        try:
            # Timestamps are usually large integers or floats
            timestamp = float(args[1])
            success = self.storage.expire_at(key, timestamp)
        
            # Redis returns 1 if expiry was set, 0 if key doesn't exist
            return integer(1) if success else integer(0)
        except ValueError:
            return error("invalid expireat timestamp")
    
    
    def ttl(self,*args):
        # handle the command and pass the arguements to the storage layer
        # TTL myvalue
        ttl_value = self.storage.ttl(args[0])

        if ttl_value == -1:
            return simple_string(f"No expiration set for key: {args[0]}")
        elif ttl_value == -2:
            return simple_string(f"Key has expired: {args[0]}")
        # Return TTL as an integer
        return integer(ttl_value)
    
        return integer(self.storage.ttl(key))

    def pttl(self,*args):
        # PYYL myvalue
        if len(args)!=1:
            return error("Wrong number of arguements for pttl ")
        
        key=args[0]
        return integer(self.storage.pttl(key))
         
    
    def persist(self,*args):
        # PERSIST myvalue
        if len(args)!=1:
            return error("Wrong number of arguements for persist ")
        
        key=args[0]
        success=self.storage.persist(key)
        return integer(1) if success else integer(0)
    
    
    def get_type(self,*args):
        # TYPE myvalue
        if len(args)!=1:
            return error("Wrong number of arguements for get_type ")
        key=args[0]
        return simple_string(self.storage.get_type(key))
    



    def info(self, *args):
        info = {
            "server": {
                "redis_version": "7.0.0-custom",
                "redis_mode": "standalone"
            },
            "stats": {
                "total_commands_processed": 0  # Would track this in server
            },
            "keyspace": {
                "db0": f"keys={len(self.storage.keys())},expires=0"
            }
        }
        sections = []
        for section, data in info.items():
            sections.append(f"#{section}")
            sections.extend(f"{k}:{v}" for k, v in data.items())
        return bulk_string("\n".join(sections))