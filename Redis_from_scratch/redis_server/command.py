import time

from .storage import DataStore
from .response import *

class CommandHandler:
    def __init__(self, storage, persistence_manager=None):
        self.storage = storage
        self.command_count=0
        self.persistence_manager = persistence_manager
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
            result= cmd(*args)
        
            # Log write commands to AOF
            if self.persistence_manager:
                self.persistence_manager.log_write_command(command, *args)

            return result

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
        
        pttl_value = self.storage.pttl(args[0])
        if pttl_value == "-1":
            return simple_string(f"No expiration set for key: {args[0]}")
        elif pttl_value == "-2":
            return simple_string(f"Key has expired: {args[0]}")
        # Return PTTL as an integer
        return integer(pttl_value)
    
         
    
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
        data_type = self.storage.get_type(args[0])
        return simple_string(data_type)

    



    def info(self, *args):
        info = {
            "server": {
                "redis_version": "7.0.0-custom",
                "redis_mode": "standalone"
            },
            "stats": {
                "total_commands_processed": self.command_count  
            },
            "keyspace": {
                "db0": f"keys={len(self.storage.keys())},expires=0"
            }
        }

        # Add persistence info if available
        if self.persistence_manager:
            persistence_stats = self.persistence_manager.get_stats()
            info["persistence"] = {
                "aof_enabled": int(persistence_stats.get('aof_enabled', False)),
                "aof_last_sync_time": int(persistence_stats.get('last_aof_sync_time', 0)),
                "aof_filename": persistence_stats.get('aof_filename', '')
            }

        sections = []
        for section, data in info.items():
            sections.append(f"#{section}")
            # Corrected the formatting to match standard Redis output (indentation)
            for k, v in data.items():
                sections.append(f"{k}:{v}")
            sections.append("")  # Empty line between sections
        
        return bulk_string("\n".join(sections))
    

    
    def _format_bytes(self, bytes_count):
        """Format bytes in human readable format"""
        for unit in ['B', 'K', 'M', 'G']:
            if bytes_count < 1024:
                return f"{bytes_count:.1f}{unit}"
            bytes_count /= 1024
        return f"{bytes_count:.1f}T"
    
    # Persistence Commands
    def bgrewriteaof(self, *args):
        """Background AOF rewrite"""
        if not self.persistence_manager:
            return error("persistence not enabled")
        
        try:
            success = self.persistence_manager.rewrite_aof_background(self.storage)
            if success:
                return simple_string("Background AOF rewrite started")
            else:
                return error("background AOF rewrite failed to start")
        except Exception as e:
            return error(f"bgrewriteaof error: {e}")
        

    def config_command(self, *args):
        """CONFIG command for persistence settings"""
        if not args:
            return error("wrong number of arguments for 'config' command")
        
        subcommand = args[0].upper()
        
        if subcommand == "GET":
            if len(args) != 2:
                return error("wrong number of arguments for 'config get' command")
            
            parameter = args[1].lower()
            if self.persistence_manager:
                config_value = self.persistence_manager.config.get(parameter)
                if config_value is not None:
                    return array([bulk_string(parameter), bulk_string(str(config_value))])
            
            return array([])
        
        elif subcommand == "SET":
            if len(args) != 3:
                return error("wrong number of arguments for 'config set' command")
            
            parameter = args[1].lower()
            value = args[2]
            
            if self.persistence_manager:
                try:
                    # Convert string values to appropriate types
                    if parameter in ['aof_enabled', 'persistence_enabled']:
                        value = value.lower() in ('true', '1', 'yes', 'on')
                    
                    self.persistence_manager.config.set(parameter, value)
                    return ok()
                except Exception as e:
                    return error(f"config set error: {e}")
            
            return error("persistence not enabled")
        
        else:
            return error(f"unknown CONFIG subcommand '{subcommand}'")
    
    def debug_command(self, *args):
        """DEBUG command for development/testing"""
        if not args:
            return error("wrong number of arguments for 'debug' command")
        
        subcommand = args[0].upper()
        
        if subcommand == "RELOAD":
            if self.persistence_manager:
                try:
                    # Reload data from persistence files
                    success = self.persistence_manager.recover_data(self.storage, self)
                    if success:
                        return ok()
                    else:
                        return error("reload failed")
                except Exception as e:
                    return error(f"reload error: {e}")
            else:
                return error("persistence not enabled")
        
        else:
            return error(f"unknown DEBUG subcommand '{subcommand}'")
        
