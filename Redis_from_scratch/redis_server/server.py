import socket
import select
import time
from .command import CommandHandler
from .storage import DataStore
from .persistence import PersistenceManager,PersistenceConfig

class RedisServer:
    def __init__(self, host='localhost', port=6379):
        

        self.host = host
        self.port = port
        self.running = False
        self.server_socket = None
        self.clients = {}
        self.storage = DataStore()
        
        #Initialize persistence
        self.persistence_config= self.persistence_config or PersistenceConfig()
        self.persitence_manager= PersistenceManager(self.persistence_config)
        
         # Command handler needs reference to persistence manager for logging
        self.command_handler = CommandHandler(self.storage, self.persistence_manager)
        
        self.last_cleanup_time = time.time()
        self.last_persistence_time = time.time()
        self.cleanup_interval = 0.1  # 100ms cleanup interval
        self.persistence_interval = 0.1  # 100ms persistence tasks interval

    def start(self):
        
        #Start persistance
        self.persitence_manager.start()

        # Recover data from persistence files
        print("Recovering data from persistence files...")
        recovery_success= self.persitence_manager.recover_data(self.storage,self.command_handler)
        if recovery_success:
            print("Data recovery completed successfully")
        else:
            print("Data recovery failed, starting with empty database")


    
         
        # Initialize server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        self.running = True
        # Start the event loop
        print(f"Redis style server is listening on {self.host}:{self.port}")
        self._event_loop()

    def _event_loop(self):
        while self.running:
            try:
                read, _, _ = select.select(
                    [self.server_socket] + list(self.clients.keys()),
                    [], [], 0.05
                )
                
                # print(f"Server Socket:{self.server_socket} ")
                # print(f"\n read: {read}")

                for sock in read:
                    if sock is self.server_socket:
                        self._accept_client() # create a new client connection and add it to the clients dictionary
                    else:
                        self._handle_client(sock)

                #Active expiration check
                current_time= time.time()


                # Background cleanup of expired keys every 100ms
                if current_time - self.last_cleanup_time >=self.cleanup_interval:
                    self.background_cleanup()
                    self.last_cleanup_time=current_time
                    
                # Persistence tasks every 100ms
                if current_time - self.last_persistence_time >= self.persistence_interval:
                    self._background_persistence_tasks()
                    self.last_persistence_time = current_time

       

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Event loop error: {e}")


    def _background_persistence_tasks(self):
        """Perform background persistence tasks"""
        try:
            self.persistence_manager.periodic_tasks()
        except Exception as e:
            print(f"Error during persistence tasks: {e}")


    def _accept_client(self):
        client, addr = self.server_socket.accept()
        # print(f"\nClient : {client}")
        # print(f"\nAddress : {addr}")
        client.setblocking(False)
        self.clients[client] = {"addr": addr, "buffer": b""}
        #print(f"\nClients list length: {len(self.clients)}")
        client.send(b"+OK\r\n")



    def _handle_client(self, client):
        try:
            data = client.recv(4096)
            if not data:
                self._disconnect_client(client)
                return
                
            self.clients[client]["buffer"] += data
            self._process_buffer(client)
            
        except ConnectionError:
            self._disconnect_client(client)



    def _process_buffer(self, client):
        buffer = self.clients[client]["buffer"]
        
        while b"\r\n" in buffer:
            command, buffer = buffer.split(b"\r\n", 1)
            # print(f"buffer: {buffer}")
            # print(f"Command: {command}")
            if command:
                response = self._process_command(command.decode())
                client.send(response)
        
        self.clients[client]["buffer"] = buffer


    def _process_command(self, command_line):
        parts = command_line.strip().split()
        # print(f"Parts: {parts}")
        if not parts:
            return self.error("empty command")
        # 1. Normalize the command name to UPPERCASE
        command_name = parts[0].upper()
    
         # 2. Keep the arguments as they are (keys/values are case-sensitive!)
        args = parts[1:]
        return self.command_handler.execute(command_name, *args)
    

    def background_cleanup(self):
        try:
            expired_count=self.storage.cleanup_expired_keys()
            if expired_count>0:
                print(f"Cleaned up {expired_count} expired keys")
        
        except Exception as e:
            print(f"Error during background cleanup : {e}")





    def _disconnect_client(self, client):
        try:
            addr = self.clients.get(client, {}).get("addr", "unknown")
            print(f"Client {addr} disconnected")
            client.close()
            self.clients.pop(client, None)
        except Exception as e:
            print(f"Error disconnecting client: {e}")

    def stop(self):
        self.running = False

        #Stop persistence
        try:
            self.persitence_manager.stop()
        except Exception as e:
            print(f"Error stopping persistence: {e}")


        # Close client connections
        for client in list(self.clients.keys()):
            self._disconnect_client(client)


        # Close server socket    
        if self.server_socket:
            self.server_socket.close()

        print("Server stopped")