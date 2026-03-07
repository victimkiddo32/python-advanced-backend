import socket
import select
from time import time
from .command import CommandHandler
from .storage import DataStore

class RedisServer:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket = None
        self.clients = {}
        self.storage = DataStore()
        self.command_handler = CommandHandler(self.storage)
        self.last_cleanup_time = time.time()
        self.cleanup_interval = 0.1 # active expiration every 100ms


    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.server_socket.setblocking(False)
        self.running = True
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
                current_time= time()
                if current_time - self.last_cleanup_time >=self.cleanup_interval:
                    #background cleanup of expired keys
                    self.last_cleanup_time=current_time
                    self.background_cleanup()


            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Event loop error: {e}")

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
        return self.command_handler.execute(parts[0], *parts[1:])
    
    def background_cleanup(self):
        try:
            expired_count=self.storage.cleanup_expired_keys()
            if expired_count>0:
                print(f"Cleaned up {expired_count} expired keys")
        
        except Exception as e:
            print(f"Error during background cleanup : {e}")





    def _disconnect_client(self, client):
        client.close()
        self.clients.pop(client, None)

    def stop(self):
        self.running = False
        for client in list(self.clients.keys()):
            self._disconnect_client(client)
        if self.server_socket:
            self.server_socket.close()