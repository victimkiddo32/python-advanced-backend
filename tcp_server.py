import socket
import threading

class TCPserver:
    def __init__(self,host="localhost",port=6379):

        #create a tcp/IPv4 socket
        self.server_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #AF_INET: address family for IPv4 :'127.0.0.1'

        #set socket options to reuse the address
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
        
        #bind the socket to the specified host and port
        self.server_socket.bind((host,port))
        
        #start listeming for incoming connnections
        self.server_socket.listen(5)

        print(f"Server is listening on {host}: {port}")

    
    
    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")
        buffer = ""
    
        try:
            while True:
                # 1. Receive raw data from the socket
                raw_data = conn.recv(1024)
                if not raw_data:
                    # Client closed the connection
                    break

                # 2. Decode and add to our cumulative buffer
                buffer += raw_data.decode('utf-8')

                # 3. Check if we have at least one complete message (ending in \n)
                # We use a while loop here in case multiple commands arrived in one packet
                while "\n" in buffer:
                    # Split the buffer: 'line' is the command, 'buffer' is whatever is left
                    line, buffer = buffer.split("\n", 1)
                
                    # 4. Process the command
                    command = line.strip().upper()
                
                    # Only print when we have the FULL command
                    if command:
                        print(f"Data received from {addr}: {command}")

                    if command == "PING":
                        conn.send(b"+PONG\r\n")
                    elif command == "":
                        # Ignore empty lines/extra newlines
                        continue
                    else:
                        conn.send(b"-ERR Unknown command\r\n")

        except Exception as e:
            print(f"Error handling client {addr}: {e}")
    
        finally:
            print(f"Closing connection with {addr}")
            conn.close()



    def run(self):
        #accept incoming client connections and spin up a new thread to handle each client
        try:
            while True:
                conn, addr = self.server_socket.accept()
                
                thread=threading.Thread(target=self.handle_client,args=(conn,addr))
                thread.start()

        except KeyboardInterrupt:
            print("\nShutting down the server...")

        finally:
            self.server_socket.close()





if __name__ == "__main__":
    server=TCPserver()
    server.run()