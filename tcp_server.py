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

    
    def handle_client(self,conn,addr):
        print(f"Connected by {addr}")
        buffer=""
        try:
            while True:
                data=conn.recv(1024).decode()
                #decode means convert bytes to string
                if not data:
                    break

                buffer+=data

                if "r\n" in buffer or "\n" in buffer:
                    command=buffer.strip() #strip means remove spaces and newlines from the beginning and end of the string

                    if command.upper()=="PING":
                        conn.send(b"+PONG\r\n")
                    else:
                        conn.send(b"-ERR Unknown command\r\n")

                    buffer="" #clear the buffer for the next command

                
        except Exception as e:
            print(f"_ERR {str(e)} from {addr}\r\n".encode())

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