#******************************************************************************#
#                   ____        __   _____       __   _  __                    #
#                  / __ \__  __/ /_ / ___/__  __/ /_ | |/ /                    #
#                 / /_/ / / / / __ \\__ \/ / / / __ \|   /                     #
#                / ____/ /_/ / /_/ /__/ / /_/ / /_/ /   |                      #
#               /_/    \__,_/_.___/____/\__,_/_.___/_/|_|                      #
#                                                                              #
#******************************************************************************#
# File   : server.py
# Product: PubSubx
# Brief  : Server implementation of publish subscribe protocol in PubSubX
# Ingroup: PubSubx
# Version: 0.1
# Updated: February 9 2022
#
# Copyright (C) Goran Josipovic. All rights reserved.
#******************************************************************************/

# Description:
# This module is first of 2 modules in PubSubX. It can be included or used as
# standalone server module. It consists of 2 classes, client and server.  
# Client class is helper class for server's client "accounting". Server class uses 
# efficient Linux I/O event notification facility Epoll. Since this is Linux-only 
# facility this module cannot be used on other platforms. Epool is used to implement
# efficent non blocking control of multiple connections between clients and server.



#******************************************************************************#
#************************          INCLUDES          **************************#
#******************************************************************************#
from socket import *
import sys, time, select, signal


#******************************************************************************#
#*********************          PRINT FUNCTIONS          **********************#
#******************************************************************************#
# Basic implementation of functions used to control printing of errors 
# and informations, 
PRINT_ERRORS = True    # Enable printing of errors
PRINT_INFO   = True    # Enable printing of info messages

# List of error messages	
errors = {}
errors["WRONG_PORT"]    = "Server port number is wrong or not enterd, must be integer in range 1024 < port < 32000"
errors["INIT_FAIL"]     = "Initialization of server has failed"

# List of information messages
infos = {}
infos["SER_INI"]  = "Server sucessfully started"
infos["CON_REG"]  = "New connection registerd with server: "
infos["CON_REM"]  = "Connection removed from server: "

infos["CLI_REM"]  = "Client removed from system: "
infos["CLI_RES"]  = "Lost client restored: "
infos["CLI_ADD"]  = "New client added to the server: "
infos["CLI_LST"]  = "Client lost: "

infos["MSG_PUB"]  = "Message published to a topic (topic data): " 
infos["MSG_ADD"]  = "Message added to a client stream (client messge): "

infos["CLI_SUB"]  = "Client subscribed to a topic (client topic): "
infos["CLI_UNS"]  = "Client unsubscribed from a topic (client topic): "

def print_error(error, msg = ""):
    if PRINT_ERRORS:
        assert(error in errors)
        print("ERROR: " + errors[error] + msg)

def print_info(info, msg = ""):
    if PRINT_INFO:
        assert(info in infos)
        print("INFO: " + infos[info] + msg)


#******************************************************************************#
#**********************          CLIENT CLASS          ************************#
#******************************************************************************#						
class Client:

    # Maximum size of message stream, 
    MAX_STREAM_SIZE = 10 * 1024

    def __init__(self, name : str, fileno : int):
        
        self.name           = name      # Name of the client
        self.fileno         = fileno    # Fileno of socket for client
        self.connected      = True      # Conencted flag
        self.lost_time      = 0         # Time when lost
        self.message_stream = b''       # Message stream of the client
        self.topics         = set()     # Set of topics client is subscribed to
     
    # Subscribe client to a topic
    def subscribe(self, topic) -> bool:

        if topic not in self.topics:
            self.topics.add(topic)
            return True
        else:
            return False

    # Unsubscribe client from a topic
    def unsubscribe(self, topic) -> bool:

        if topic in self.topics:
            self.topics.remove(topic)
            return True
        else:
            return False

    # Add a message to client message stream
    def add_message(self, message : str)->int:

        # If stream too large stop receiving, better solution would be to dump oldest messages, (TODO)
        if len(self.message_stream) +  len(message) < self.MAX_STREAM_SIZE:
            self.message_stream += message

        return len(self.message_stream)

    # Return message chunk and remainig stream size 
    def get_message_chunk(self, max_chunk_size):

        # Chunk is either entire message_stream or max_chunk_size
        stream_size = self.message_stream_size()        
        if stream_size > max_chunk_size:
            remove = max_chunk_size
        else:
            remove = stream_size

        msg_chunk = self.message_stream[0 : remove]
        self.message_stream = self.message_stream[remove : ]

        remaining = stream_size - remove

        return (msg_chunk, remaining)
    
    def message_stream_size(self)->int:
        return len(self.message_stream)


#******************************************************************************#
#**********************          SERVER CLASS          ************************#
#******************************************************************************#								
class Server:

    #******************************************************************************#
    #***********************          DEFINITIONS          ************************#
    #******************************************************************************#	
    # List of all possible commands client ca send to server
    commands = ["CONNECT", "DISCONNECT", "PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE"]

    EOM              = b'\n\nx'          # End of message delimiter string
    BUFFER_SIZE      = 1024              # Single buffer size to send over connection at once
    MAX_REQUEST_SIZE = 10 * BUFFER_SIZE  # Maximum size of request without EOM
    LOST_TIMEOUT     = 60                # Time befor lost client is deleted


    #******************************************************************************#
    #***********************          SERVER INIT          ************************#
    #******************************************************************************#								
    def __init__(self, port : int):

						
        #********************          SERVER ATTRIBUTES          *********************#
        # Listening socket and epoll 
        self.__port         = port    # Port number of the Server
        self.__socket       = socket(AF_INET, SOCK_STREAM) # Main, listening server socket
        self.__epoll        = select.epoll() # Epool structre for efficient monitorinig og large number of connections
            
        # Connections and requests dictionaries, fileno is key
        self.__connections  = {}      # Dictionary of all active conections, pending or with active client
        self.__pending      = {}      # Dictionary of pending connections, active but not yet sent CONNECTED message
        self.__requests     = {}      # Dicitonray of all raw requests received from sockets
     
        # Clients dictionaries
        self.__connected       = {}   # Dictionary of connected clients      by fileno key
        self.__connected_names = {}   # Dictionary of connected client       by name key
        self.__temp_lost       = {}   # Dictionary of temporary lost clients by name key 
        
        # Topics, topic name is the key
        self.__topics       = {}   # Dictionary of subscribed clients (sets) by topic
        
        # Last time lost list refreshed
        self.__last_refresh = time.time()

        #******************          SOCKET AND EPOLL INIT          *********************#
        # Main socket initialization
        self.__socket.bind(('0.0.0.0', port))
        self.__socket.listen(10)
        self.__socket.setblocking(0)
        self.__socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)        
        self.__socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        self.__socket.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        
        # Epoll initialization, set main socket in listening pool
        self.__epoll.register(self.__socket.fileno(), select.EPOLLIN)

        print_info("SER_INI")

    #******************************************************************************#
    #***************          SERVER CONNECTION FUNCTIONS          ****************#
    #******************************************************************************#
    # Functions for control of connections (sockets) between clients and server

    #*******************          CONNECTION REGISTER          ********************#								
    def __connection_register(self, fileno):

        # Establish connection and set its options
        connection, address = self.__socket.accept()
        connection.setblocking(0)
        connection.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)        
        connection.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        connection.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)

        # Register connection to dictionaries
        fileno = connection.fileno()    

        self.__epoll.register(fileno, select.EPOLLIN)
        self.__connections[fileno] = connection
        self.__pending[fileno]     = connection
        self.__requests[fileno]    = b''

        print_info("CON_REG", str(fileno))

    #********************          CONNECTION REMOVE          *********************#								
    def __connection_remove(self, fileno):

        # Unregister and shutdown connection
        self.__epoll.unregister(fileno)
        self.__connections[fileno].shutdown(SHUT_RDWR)
        self.__connections[fileno].close()
        
        # Remove connection from dictionaries
        del(self.__connections[fileno])
        del(self.__requests[fileno])
        if fileno in self.__pending:
            del(self.__pending[fileno])

        print_info("CON_REM", str(fileno))


    #******************************************************************************#
    #*****************          SERVER CLIENT FUNCTIONS          ******************#			
    #******************************************************************************#
    # Functions to control lifecycle of clients that are connected (or temp lost) 								
    
    #**********************          CLIENT REMOVE          ***********************#								
    def __client_remove(self, client : Client):

        # Remove from dictionaries
        if client.connected:
            del self.__connected[client.fileno]
            del self.__connected_names[client.name]
        else:
            del self.__temp_lost[client.name]
        
        # Remove from all subscriptions
        for topic in client.topics:                        
            self.__topics[topic].remove(client)

            # If set is empty (no subscribers) remove it from dictionary
            if self.__topics[topic] == set(): 
                del(self.__topics[topic])

        # Remove corresponding connection
        if client.connected:
            self.__connection_remove(client.fileno)

        print_info("CLI_REM", client.name)
        del(client)
       

    #*********************          CLIENT RESTORE          **********************#			
    def __client_restore(self, fileno : int, client_name : str):

        client = self.__temp_lost[client_name]

        # Send return message, with info on past subscriptions
        msg1 = "RESTORED " + client_name
        msg1 = msg1.encode() + self.EOM
        msg2 = " ".join(client.topics)
        msg2 = msg2.encode() + self.EOM  
        ret_msg = msg1 + msg2
        self.__connections[fileno].send(ret_msg)

        # Update dictionaries
        del(self.__pending[fileno])
        del(self.__temp_lost[client_name])
        self.__connected[fileno] = client
        self.__connected_names[client_name] = client
        

        # Restore client data, set new fileno and status flag
        client.fileno    = fileno
        client.connected = True

        # Check if client has any messages in stream and add it to epoll
        if client.message_stream_size():
            self.__epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT) 

        print_info("CLI_RES", client.name)


    #***********************          CLIENT ADD          ************************#
    def __client_add(self, fileno : int, client_name : str):
        
        # Create new client object
        client = Client(client_name, fileno)

        # Send return message
        ret_msg = b'OK: Conn accepted' + self.EOM
        self.__connections[fileno].send(ret_msg)
        
        # Update dictionaries
        self.__connected[fileno]   = client
        self.__connected_names[client_name] = client
        del(self.__pending[fileno])

        print_info("CLI_ADD", client.name)


    #***********************          CLIENT LOST          ************************#								
    def __client_lost(self, fileno : int):
   
        client = self.__connected[fileno]

        # Update dictionaries
        del(self.__connected[fileno])
        del(self.__connected_names[client.name])
        self.__temp_lost[client.name] = client

        # Restore client data, set new fileno and status flag
        client.fileno    = 0
        client.connected = False
        client.lost_time = time.time()

        # Remove dead connection
        self.__connection_remove(fileno)

        print_info("CLI_LST", client.name)


    #*******************          CLIENTS REMOVE DEAD          ********************#
    def __clients_remove_dead(self, now):

        # To avoid reshaping self.temp_lost first save results in remove list
        remove_list = set()
        for client_name in self.__temp_lost:
            client = self.__temp_lost[client_name]

            # If lost for too long client is dead
            if (now - client.lost_time) > self.LOST_TIMEOUT:
                remove_list.add(client)

        # Remove dead clients
        for client in remove_list: 
            self.__client_remove(client)


    #******************************************************************************#
    #*****************          SERVER PROCESS COMMANDS          ******************#						
    #******************************************************************************#
    # Functions for processing commands from clients

    #*********************          PROCESS COMMAND          **********************#								
    def __process_command(self, fileno : int, command : str, arg1 : str, data : str):

        if fileno in self.__pending:
            if command == 'CONNECT' and arg1 != "":
                self.__process_connect(fileno, arg1)            
            else:
                self.__connection_remove(fileno)

        elif command == 'DISCONNECT':
            self.__process_disconect(fileno)  
            
        elif command == 'PUBLISH' and arg1 != "":
            self.__process_publish(arg1, data)  

        elif command == 'SUBSCRIBE' and arg1 != "":
            self.__process_subscribe(fileno, arg1)  

        elif command == 'UNSUBSCRIBE' and arg1 != "":
            self.__process_unsubscribe(fileno, arg1) 


    #*********************          PROCESS CONNECT          **********************#	
    def __process_connect(self, fileno : int, client_name : str):

        assert(client_name != "")

        # Client name taken, send return message and kill connection
        if client_name in self.__connected_names:
            ret_msg = b'ERROR: Name already taken' + self.EOM
            self.__connections[fileno].send(ret_msg)
            self.__connection_remove(fileno)
        
        # Client in temp lost, restore
        elif client_name in self.__temp_lost:
            self.__client_restore(fileno, client_name)

        # Add new client 
        else:
            self.__client_add(fileno, client_name)


    #*******************          PROCESS DISCONNECT          ********************#
    def __process_disconect(self, fileno : int):

        client = self.__connected[fileno]

        # Remove the client and corresponding connection
        self.__client_remove(client)


    #*********************          PROCESS PUBLISH          **********************#
    def __process_publish(self, topic : str, data : str):

        assert(topic != "")

        print_info("MSG_PUB", topic +  " " + data)
        
        # Is anybody subscribed to this topic
        if topic in self.__topics:
            # Create message 
            message = topic + " " + data
            message = message.encode() + self.EOM

            # And queue it to clients
            for client in self.__topics[topic]:

                print_info("MSG_ADD", client.name + " " +  topic +  " " + data)  
                              
                # If this is first message in queue add client to output pool (if connected)                                
                if client.add_message(message) == len(message):
                    fileno = client.fileno
                    if fileno in self.__connected:                    
                        self.__epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT)   
    

    #********************          PROCESS SUBSCRIBE          *********************#
    def __process_subscribe(self, fileno : int, topic : str):
        
        assert(topic != "")

        client = self.__connected[fileno]        

        # Check if client is already subscribed
        if client.subscribe(topic):

            # If this is first subscriber create a topic
            if topic not in self.__topics:
                self.__topics[topic] = set()
                self.__topics[topic].add(client)

            # Otherwise append client to topic
            else:
                self.__topics[topic].add(client)

        print_info("CLI_SUB", client.name + " " + topic)


    #*******************          PROCESS UNSUBSCRIBE          ********************#								
    def __process_unsubscribe(self, fileno: int, topic : str):

        assert(topic != "" )

        client = self.__connected[fileno]        

        # Check if client was subscribed
        if client.unsubscribe(topic):

            self.__topics[topic].remove(client)

            # If this was the last subscriber remove the topic                
            if self.__topics[topic] == set():
                del(self.__topics[topic])

        print_info("CLI_UNS", client.name + " " + topic)


    #******************************************************************************#
    #******************          SERVER I/O FUNCTIONS          ********************#				
    #******************************************************************************#
    # Functions to read, write and process data from/to streams 

    #******************          RECEIVE REQUEST CHUNK          *******************#
    def __receive_request_chunk(self, fileno : int):

        request_chunk = self.__connections[fileno].recv(self.BUFFER_SIZE)
        
        # If request chunk is empty connection is dead
        if request_chunk == b'':
            # If it was from connected client set him as lost
            if fileno in self.__connected:
                self.__client_lost(fileno)
            # Otherwise pending connection died before it sent any message
            else:
                assert(fileno in self.__pending)                
                self.__connection_remove(fileno)
            return

        # Add chunk to previous part
        self.__requests[fileno] += request_chunk

        # Dump request that is too long
        if len(self.__requests[fileno]) > self.MAX_REQUEST_SIZE: 
            self.__requests[fileno] = b''
            return
        
        # If chunk holds last part of single or multiple messages
        if self.EOM in self.__requests[fileno]:                

            bin_msg_list = self.__requests[fileno].split(self.EOM)

            # If request ends with EOM reset request buffer
            last = len(bin_msg_list) - 1
            if self.__requests[fileno].endswith(self.EOM):
                self.__requests[fileno] = b''                
                bin_msg_list.pop(last)
            # If not bin_msg_list holds parts of future message
            else:
                last = len(bin_msg_list) - 1
                self.__requests[fileno] = bin_msg_list.pop(last)
            
            # Send messages to processing
            for bin_msg in bin_msg_list:
                self.__process_message(fileno, bin_msg)


    #*********************          PROCESS MESSAGE          **********************#
    def __process_message(self, fileno : int, bin_msg : str):

        # Skip empty and non decodable messages
        if   bin_msg == b'': return
        try: msg = bin_msg.decode()
        except:  return
        
        # Parse command, arguments and data from the message
        msg_list = msg.split(' ')
        command  = msg_list[0]

        try:    arg1 = msg_list[1]
        except: arg1 = ''

        try:    data = " ".join(msg_list[2: ])
        except: data = ""

        # Send command to processing
        self.__process_command(fileno, command, arg1, data)


    #*******************          SEND MESSAGE CHUNK          ********************#
    def __send_message_chunk(self, fileno):

        # Client that is ready to receive message
        client = self.__connected[fileno]
        
        # Get single message chunk from client 
        (msg_chunk, remaining) = client.get_message_chunk(self.BUFFER_SIZE) 
        assert(len(msg_chunk) <= self.BUFFER_SIZE)

        # Send message chunk 
        self.__connections[fileno].send(msg_chunk)   

        # If message_stream of client is empty remove him from epoll
        if remaining == 0:    
            self.__epoll.modify(fileno, select.EPOLLIN)   


	#******************************************************************************#
    #********************          SERVER MAIN LOOP          **********************#								
    #******************************************************************************#
    def server_loop(self):

        while True:

            # Wait for ready sockets, pass throught after 1 sec
            events = self.__epoll.poll(1)

            for fileno, event in events:

                # New connection ready for establishment 
                if fileno == self.__socket.fileno():
                    self.__connection_register(fileno)

                # New request chunk ready to be received 
                elif event & select.EPOLLIN:
                    self.__receive_request_chunk(fileno)

                # Some client ready to receive message chunk
                elif event & select.EPOLLOUT:
                    self.__send_message_chunk(fileno)

            # Refresh temp lost list if any client ever second
            if (self.__temp_lost) and (self.__last_refresh - time.time()):
                now = time.time()
                self.__last_refresh = now
                self.__clients_remove_dead(now)



#******************************************************************************#
#***********************          USAGE MODE          *************************#
#******************************************************************************#
def signal_handler(sig, frame):
    print('\Server killed by Ctrl+C!')
    sys.exit(0) 
								
if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)

    try:    
        port = int(sys.argv[1])
        if not (1024 < port < 32000): raise Exception
            
    except: 
        print_error("WRONG_PORT"); 
        exit()

    server = Server(port)
    server.server_loop()
        

    
    



   










