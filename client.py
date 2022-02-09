#******************************************************************************#
#                   ____        __   _____       __   _  __                    #
#                  / __ \__  __/ /_ / ___/__  __/ /_ | |/ /                    #
#                 / /_/ / / / / __ \\__ \/ / / / __ \|   /                     #
#                / ____/ /_/ / /_/ /__/ / /_/ / /_/ /   |                      #
#               /_/    \__,_/_.___/____/\__,_/_.___/_/|_|                      #
#                                                                              #
#******************************************************************************#
# File   : client.py
# Product: PubSubx
# Brief  : Client implementation of publish subscribe protocol in PubSubX
# Ingroup: PubSubx
# Version: 0.1
# Updated: February 9 2022
#
# Copyright (C) Goran Josipovic. All rights reserved.
#******************************************************************************/

# Description:
# This module is second of 2 modules in PubSubX. It can be included or used as
# standalone server module. It consists of 1 class - client.  
# Client class uses portable select mechanism for nonblocking overview of input
# and output sockets for external and inter-thread communication. Client has
# 2 loop functions, one for command line interface, for issuing commands and 
# and other for socket monitoring and communication wiht server. These 2 loops 
# communicate via sockets, command loop sends commands and informations over to
# socket loop


#******************************************************************************#
#************************          INCLUDES          **************************#
#******************************************************************************#
from socket import *          # Portable socket interface plus constants
from select import select     # Portable select interface plus constants
import _thread as thread      # Portable threa interface plus constants
import sys, signal


#******************************************************************************#
#*********************          PRINT FUNCTIONS          **********************#
#******************************************************************************#
# Basic implementation of functions used to control printing of errors 
# and informations, 
PRINT_ERRORS = True    # Enable printing of errors
PRINT_INFO   = False   # Default disable printing of info messages

# List of error messages		
errors = {}
errors["INIT_FAIL"]     = "Initialization of local sockets has failed"
errors["WRONG_PORT"]    = "Server port number is wrong, must be integer in range 1024 < port < 32000"
errors["WRONG_NAME"]    = "Client name is empty/too long, must be between 1 and 64 characters"
errors["NAME_TAKEN"]    = "Client name is alreaday taken, please enter other name"
errors["CONN_FAIL"]     = "Connection to the server has failed, please check port and try again"
errors["MSG_TOO_LONG"]  = "Received/(trying to send) message that is too long"
errors["CONN_LOST"]     = "Client lost connection to the server try to reconnect "
errors["CONN_DOWN"]     = "Server shut the connection, all subscriptions are lost"
errors["NOT_CONN"]      = "Client is not connected, only CONNECT command is accepted "
errors["WRONG_TOPIC"]   = "Client received message on a topic he is not subscribed to "
errors["EMPTY_TOPIC"]   = "Trying to publish/subscribe/unsubscribe to an empty topic"
errors["WRONG_CMD"]     = "Wrong command is entered, to see help enter -h"
errors["NO_RSP"]        = "No response from server: "
errors["UNKNOWN_RSP"]   = "Unknown response from server: "
errors["EXCEPTION"]     = "Exception occured: "

# List of information messages
infos = {}
infos["CONN_ACC"]       = "Connection sucessfully established"
infos["ALR_CONN"]       = "Already connected to server, first disconnect"
infos["ALR_SUB"]        = "Already subscribed to topic: "
infos["NOT_SUB"]        = "Was not subscribed to topic: " 
infos["CONN_RESTORED"]  = "Connection restored "

def print_help():    
    print("client.py - list of possible client commands: ")
    print('CONNECT <port> <client_name>    : connect to PubSubX server at specified port with client name')
    print('DISCONNECT                      : disconect from to PubSubX server, all subscriptions will be removed')
    print('PUBLISH <topic_name> <message>  : publish message to topic on PubSubX server')
    print('SUBSCRIBE <topic>               : subscribe client to a topic on a PubSubX server')
    print('UNSUBSCRIBE <topic_name>        : remove subscription from a topic on PubSubX server')

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


    #******************************************************************************#
    #*******************          CLIENT DEFINITIONS          *********************#
    #******************************************************************************#	
    MAX_NAME_LEN     = 64                # Maximum length of client name
    BUFFER_SIZE      = 1024              # Size of the single receive / send buffer
    MAX_MESSAGE_SIZE = 10 * BUFFER_SIZE  # Maximal size of send/receive message
    EOM              = b'\n\nx'          # End of message string

    # List of all possible commands from user
    COMMANDS    = ['-H', 'CONNECT', 'DISCONNECT', 'PUBLISH', 'SUBSCRIBE', 'UNSUBSCRIBE']


    #******************************************************************************#
    #***********************          CLIENT INIT          ************************#
    #******************************************************************************#							
    def __init__(self, server_host : str):
        
        #********************          CLIENT ATTRIBUTES          *********************#
        # Server data
        self.__server_host = server_host  # Server host to connect to
        self.__port        = 0            # Port number to connect to
        self.__connected   = False        # Connection flag
        self.__ext_socket  = socket(AF_INET, SOCK_STREAM)   # External socket to connect to server
        
        # Inter thread communication data
        # Socket for inter-thread communication, between the main loop and socket loop
        # used instead of fifo for portability on windows platforms
        self.__msg_in_sock     = socket(AF_INET, SOCK_STREAM)      # Socket for messages from the input side
        self.__close_in_sock   = socket(AF_INET, SOCK_STREAM)      # Socket for close signalisation from the input side

        self.__msg_out_sock    = socket(AF_INET, SOCK_STREAM)      # Socket to receive messages, socket thread side
        self.__close_out_sock  = socket(AF_INET, SOCK_STREAM)      # Socket for close signalisation, socket thread side

        self.__mutex           = thread.allocate_lock()            # Mutex to avoid race conditions between command loop and socket loop

        # Messages streams buffers
        self.__receive_stream    = b''  # Buffer for holding message stream from server
        self.__send_message_list = []   # List of messages to send to server  

        self.__subscribed_topics = []      # List of the topics client is subscribed to
                   


    #******************************************************************************#
    #**************          CLIENT CONNECTIONS FUNCTIONS          ****************#
    #******************************************************************************#
    # Functions to establish connection between local sockets and to server


    #******************          CONENCT LOCAL SOCKETS          *******************#														
    def __connect_local_sockets(self):

        # Socket used for connection establishement
        listen_socket = socket(AF_INET, SOCK_STREAM) 
        
        # Serch for empty port to bind 
        for listen_port in range(32000, 65535):
            try:
                listen_socket.bind(('', listen_port))
                listen_socket.listen(5)
                break
            except:
                pass

        if listen_port == 65535:
            print_error("INIT_FAIL")
            exit()

        try:
        
            self.__msg_out_sock.setblocking(0)
            self.__close_out_sock.setblocking(0)

            # Avoid [Errno 115] Operation now in progress for nonblockig sockets
            try:    self.__msg_out_sock.connect(("", listen_port))
            except: pass
            self.__msg_in_sock, addr  = listen_socket.accept()

            try:    self.__close_out_sock.connect(("", listen_port))
            except: pass
            self.__close_in_sock, addr = listen_socket.accept()

            listen_socket.close()

        except Exception as e:
            print_error("EXCEPTION", str(e))
            print_error("INIT_FAIL")
            exit()


    #****************          CONNECT SERVER ARG CHECK          *****************#							
    def __connect_server_arg_check(self, server_port, client_name):

        try:    
            server_port = int(server_port)
        except: 
            print_error("WRONG_PORT"); 
            return False

        if server_port <= 1024 or server_port > 65535:
            print_error("WRONG_PORT")
            return False

        if client_name == "" or len(client_name) > self.MAX_NAME_LEN:
            print_error("WRONG_NAME")
            return False

        return True


    #*****************          CONNECT SERVER ACCEPTED          ******************#								
    def __connect_server_accepted(self, port):

        # Update attributes
        self.__port = port
        self.__connected = True   

        # Start socket loop
        thread.start_new_thread(self.__socket_loop, ())  

        print_info("CONN_ACC")


    #*****************          CONNECT SERVER RESTORED          ******************#								
    def __connect_server_restored(self, rsp, port):   

        print_info("CONN_RESTORED")

        # Update attributes
        self.__port = port
        self.__connected = True   

        # Restore response has previous subscribed topics as first message
        msg_list = rsp.split(self.EOM)
        self.__subscribed_topics = msg_list[1].decode().split(" ")

        # And eventual packed topics messages with rest of response            
        msg_rest = self.EOM.join(msg_list[2:])       
        self.__process_message_chunk(msg_rest, True)

        # Start socket loop
        thread.start_new_thread(self.__socket_loop, ())  
       

    #*****************          CONNECT SERVER REFUSED          ******************#						
    def __connect_server_refused(self, rsp):

        if rsp == b"ERROR: Name already taken" + self.EOM:
            print_error("NAME_TAKEN")

        else: 
            print_error("UNKNOWN_RSP", rsp)

        # Close external socket gracefully
        self.__ext_socket.shutdown(SHUT_RDWR)
        self.__ext_socket.close()
   

    #*********************          CONNECT SERVER          **********************#								
    def __connect_server(self, server_port : str, client_name : str):
        
        if not self.__connect_server_arg_check(server_port, client_name):
            return

        try:
            # Initialize external socket
            self.__ext_socket = socket(AF_INET, SOCK_STREAM)

            # Try to connect to server host and send connection message          
            self.__ext_socket.connect((self.__server_host, int(server_port)))  
            self.__ext_socket.settimeout(10)
            message = "CONNECT " + client_name   
            self.__ext_socket.send(message.encode() + self.EOM) 

            # Read server reply message        
            rsp = self.__ext_socket.recv(self.BUFFER_SIZE)        
                    
            # Connection accepted
            if rsp == b"OK: Conn accepted" + self.EOM:
                self.__connect_server_accepted(server_port)

            # Connection restored
            elif b"RESTORED" in rsp:                
                self.__connect_server_restored(rsp, server_port)

            # Connection refused
            else:
                self.__connect_server_refused(rsp, server_port)

        except ConnectionRefusedError:
            print_error("EXCEPTION", str(ConnectionRefusedError))
            self.__ext_socket.close()
            print_error("CONN_FAIL")
            return

        except Exception as e:
            self.__ext_socket.shutdown(SHUT_RDWR)
            self.__ext_socket.close()
            print_error("EXCEPTION", str(e))
            print_error("CONN_FAIL")
            return


    #******************************************************************************#
    #*****************          CLIENT SOCKET FUNCTIONS          ******************#
    #******************************************************************************#
    # Functions for socket communication between threads and server

    #***********************          SOCKET READ          ************************#								
    def __socket_read(self, socket, outputs):
            
        # Disconnect command sent by the user          
        if socket == self.__close_out_sock:
            message = self.__close_out_sock.recv(self.MAX_MESSAGE_SIZE)
            self.__ext_socket.send(message + self.EOM)
            self.__ext_socket.shutdown(SHUT_RDWR)
            self.__ext_socket.close()
            self.__connected = False     

        # Other command sent by the user
        elif  socket == self.__msg_out_sock:                        
            if outputs == []:
                outputs.append(self.__ext_socket)
            message = self.__msg_out_sock.recv(self.MAX_MESSAGE_SIZE)
            self.__send_message_list.append(message)    

        # Message chunk received from server
        elif socket == self.__ext_socket:
            msg_chunk = self.__ext_socket.recv(self.BUFFER_SIZE) 
            # If chunk is empty connection is down
            if msg_chunk == b'': 
                print_error("CONN_DOWN")
                self.__ext_socket.close()
                self.__connected = False     
            else: 
                self.__process_message_chunk(msg_chunk, False)

        return (outputs, self.__connected)

    #**********************          SOCKET WRITE          ***********************#								
    def __socket_write(self, outputs):
        msg_chunk, last = self.__get_send_chunk()
        self.__ext_socket.send(msg_chunk)
        if last: 
            outputs = []
        return outputs

    #***********************          SOCKET LOOP          ************************#						
    def __socket_loop(self):

        self.__receive_stream = b''
        self.__send_message_list = []

        inputs  =  [self.__ext_socket, self.__msg_out_sock, self.__close_out_sock]
        outputs  = []
        errors   = inputs
    
        while True:

            read, write, error = select(inputs, outputs, errors)
            
            with self.__mutex:

                if read:
                    for socket in read:
                        (outputs, conn) = self.__socket_read(socket, outputs)
                        if not conn: return                  

                elif write:
                    outputs = self.__socket_write(outputs)

                elif error:
                    print_error("CONN_LOST")
                    self.__ext_socket.close()
                    self.__connected = False
                    return

    #******************************************************************************#
    #**************          CLIENT I/O MESSAGES FUNCTIONS          ***************#
    #******************************************************************************#	
    # Functions to get/process message chunks to send to or that are received from 
    # server
    
    #******************          PROCESS MESSAGE CHUNK          *******************#
    def __process_message_chunk(self, msg_chunk : str, from_restore):
   
        self.__receive_stream += msg_chunk

        if len(self.__receive_stream) > self.MAX_MESSAGE_SIZE:
            print_error("MSG_TOO_LONG")
            self.__receive_stream = b''

        # If stream has EOM than at leas one is in the stream 
        if self.EOM in self.__receive_stream:

            bin_msg_list = self.__receive_stream.split(self.EOM)
            
            last = len(bin_msg_list) - 1
            # If request ends with EOM 
            if self.__receive_stream.endswith(self.EOM):
                self.__receive_stream = b''
                bin_msg_list.pop(last)
            # If not stream holds parts of future messages
            else:    
                self.__receive_stream = bin_msg_list.pop(last)

            # Print new line
            if not from_restore:
                print("")

            for bin_msg in bin_msg_list:
                # Skip empty and non decodable messages
                if   bin_msg == b'': continue
                try: msg = bin_msg.decode()
                except:  continue
                self.__print_received_message(msg)

            if not from_restore:
                # Print prompt without new line   
                sys.stdout.write("Enter command or (-h): ")
                sys.stdout.flush()

    #*********************          GET SEND CHUNK          **********************#								    
    def __get_send_chunk(self):

        if len(self.__send_message_list[0]) < self.BUFFER_SIZE - len(self.EOM):
            msg_chunk = self.__send_message_list[0] + self.EOM
            self.__send_message_list.pop(0)
            last = True if not self.__send_message_list else False
            
        else:
            msg_chunk = self.__send_message_list[0][0 : self.BUFFER_SIZE]
            self.__send_message_list[0] = self.__send_message_list[0][self.BUFFER_SIZE :]
            last = False

        return (msg_chunk, last)  

    #*****************          PRINT RECEIVED MESSAGE          ******************#								
    def __print_received_message(self, msg : str):

        topic = msg.split(' ')[0]
        data  = msg.replace(topic, "", 1)
        data  = data.lstrip() 

        if topic in self.__subscribed_topics:
            print("Topic: " + topic + "  Data: " + data)
        else:
            print_error("WRONG_TOPIC")

    #******************************************************************************#
    #****************          CLIENT COMMAND FUNCTIONS          ******************#
    #******************************************************************************#
    # Functions to parse and process command from command input		

    #**********************          COMMAND PARSE          ***********************#								
    def __command_parse(self, input):

        input_list = input.split(' ')

        command      = input_list[0].upper()

        try:    arg1 = input_list[1] 
        except: arg1 = ''
        
        try:    arg2 = input_list[2]
        except: arg2 = ''

        try:    data = " ".join(input_list[2: ])
        except: data = ""

        return (command, arg1, arg2, data)


    #*********************          COMMAND PROCESS          **********************#								
    def __command_process(self, command, topic, data):

        # Check that it is a valid command and not one that should already be processed
        assert(command in self.COMMANDS and command != '-h' and command != 'CONNECT')
        
        if  command == 'DISCONNECT':
            self.__command_disconnect()

        elif command == 'PUBLISH':
            self.__command_publish(topic, data)

        elif command == 'SUBSCRIBE':
            self.__command_subscribe(topic)

        elif command == 'UNSUBSCRIBE':
            self.__command_unsubscribe(topic)

    #*******************          COMMAND DISCONNECT          ********************#						
    def __command_disconnect(self):
        # Send message to socket loop to close the connections and exit
        message = 'DISCONNECT'
        self.__subscribed_topics = []
        self.__close_in_sock.send(message.encode())

    def __command_publish(self, topic, data):
        if topic == "": 
            print_error("EMPTY_TOPIC"); 
            return

        message = "PUBLISH" + " " + topic + " " + data      
        # Send message to socket loop 
        self.__msg_in_sock.send(message.encode())


    #********************          COMMAND SUBSCRIBE          *********************#				
    def __command_subscribe(self, topic):

        if topic == "": 
            print_error("EMPTY_TOPIC"); 
            return

        # If not already subscribed
        if topic not in self.__subscribed_topics:
            self.__subscribed_topics.append(topic)
            message = "SUBSCRIBE" + " " + topic
            # Send message to socket loop 
            self.__msg_in_sock.send(message.encode()) 
        # If already subscribed just ignore
        else:
            print_info("ALR_SUB", topic)   


    #*******************          COMMAND UNSUBSCRIBE          ********************#								
    def __command_unsubscribe(self, topic):

        if topic == "": 
            print_error("EMPTY_TOPIC"); 
            return

        # If subscribed to topic   
        if topic in self.__subscribed_topics:            
            self.__subscribed_topics.remove(topic)
            message = "UNSUBSCRIBE" + " " + topic
            # Send message to socket loop 
            self.__msg_in_sock.send(message.encode())

        # If not subscribed just ignore
        else:
            print_info("NOT_SUB", topic)  

    #**********************          COMMAND LOOP          ***********************#	
    def command_loop(self):

        # Initialize local connections to server loop								
        self.__connect_local_sockets()

        while True:

            input_str = input("Enter command or (-h): ")

            with self.__mutex:

                if input_str == '\n':
                    continue

                if len(input_str) > Client.MAX_MESSAGE_SIZE:
                    print_error("TOO_LONG")   
                    continue
            
                (command, arg1, arg2, data) = self.__command_parse(input_str)

                if command not in Client.COMMANDS:
                    print_error("WRONG_CMD")
                    
                elif command == '-H':
                    print_help()

                elif not self.__connected and command != 'CONNECT':
                    print_error("NOT_CONN")

                elif not self.__connected:
                    self.__connect_server(arg1, arg2)

                elif self.__connected and command == 'CONNECT':
                    print_info("ALR_CONN")

                else:
                    self.__command_process(command, arg1, data)


#******************************************************************************#
#***********************          USAGE MODE          *************************#
#******************************************************************************#
def signal_handler(sig, frame):
    print('\nClient killed by Ctrl+C!')
    sys.exit(0)
								
if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)

    PRINT_INFO = True

    client = Client('localhost')
    client.command_loop()
    





  
