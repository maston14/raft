"""
start like this: python client.py port

buy:

show:
    (1)In the first line, show the state of the state machine for the application
    (2) In the following lines, show the committed logs in the datacenter the client connected to.

change:


"""

import xmlrpclib

def read_config_file(config_filename):
    addresses_tuple_list = []

    # get ticket number
    fin = open(config_filename)

    # skip ticket num, useless for client
    fin.readline()

    for aline in fin:
        split_line = aline.strip().split()

        node_id = int(split_line[0])
        ip = split_line[1]
        port = int(split_line[2])

        addresses_tuple_list.append((node_id, ip, port))

    return addresses_tuple_list

if __name__ == '__main__':
    import sys
    port_to_connect = sys.argv[1]
    if not port_to_connect.isdigit():
        print 'wrong port number'
        sys.exit(0)

    try:
        url = 'http://localhost:' + str(port_to_connect)
        s = xmlrpclib.ServerProxy(url)
        while True:
            command = raw_input("what to do: 1. buy xxx; 2. show; 3. change config\n:")
            if command.startswith('buy'):
                num = command.split(' ')[1]
                if not num.isdigit():
                    print 'wrong number of ticket'
                    continue
                print s.buy_ticket_rpc(num, port_to_connect)

            elif command.startswith('show'):
                ticket_left, log, committed_log =  s.show_rpc()
                print 'Reaching to Kiosk# ', port_to_connect, ', tickets left: ', ticket_left, '\n'
                print 'Local Log: ', log, '\n'
                print 'Commited Log:', committed_log, '\n'

            elif command.startswith('change'):
                config_filename = raw_input("Input the new config filename: ")
                addresses_tuple_list = read_config_file(config_filename)
                print addresses_tuple_list
                s.add_server_rpc(addresses_tuple_list)
            else:
                print 'wrong command'
    except Exception as e:
        print 'STH is wrong, Exception: ' + str(e)


