"""
start like this: python client.py port

buy:

show:
    (1)In the first line, show the state of the state machine for the application
    (2) In the following lines, show the committed logs in the datacenter the client connected to.

change:


"""

import xmlrpclib

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
            command = raw_input("what to do: 1. buy xxx; 2. show; 3. add xxx\n:")
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

            elif command.startswith('add'):
                configs = []
                num = int(command.split(' ')[1])
                for i in range(num):
                    command = raw_input("Input: id, ip, port, split by space\n:")
                    configs.append( command.split() )
                print configs
                s.add_server_rpc(configs)
            else:
                print 'wrong command'
    except Exception as e:
        print 'STH is wrong, Exception: ' + str(e)


