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
            command = raw_input("what to do: 1. buy xxx; 2. show; 3. change config ")
            if command.startswith('buy'):
                num = command.split(' ')[1]
                if not num.isdigit():
                    print 'wrong number of ticket'
                    continue
                print s.buy(num)

            elif command.startswith('show'):
                #TODO
                TODO = 1
            elif command.startswith('change'):
                #TODO
                TODO = 1
            else:
                print 'wrong command'
    except Exception as e:
        print 'sth is wrong, Exception: ' + str(e)


