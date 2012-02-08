#!/usr/bin/env python
import sys
import os
import time
import json
import libvirt
import mysql
from subprocess import call

class TwinLogger(object):
    """ Logging Interface. TODO: Add times. """
    
    def __init__(self, log_file, console_mode=False):
        self.console_mode = console_mode
        self.pointer = open(log_file, 'a')
        if not self.console_mode:
            self.pointer.write('- Log Opened')

    def log(self, mesg):
        if self.console_mode:
            print '- ' + mesg
        else:
            pointer.write('LOG: ' + mesg) 

    def error(self, mesg):
        if self.console_mode:
            print '- ABORT: ' + mesg
        else:
            pointer.write('ERROR: ' + mesg)
            self.close()
            sys.exit(1)

    def close(self):
        if not self.console_mode:
            pointer.write('- Log Closed')

class PyTwin(object):
    """ Interface for PyTwin commands """

    def __init__(self, console_mode):
        try:
            self.config = json.loads(open('/etc/twin/config.json', 'r').read())
        except:
            print 'FATAL: Failed to load config.'
            sys.exit(1)

        self.log = TwinLogger(self.config['log_file'], True)

        self.log.log('Opening hypervisor connection.')
        self.conn = libvirt.open(self.config['hypervisor_path']);
        if self.conn == None:
            self.log.error('Failed to open hypervisor connection.')

        # Set up the MySQL connection
        self.sql = _mysql.connect(host=self.config.db.host,
                user=self.config.db.user, passwd=self.config.db.password);
            
    ### Read VM information ###

    def get_domain(self, name):
        try:
            return self.conn.lookupByName(name)  
        except:
            return False

    def is_running(self, domain):
        return domain.info()[0] == libvirt.VIR_DOMAIN_SHUTOFF

    ### Manage VM state ###

    def start(self, domain):
        self.log.log('Starting ' + domain.name())
        if domain.create() < 0:
            domain.free()
            self.log.error('Failed to start ' + domain.name())
        else:
            self.log.log(domain.name() + ' started.')

    def stop(self, domain):
        self.log.log('Stopping ' + domain.name())
        domain.shutdown()

        # Wait for the VM to shut down nicely
        count = 0
        while self.is_running(domain) and count < self.config['wait_time']:
            count += 1 
            time.sleep(1)
        
        if self.is_running(domain):
            self.log.log(domain.name() + ' didn\'t shut down nicely. Forcing.')
            domain.destroy()

        if self.is_running(domain):
            self.log.error(domain.name() + ' still running after destruction.')
        else:
            domain.free()
            self.log.log(domain.name() + ' stopped.')

    def lock_database(self):
        self.log.log('Locking MySQL database.')
        try:
            self.sql.query('FLUSH TABLES WITH READ LOCK')
        except:
            self.log.log('Failed to lock MySQL databases.')

    def unlock_database(self):
        self.log.log('Unlocking MySQL database.')
        try:
            self.sql.query('UNLOCK TABLES')
        except:
            self.log.log('Failed to unlock MySQL tables.')

    ### Twin functionality ###

    def send(self, domain):
        self.log.log('Sending ' + domain.name())
        self.lock_database();

        # TODO: Create snapshot
        
        self.unlock_database()

        # Send snapshot
        # TODO: Replace with in-built Python utils for this.

        connect_string = 'ssh -p ' + self.config.remote_port + ' -l ' + 
            self.config.remote_username + ' -i ' + self.config.remote_key +
            ' ' + self.config.remote_ip

        # Remove the old completion indicator
        call([connect_string,
            '"rm --force ' + self.config.remote_dir + '/' + domain.name() +
            '/incoming/complete.txt"'])

        # Send the VM snapshot
        call(['rsync',
            '--inplace',
            '--ignore-times',
            '--bwlimit=' + self.config.bandwidth_limit,
            '--verbose',
            '--stats',
            '--human-readable',
            '--rsh "' + connect_string + '" ',
            self.config.mount_point + '/' + domain.name() + '-snapshot/' +
                self.config.disk_name,
            self.config.remote_ip + ':' + self.config.remote_dir + '/' +
                domain.name() + '/incoming/'
            ])

        # Create the completion indicator
        call([connect_string,
            '"touch ' + self.config.remote_dir + '/' + domain.name() +
                '/incoming/complete.txt"'
            ])

        self.log.log('Finished sending ' + domain.name());

    def receive(self, domain):
        complete_flag = os.path.join(self.config['work_dir'], domain.name(), 
            'incoming', 'complete.txt')
        if os.path.exists(complete_flag)
            os.remove(complete_flag)

            # Start transient test VM
            test_conf = open('/etc/twin/' + domain.name + '.xml', 'r').read()
            test_domain = libvirt.domain.createXML(test_conf)

            if test_domain < 0:
                self.log.error('Failed to start test VM')

            # Wait for the test VM to report back
            start_flag = os.path.join(self.config['work_dir'], domain.name(), 
                'incoming', 'startup.txt'
            count = 0
            while not os.path.exists(start_flag) and count < self.config['wait_time']:
                count += 1
                time.sleep(1)

            # Stop the test VM
            self.stop(test_domain)

            if os.path.exists(start_path):
                os.remove(start_path)
                self.stop(domain)
                shutil.copy2(os.path.join(self.work_dir, 'incoming', 'hda.raw'), 
                    os.path.join(self.mount_point, domain.name())
                self.start(domain)
            else:
                self.log.error('Test VM didn\'t respond')


### Main Program ###

twin = PyTwin()

domain = twin.get_domain(sys.argv[1])
if not domain:
    print 'Failed to connect to domain: ' + sys.argv[1]
    sys.exit(1)

if sys.argv[2] == 'start':
    print 'Starting ' + sys.argv[1]
    twin.start(domain) 
else if sys.argv[2] == 'receive':
    print 'Receiving ' + sys.argv[1]
    twin.receive(domain)
else if sys.argv[2] == 'send':
    print 'Sending ' + sys.argv[1]
    twin.send(domain)
