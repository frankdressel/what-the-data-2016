import hashlib
import time
import codecs
import paho.mqtt.client as mqtt

class KleinerStaubsauger:
    """
        This class wraps a single mqtt client. As soon as the client is connected, it registers for the given topic.
        All messages are written to file as is. There is no data processing done.

        The file path is hard coded to /vagrant/data.
    """

    def __init__(self, name, topic, host, port):
        """
           Keyword arguments:
           name -- The file name for the result file. This is just the file name without any paths.
           topic -- The topic for which the mqqt client should be registered. Can contain the mqtt wildcards.
           host -- The host of the mqtt message broker.
           port -- The port of the mqtt message broker.
        """
        self._name=name
        self._topic=topic

        self._client = mqtt.Client()
        self._client.on_message = self._on_message
        self._client.on_connect = self._on_connect
        self._client.connect(host, port, 60)

    def _on_message(self, client, userdata, msg):
        try:
            f=open('/vagrant/data/'+self._name+'.raw', 'a')
            f.write(str(msg.payload)+'\n')
            f.close()
        except:
            print('Error while writing to file: '+self._name)

    def _on_connect(self, client, userdata, flags, rc):
        print('Connected: '+str(rc))
        self._client.subscribe(self._topic)

    def start(self):
        """
            Starts the mqtt client in the background.
        """
        print('Start client: '+self._name)
        self._client.loop_start()

    def stop(self):
        """
            Stops the mqtt client.
        """
        self._client.loop_stop()
        self._client.disconnect()

class Staubsauger:
    """
        This class reads in a configuration file. The configuration contains all the necessary
        information to setup mqtt clients. Configuration changes are detected during runtime and the clients are
        updated (old ones are removed and new ones created if needed).

        One instance of this class is enough to control the whole data gathering process.

        The configuration file has to have the name staubsauger_config.txt and has to be in the working
        directory.
    """

    def __init__(self):
        self.__configfile_name='staubsauger_config.txt'
        self._staubis={}

    def _loadConfig(self):
        config_content=[]

        try:
            f=codecs.open(self.__configfile_name, 'r', 'UTF-8')
            config_content=f.readlines()
            f.close()
        except FileNotFoundError as e:
            print('Config file '+self.configfile_name+' not found')
        
        return [
            {
                'name': s[0],
                'topic': s[1],
                'host': s[2],
                'port': s[3],
                'hash_value': hashlib.md5(s[0].encode('utf-8')+s[1].encode('utf-8')+s[2].encode('utf-8')+s[3].encode('utf-8')).digest()
            }
            for s in [x.split() for x in config_content]
        ]

    def update_clients(self):
        """
            Updates (or initialises) the mqtt clients based on the configuration file.
        """
        client_defs=self._loadConfig()

        # Stop clients without config entry.
        client_def_hashes=[d['hash_value'] for d in client_defs]
        for hash_value, staubi in list(self._staubis.items()):
            if hash_value not in client_def_hashes:
                staubi.stop()
                del self._staubis[hash_value]
                print('Removed '+str(hash_value)+' from staubis')

        # Start non-existing clients.
        for client_def in client_defs:
            if client_def['hash_value'] not in self._staubis:
                try:
                    kleiner_staubsauger=KleinerStaubsauger(client_def['name'], client_def['topic'], client_def['host'], int(client_def['port']))
                    kleiner_staubsauger.start()
                    self._staubis[client_def['hash_value']]=kleiner_staubsauger
                    print('Added '+client_def['name']+' to staubis')
                except ValueError as e:
                    print('Invalid config: ', client_def['name'], client_def['topic'], client_def['host'], client_def['port'])
                except ConnectionRefusedError as e:
                    print('No connection possible: ', client_def['name'], client_def['topic'], client_def['host'], client_def['port'])

staubsauger=Staubsauger()
# Runs the clients update in a loop.
while True:
    staubsauger.update_clients()
    time.sleep(60)
