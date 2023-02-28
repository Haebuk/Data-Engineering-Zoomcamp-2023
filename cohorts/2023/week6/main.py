from confluent_kafka import Producer

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != '#':
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
                
    return conf

if __name__ == "__main__":
    producer = Producer(read_ccloud_config("client.properties"))
    