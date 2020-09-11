import configparser
import os
import grpc

from exact_test_sampler_pb2_grpc import ExactTestSamplerStub

SELF_DIR = os.path.dirname(os.path.abspath(__file__))
TOPLEVEL = os.path.abspath(SELF_DIR + "/../") + "/"
CONFIG_PATH = TOPLEVEL + ".sampler_clients.ini"


def parse_sampler_servers(path=CONFIG_PATH):
    config = configparser.ConfigParser()
    ret = []
    try:
        config.read(path)
        for host in config:
            if host == "DEFAULT":
                continue
            hostname = host
            if "hostname" in config[host]:
                hostname = config[host]["hostname"]
            ret.append(host + ":" + config[host]["port"])
    except FileNotFoundError:
        pass
    return ret


def _print_sampler_servers():
    try:
        servers = parse_sampler_servers()
    except Exception as e:
        print("Exc: %s" % e)
        servers = []
    print("Found %i sampler servers in %s." % (len(servers), CONFIG_PATH))


_print_sampler_servers()


def get_sampler_servers():
    ret = []
    for connection_string in parse_sampler_servers():
        channel = grpc.insecure_channel(connection_string)
        ret.append(ExactTestSamplerStub(channel))
    return ret
