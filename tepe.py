#!/usr/bin/python3

from __future__ import annotations

import argparse
import os
import subprocess
import yaml
import shlex

from string import Template
from pylxd import Client
from yaml import ScalarNode
from typing import Union


class ContainerLoader:
    def __init__(self, definitions_dir: str, lxd: Client):
        self._definitions_dir = os.path.abspath(definitions_dir)
        self.container = {}
        self.lxd = lxd

    def list(self) -> list:
        result = []
        for file in os.listdir(self._definitions_dir):
            if not file.endswith('.yml') and not file.endswith('.yaml'):
                continue
            result.append(file.replace('.yaml', '').replace('.yml', ''))
        return result

    def path(self, container_id: str) -> str:
        path = os.path.join(self._definitions_dir, f'{container_id}.yaml')
        if not os.path.exists(path):
            path = os.path.join(self._definitions_dir, f'{container_id}.yml')
        return path

    def get(self, container_id: str) -> Container:
        if container_id in self.container:
            return self.container[container_id]
        path = self.path(container_id)
        with open(path, 'r+') as f:
            data = yaml.full_load(f)
        self.container[container_id] = Container(container_id, data['container'], loader=self, lxd=self.lxd)
        return self.container[container_id]

    def has(self, container: str) -> bool:
        return container in self.container or os.path.exists(self.path(container))


class Port:
    def __init__(self, port_data: dict, container: Container):
        self.container = container
        self.device = port_data['device']
        self.protocol = port_data['protocol']
        self.from_port = port_data['from']
        self.to_port = port_data['to']
        self.comment = port_data['comment'] if 'comment' in port_data else container.name

    def delete(self):
        check = subprocess.check_output(['sudo', '-S', 'iptables', '-L', '-n', '-t', 'nat', '--line-numbers'])\
            .split(b'\n')
        existing_rules = [line.split(b' ')[0] for line in check if f'dpt:{self.to_port}' in str(line)]
        existing_rules.reverse()
        for existing_rule in existing_rules:
            subprocess.call(['sudo', '-S', 'iptables', '-t', 'nat', '-D', 'PREROUTING', existing_rule])

    def create(self):
        ip = self.get_ip()
        subprocess.call([
            'sudo', '-S', 'iptables', '-t', 'nat', '-A', 'PREROUTING', '-p', self.protocol, '-i', 'enp1s0f0',
            '--dport', str(self.to_port), '-j', 'DNAT', '--to-destination', f'{ip}:{self.from_port}', '-m',
            'comment', '--comment', self.comment
        ])

    def get_ip(self) -> str:
        return self.container.get_ip(self.device)


class Container:
    def __init__(self, id: str, data: dict, loader: ContainerLoader, lxd: Client):
        self.id = id
        self.loader = loader
        self.lxd = lxd
        self.lxc = None
        self.ips = None
        self.name = data['name']
        self.description = data['description']
        self.box = data['box']
        self.mountpoints = data['mountpoints'] if 'mountpoints' in data else []
        self.ports = map(lambda port: Port(port, self), data['ports'] if 'ports' in data else [])
        self.requires = data['requires'] if 'requires' in data else []
        self.actions = data['actions'] if 'actions' in data else []
        self.variables = data['variables'] if 'variables' in data else {}
        self.files = data['files'] if 'files' in data else {}

    def create(self):
        print(f'[{self.name}] Create new container {self.id} from {self.box}')
        if 0 == subprocess.call(['lxc', 'launch', self.box, self.id, '-v']):
            self.execute_action('create')
        else:
            print(f'[{self.name}] Creation failed')

    def destroy(self):
        if self.get_lxc().status == 'Running':
            self.execute_action('down')
            self.denat()
            self.get_lxc().stop(wait=True)
        self.execute_action('destroy')
        subprocess.call(['lxc', 'delete', self.id, '-f'])

    def up(self):
        if self.get_lxc().status != 'Running':
            print(f'[{self.name}] Starting...')
            self.get_lxc().start(wait=True)
            self.nat()
            self.execute_action('up')
            print(f'[{self.name}] Done')
        else:
            print(f'[{self.name}] Already running')

    def down(self):
        if self.get_lxc().status == 'Running':
            print(f'[{self.name}] Stopping...')
            self.execute_action('down')
            self.denat()
            self.get_lxc().stop(wait=True)
            print(f'[{self.name}] Done')
        else:
            print(f'[{self.name}] Is not running')

    def nat(self):
        for port in self.ports:
            print(f'[{self.name}] Forwarding {port.to_port} to {port.get_ip()}:{port.from_port} ({port.device})')
            port.delete()
            port.create()

    def denat(self):
        for port in self.ports:
            print(f'[{self.name}] Removing forward from {port.to_port} to {port.get_ip()}:{port.from_port} ({port.device})')
            port.delete()

    def execute_action(self, action: str, parameters: dict = {}):
        for line in self.actions[action]:
            if type(line) == str:
                line = line.format(**parameters)
                print(f'[{self.name}] {line}')
                if 0 != subprocess.call(['lxc', 'exec', self.id, '--'] + shlex.split(line)):
                    print(f'[{self.name}] Execution failed')
                    return
            if isinstance(line, SpecialAction):
                line.call(self, self.loader)

    def get_ip(self, device: str = 'eth0') -> str:
        if self.ips and device not in self.ips:
            raise Exception(f'Container {self.id} has no device {device}')
        self.ips = {}
        for dev, configs in self.get_lxc().state().network.items():
            self.ips[dev] = [c['address'] for c in filter(lambda c: 'inet' == c['family'], configs['addresses'])][0]
        return self.ips[dev]

    def get_lxc(self):
        if not self.lxc:
            self.lxc = self.lxd.containers.get(self.id)
        return self.lxc


class SpecialAction:
    def call(self, container: Container, loader: ContainerLoader):
        pass


class Rpc(SpecialAction):
    def __init__(self, node: Union[ScalarNode, list]):
        value = node.value.split(' ') if type(node) == ScalarNode else node
        parameters = [f for f in filter(lambda a: a != '', value)]
        self.container = parameters.pop(0)
        self.action = parameters.pop(0)
        self.parameters = {}
        for parameter, value in map(lambda p: p.split('=', 2), parameters):
            self.parameters[parameter] = value

    def call(self, container: Container, loader: ContainerLoader):
        target = loader.get(self.container)
        parameters = {}
        for parameter, value in self.parameters.items():
            parameters[parameter] = Template(value).safe_substitute(container.variables)
        target.execute_action(self.action, parameters)


class DumpFile(SpecialAction):
    def __init__(self, node: Union[ScalarNode. list]):
        self.filename = node.value if type(node) == ScalarNode else node

    def call(self, container: Container, loader: ContainerLoader):
        print(f'[{container.name}] Dropping file {self.filename}')
        container.get_lxc().execute(['mkdir', '-p', os.path.dirname(self.filename)])
        t = Template(container.files[self.filename]).safe_substitute(container.variables)
        container.get_lxc().files.put(self.filename, t)


yaml.add_constructor('!rpc', lambda loader, node: Rpc(node))
yaml.add_constructor('!df', lambda loader, node: DumpFile(node))


def main():
    parser = argparse.ArgumentParser(description='Manager/Provisioner for LXD')
    parser.add_argument('verb', metavar='VERB', type=str, help='Operation to perform')
    parser.add_argument('container', metavar='CONTAINER', type=str, help='Container to work on')
    parser.add_argument('parameters', metavar='PARAMS', type=str, help='Parameters for the operation', nargs="*")
    parser.add_argument('-d', metavar='DIR', type=str, dest='definitions_dir', help='Definitions directory')

    args = parser.parse_args()

    loader = ContainerLoader(args.definitions_dir, Client())
    container = loader.get(args.container)

    if 'create' == args.verb:
        container.create()
    elif 'destroy' == args.verb:
        container.destroy()
    elif 'up' == args.verb:
        container.up()
    elif 'down' == args.verb:
        container.down()
    elif 'nat' == args.verb:
        container.nat()
    elif 'denat' == args.verb:
        container.denat()
    elif 'exec' == args.verb:
        call = Rpc([container.id] + args.parameters)
        call.call(container, loader)


if __name__ == '__main__':
    main()
