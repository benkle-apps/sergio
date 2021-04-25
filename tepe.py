#!/usr/bin/python3

from __future__ import annotations

import argparse
import os
import subprocess
import time

import pylxd.models
import yaml

from string import Template
from pylxd import Client
from yaml import ScalarNode
from typing import Union


def defaulting(obj: dict, key: str, default=None):
    if key in obj and obj[key] is not None:
        return obj[key]
    return default


class Templating:
    def __init__(self, variables: dict):
        self.variables = variables

    def apply(self, template: str, container_variables: dict = None, rpc_variables: dict = None) -> str:
        if container_variables is None:
            container_variables = {}
        variables = {**container_variables, **self.variables}
        if rpc_variables is not None:
            variables = {**variables, **rpc_variables}
        t = Template(template)
        return t.safe_substitute(variables)


class ContainerLoader:
    def __init__(self, definitions_dir: str, lxd: Client, variables_path: str = None):
        self.definitions_dir = definitions_dir
        self.container = {}
        self.lxd = lxd
        if variables_path is not None and os.path.isfile(variables_path):
            with open(variables_path, 'r+') as f:
                variables = yaml.full_load(f)['variables']
        else:
            variables = {}
        self.templating = Templating(variables)

    def list(self) -> list:
        result = []
        for file in os.listdir(self.definitions_dir):
            if not file.endswith('.yml') and not file.endswith('.yaml'):
                continue
            result.append(file.replace('.yaml', '').replace('.yml', ''))
        return result

    def path(self, container_id: str) -> str:
        path = os.path.join(self.definitions_dir, f'{container_id}.yaml')
        if not os.path.exists(path):
            path = os.path.join(self.definitions_dir, f'{container_id}.yml')
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
    def __init__(self, data: dict, container: Container):
        self.container = container
        self.device = data['device']
        self.protocol = data['protocol']
        self.from_port = data['from']
        self.to_port = data['to']
        self.comment = defaulting(data, 'comment', container.name)

    def delete(self):
        check = subprocess.check_output(['sudo', '-S', 'iptables', '-L', '-n', '-t', 'nat', '--line-numbers']) \
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


class Mountpoint:
    def __init__(self, name: str, data: dict, container: Container):
        self.container = container
        self.name = name
        self.source = data['source']
        self.path = data['path']

    def mount(self):
        if not self.is_mounted():
            self.container.get_lxc().devices[self.name] = {
                'path': self.path,
                'source': self.source,
                'type': 'disk',
            }

    def is_mounted(self) -> bool:
        return self.name in self.container.get_lxc().devices


class Container:
    def __init__(self, cid: str, data: dict, loader: ContainerLoader, lxd: Client):
        self.id = cid
        self.loader = loader
        self.lxd = lxd
        self.lxc = None
        self.ips = None
        self.name = data['name']
        self.description = data['description']
        self.box = data['box']
        self.mountpoints = map(
            lambda mp: Mountpoint(mp[0], mp[1], self),
            defaulting(data, 'mountpoints', {}).items()
        )
        self.ports = map(lambda port: Port(port, self), defaulting(data, 'ports', []))
        self.requires = defaulting(data, 'requires', [])
        self.actions = defaulting(data, 'actions', [])
        self.variables = defaulting(data, 'variables', {})
        self.files = defaulting(data, 'files', {})
        self.shell = defaulting(data, 'shell', '/bin/sh')
        self.user = defaulting(data, 'user', 'root')

    def check_requirements(self, ignore_stopped: bool = False):
        okay = True
        for requirement in self.requires:
            requirement = self.loader.get(requirement)
            if not requirement.exists():
                self.log(f'Requires {requirement.name} ({requirement.id}), but it does not exist')
                okay = False
            elif not ignore_stopped and not requirement.is_running():
                self.log(f'Requires {requirement.name} ({requirement.id}), but it is not running')
                okay = False
        return okay

    def is_running(self) -> bool:
        return self.get_lxc().status == 'Running'

    def exists(self):
        return self.lxd.containers.exists(self.id)

    def get_launch_order(self):
        containers = {}
        launch_order = []
        for requirement in self.requires:
            requirement = self.loader.get(requirement)
            containers[requirement.id] = requirement.requires
        changes = True
        while changes:
            changes = False
            for container, requirements in list(containers.items()):
                for requirement in requirements:
                    if requirement not in containers:
                        requirement = self.loader.get(requirement)
                        containers[requirement.id] = requirement.requires
                        changes = True
        while containers:
            launchables = [container for container, requirements in containers.items() if [] == requirements]
            if not launchables:
                raise Exception('Unresolvable requirements')
            launchable = launchables.pop(0)
            launch_order.append(launchable)
            del containers[launchable]
            for container, requirements in containers.items():
                if launchable in requirements:
                    requirements.remove(launchable)
        for launchable in launch_order:
            launchable = self.loader.get(launchable)
            if not launchable.exists():
                raise Exception(f'Requires {launchable.name} ({launchable.id}), but it does not exist')
        return launch_order

    def log(self, message: str):
        print(f'[{self.name}] {message}')

    def mount(self):
        for mountpoint in self.mountpoints:
            if not mountpoint.is_mounted():
                self.log(f'Mounting {mountpoint.name}')
                mountpoint.mount()
        self.get_lxc().save()

    def create(self):
        self.log(f'Create new container {self.id} from {self.box}')
        if not self.check_requirements():
            self.log('Requirements not met')
        elif 0 == subprocess.call(['lxc', 'launch', self.box, self.id, '-v']):
            self.mount()
            self.log('Waiting for network to calm down')
            time.sleep(5)
            self.nat()
            self.execute_action('create')
            self.execute_action('up')
            self.log('Done')
        else:
            self.log(f'Creation failed')

    def destroy(self):
        if self.is_running():
            self.execute_action('down')
            self.denat()
            self.get_lxc().stop(wait=True)
        self.execute_action('destroy')
        subprocess.call(['lxc', 'delete', self.id, '-f'])

    def up(self, recursive: bool):
        if self.is_running():
            self.log('Already running')
        elif not self.check_requirements(recursive):
            self.log('Requirements not met')
        else:
            if recursive:
                for requirement in self.get_launch_order():
                    container = self.loader.get(requirement)
                    if not container.is_running():
                        container.up(False)
            self.log('Starting...')
            self.get_lxc().start(wait=True)
            self.log('Waiting for network to calm down')
            time.sleep(5)
            self.nat()
            self.execute_action('up')
            self.log('Done')

    def down(self):
        if self.is_running():
            self.log('Stopping...')
            self.execute_action('down')
            self.denat()
            self.get_lxc().stop(wait=True)
            self.log('Done')
        else:
            self.log('Is not running')

    def nat(self):
        if not self.is_running():
            self.log('Container not running, not NAT needed')
            return
        for port in self.ports:
            self.log(f'Forwarding {port.to_port} to {port.get_ip()}:{port.from_port} ({port.device})')
            port.delete()
            port.create()

    def denat(self):
        for port in self.ports:
            self.log(f'Removing forward from {port.to_port} to {port.get_ip()}:{port.from_port} ({port.device})')
            port.delete()

    def exec(self, code: str = None) -> int:
        cmd = []
        if code is not None:
            cmd = ['-c', code]
        return subprocess.call(['lxc', 'exec', self.id, '--', 'sudo', '--login', '--user', self.user, self.shell] + cmd)

    def execute_action(self, action: str, parameters: dict = {}):
        if action not in self.actions:
            self.log(f'Action "{action}" does not exist')
            return
        self.log(f'Execute action "{action}"')
        for line in self.actions[action]:
            if type(line) == str:
                line = self.loader.templating.apply(line, self.variables, parameters)
                self.log(line)
                if 0 != self.exec(line):
                    self.log('Execution failed')
                    return
            if isinstance(line, SpecialAction):
                line.call(self, self.loader)

    def get_ip(self, device: str = 'eth0') -> str:
        if self.ips and device not in self.ips:
            raise Exception(f'Container {self.id} has no device {device}')
        self.ips = {}
        for dev, configs in self.get_lxc().state().network.items():
            self.ips[dev] = [c['address'] for c in filter(lambda c: 'inet' == c['family'], configs['addresses'])][0]
        return self.ips[device]

    def get_lxc(self) -> pylxd.models.Container:
        if not self.lxc:
            self.lxc = self.lxd.containers.get(self.id)
        return self.lxc

    def login(self):
        if not self.is_running():
            self.log('Not running')
        else:
            self.exec()


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
            parameters[parameter] = loader.templating.apply(value, container.variables)
        target.execute_action(self.action, parameters)


class DumpFile(SpecialAction):
    def __init__(self, node: Union[ScalarNode, list]):
        self.filename = node.value if type(node) == ScalarNode else node

    def call(self, container: Container, loader: ContainerLoader):
        container.log(f'Dropping file {self.filename}')
        container.get_lxc().execute(['mkdir', '-p', os.path.dirname(self.filename)])
        container.get_lxc().files.put(
            self.filename,
            loader.templating.apply(container.files[self.filename], container.variables)
        )
        container.exec(f'sudo chown {container.user}:{container.user} {self.filename}')


yaml.add_constructor('!rpc', lambda loader, node: Rpc(node))
yaml.add_constructor('!df', lambda loader, node: DumpFile(node))


def main():
    parser = argparse.ArgumentParser(description='Manager/Provisioner for LXD')
    parser.add_argument('container', metavar='CONTAINER', type=str, help='Container to work on')
    parser.add_argument('verb', metavar='VERB', type=str, help='Operation to perform')
    parser.add_argument('parameters', metavar='PARAMS', type=str, help='Parameters for the operation', nargs="*")
    parser.add_argument('-d', metavar='DIR', type=str, dest='definitions_dir', help='Definitions directory')
    parser.add_argument('-v', metavar='FILE', type=str, dest='variables_file', help='YAML file with variable values',
                        default=None)
    parser.add_argument('-r', type=bool, dest='recursive', help='Start containers recursively', default=False)

    args = parser.parse_args()

    loader = ContainerLoader(os.path.abspath(args.definitions_dir), Client(), os.path.abspath(args.variables_file))
    container = loader.get(args.container)

    if 'create' == args.verb:
        container.create()
    elif 'destroy' == args.verb:
        container.destroy()
    elif 'up' == args.verb:
        container.up(args.recursive)
    elif 'down' == args.verb:
        container.down()
    elif 'nat' == args.verb:
        container.nat()
    elif 'denat' == args.verb:
        container.denat()
    elif 'login' == args.verb:
        container.login()
    elif 'exec' == args.verb:
        call = Rpc([container.id] + args.parameters)
        call.call(container, loader)
    else:
        call = Rpc([container.id] + [args.verb] + args.parameters)
        call.call(container, loader)


if __name__ == '__main__':
    main()
