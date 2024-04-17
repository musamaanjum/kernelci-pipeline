#!/usr/bin/env python3
#
# SPDX-License-Identifier: LGPL-2.1-or-later
#
# Copyright (C) 2022 Maryam Yusuf
# Author: Maryam Yusuf <maryam.m.yusuf1802@gmail.com>
#
# Copyright (C) 2022 Collabora Limited
# Author: Jeny Sadadia <jeny.sadadia@collabora.com>

"""KCIDB bridge service"""

import datetime
import sys

import kernelci
import kernelci.config
from kernelci.legacy.cli import Args, Command, parse_opts
import kcidb

from base import Service


KCIDB_TEST_SUITE_MAPPING = {
    "baseline": "kernelci_baseline",
    "kver": "kernelci_kver",
    "kselftest": "kselftest",
    "kunit": "kunit",
    "sleep": "kernelci_sleep",
    "tast": "kernelci_tast",
}


class KCIDBBridge(Service):
    def __init__(self, configs, args, name):
        super().__init__(configs, args, name)

    def _setup(self, args):
        return {
            'client': kcidb.Client(
                project_id=args.kcidb_project_id,
                topic_name=args.kcidb_topic_name
            ),
            'sub_id': self._api_helper.subscribe_filters({
                'state': ('done', 'available'),
            }),
            'origin': args.origin,
        }

    def _stop(self, context):
        if context['sub_id']:
            self._api_helper.unsubscribe_filters(context['sub_id'])

    def _remove_none_fields(self, data):
        """Remove all keys with `None` values as KCIDB doesn't allow it"""
        if isinstance(data, dict):
            return {key: self._remove_none_fields(val)
                    for key, val in data.items() if val is not None}
        if isinstance(data, list):
            return [self._remove_none_fields(item) for item in data]
        return data

    def _send_revision(self, client, revision):
        revision = self._remove_none_fields(revision)
        self.log.debug(f"DEBUG: sending revision: {revision}")
        if kcidb.io.SCHEMA.is_valid(revision):
            return client.submit(revision)
        self.log.error("Aborting, invalid data")
        try:
            kcidb.io.SCHEMA.validate(revision)
        except Exception as exc:
            self.log.error(f"Validation error: {str(exc)}")

    @staticmethod
    def _set_timezone(created_timestamp):
        created_time = datetime.datetime.fromisoformat(created_timestamp)
        if not created_time.tzinfo:
            tz_utc = datetime.timezone(datetime.timedelta(hours=0))
            created_time = datetime.datetime.fromtimestamp(
                created_time.timestamp(), tz=tz_utc)
        return created_time.isoformat()

    def _parse_checkout_node(self, origin, checkout_node):
        result = checkout_node.get('result')
        result_map = {
            'pass': True,
            'fail': False,
            'incomplete': False,
        }
        valid = result_map[result] if result else None
        return [{
            'id': f"{origin}:{checkout_node['id']}",
            'origin': origin,
            'tree_name': checkout_node['data']['kernel_revision']['tree'],
            'git_repository_url':
                checkout_node['data']['kernel_revision']['url'],
            'git_commit_hash':
                checkout_node['data']['kernel_revision']['commit'],
            'git_commit_name':
                checkout_node['data']['kernel_revision'].get('describe'),
            'git_repository_branch':
                checkout_node['data']['kernel_revision']['branch'],
            'start_time': self._set_timezone(checkout_node['created']),
            'patchset_hash': '',
            'misc': {
                'submitted_by': 'kernelci-pipeline'
            },
            'valid': valid,
        }]

    def _get_output_files(self, artifacts: dict, exclude_properties=None):
        output_files = []
        for name, url in artifacts.items():
            if exclude_properties and name in exclude_properties:
                continue
            # Replace "/" with "_" to match with the allowed pattern
            # for "name" property of "output_files" i.e. '^[^/]+$'
            name = name.replace("/", "_")
            output_files.append(
                {
                    'name': name,
                    'url': url
                }
            )
        return output_files

    def _parse_build_node(self, origin, node):
        return [{
            'checkout_id': f"{origin}:{node['parent']}",
            'id': f"{origin}:{node['id']}",
            'origin': origin,
            'comment': node['data']['kernel_revision'].get('describe'),
            'start_time': self._set_timezone(node['created']),
            'architecture': node['data'].get('arch'),
            'compiler': node['data'].get('compiler'),
            'output_files': self._get_output_files(
                artifacts=node.get('artifacts', {}),
                exclude_properties=('build_log', '_config')
            ),
            'config_name': node['data'].get('config_full'),
            'config_url': node.get('artifacts', {}).get('_config'),
            'log_url': node.get('artifacts', {}).get('build_log'),
            'valid': node['result'] == 'pass',
            'misc': {
                'platform': node['data'].get('platform'),
                'runtime': node['data'].get('runtime'),
                'job_id': node['data'].get('job_id'),
                'job_context': node['data'].get('job_context'),
                'kernel_type': node['data'].get('kernel_type'),
            }
        }]

    def _parse_node_path(self, path, is_checkout_child, group):
        """Parse and create KCIDB schema compatible node path
        Convert node path list to dot-separated string and exclude
        'checkout' and build node from the path to make test suite
        the top level node
        """
        if isinstance(path, list):
            if is_checkout_child:
                # nodes with path such as ['checkout', 'kver']
                path_str = '.'.join(path[1:])
            else:
                # nodes with path such as ['checkout', 'kbuild-gcc-10-x86', 'baseline-x86']
                path_str = '.'.join(path[2:])

            # Prepend KCIDB test suite name to node path
            suite_name = KCIDB_TEST_SUITE_MAPPING.get(group)
            if not suite_name:
                # split group with '-' and match for group such as 'baseline-arm64'
                test_suite = group.split('-')[0]
                suite_name = KCIDB_TEST_SUITE_MAPPING.get(test_suite)
            if suite_name:
                path_str = suite_name + "." + path_str

            # Replace whitespace with "_" to match the allowed pattern for
            # test `path` i.e '^[.a-zA-Z0-9_-]*$'
            return path_str.replace(" ", "_")
        return None

    def _parse_node_result(self, test_node):
        if test_node['result'] == 'incomplete':
            if test_node['data'].get('error_code') in ('submit_error', 'invalid_job_params'):
                return 'MISS'
            return 'ERROR'
        return test_node['result'].upper()

    def _get_parent_build_node(self, node):
        node = self._api.node.get(node['parent'])
        if node['kind'] == 'kbuild' or node['kind'] == 'checkout':
            return node
        return self._get_parent_build_node(node)

    def _create_dummy_build_node(self, origin, checkout_node, arch):
        return {
            'id': f"{origin}:dummy_{checkout_node['id']}_{arch}" if arch
                  else f"{origin}:dummy_{checkout_node['id']}",
            'checkout_id': f"{origin}:{checkout_node['id']}",
            'comment': 'Dummy build for tests hanging from checkout',
            'origin': origin,
            'start_time': self._set_timezone(checkout_node['created']),
            'valid': True,
            'architecture': arch,
        }

    def _parse_test_node(self, origin, test_node):
        dummy_build = {}
        is_checkout_child = False
        build_node = self._get_parent_build_node(test_node)
        # Create dummy build node if test is hanging directly from checkout
        if build_node['kind'] == 'checkout':
            is_checkout_child = True
            dummy_build = self._create_dummy_build_node(origin, build_node,
                                                        test_node['data'].get('arch'))
            build_id = dummy_build['id']
        else:
            build_id = f"{origin}:{build_node['id']}"

        parsed_test_node = {
            'build_id': build_id,
            'id': f"{origin}:{test_node['id']}",
            'origin': origin,
            'comment': f"{test_node['name']} on {test_node['data'].get('platform')} \
in {test_node['data'].get('runtime')}",
            'start_time': self._set_timezone(test_node['created']),
            'environment': {
                'comment': f"Runtime: {test_node['data'].get('runtime')}",
                'misc': {
                    'platform': test_node['data'].get('platform'),
                    'job_id': test_node['data'].get('job_id'),
                    'job_context': test_node['data'].get('job_context'),
                }
            },
            'waived': False,
            'path': self._parse_node_path(test_node['path'], is_checkout_child,
                                          test_node['group']),
            'output_files': self._get_output_files(
                artifacts=test_node.get('artifacts', {})
            ),
            'misc': {
                'test_source': test_node['data'].get('test_source'),
                'test_revision': test_node['data'].get('test_revision'),
                'compiler': test_node['data'].get('compiler'),
                'kernel_type': test_node['data'].get('kernel_type'),
                'arch': test_node['data'].get('arch'),
                'error_code': test_node['data'].get('error_code'),
                'error_msg': test_node['data'].get('error_msg'),
            }
        }
        if test_node['result']:
            parsed_test_node['status'] = self._parse_node_result(test_node)
        return parsed_test_node, dummy_build

    def _run(self, context):
        self.log.info("Listening for events... ")
        self.log.info("Press Ctrl-C to stop.")

        while True:
            node = self._api_helper.receive_event_node(context['sub_id'])
            self.log.info(f"Submitting node to KCIDB: {node['id']}")

            parsed_checkout_node = []
            parsed_build_node = []
            parsed_test_node = []

            if node['kind'] == 'checkout':
                parsed_checkout_node = self._parse_checkout_node(
                    context['origin'], node)

            elif node['kind'] == 'kbuild':
                parsed_build_node = self._parse_build_node(
                    context['origin'], node
                )

            elif node['kind'] == 'test':
                test_node, build_node = self._parse_test_node(
                    context['origin'], node
                )
                parsed_test_node.append(test_node)
                if build_node:
                    parsed_build_node.append(build_node)

            revision = {
                'checkouts': parsed_checkout_node,
                'builds': parsed_build_node,
                'tests': parsed_test_node,
                'version': {
                    'major': 4,
                    'minor': 3
                }
            }
            self._send_revision(context['client'], revision)
        return True


class cmd_run(Command):
    help = "Listen for events and send them to KCDIB"
    args = [
        Args.api_config,
        {
            'name': '--kcidb-topic-name',
            'help': "KCIDB topic name",
        },
        {
            'name': '--kcidb-project-id',
            'help': "KCIDB project ID",
        },
        {
            'name': '--origin',
            'help': "CI system identifier",
        },
    ]

    def __call__(self, configs, args):
        return KCIDBBridge(configs, args, 'send_kcidb').run(args)


if __name__ == '__main__':
    opts = parse_opts('send_kcidb', globals())
    yaml_configs = opts.get_yaml_configs() or 'config'
    configs = kernelci.config.load(yaml_configs)
    status = opts.command(configs, opts)
    sys.exit(0 if status is True else 1)
