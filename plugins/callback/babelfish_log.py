# (C) 2020, Teagan Glenn, <that@teagantotally.rocks>
# (C) 2020 Constructor Fleet
# (C) 2012, Michael DeHaan, <michael.dehaan@gmail.com>
# (c) 2017 Ansible Project
# MIT License
from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

DOCUMENTATION = '''
    author: Teagan Glenn
    callback: babelfish_log
    type: notification
    short_description: write playbook output to human readable log file
    description:
      - This callback writes human readable playbook output to a file per host in the `/var/log/ansible/hosts` directory
    requirements:
     - Whitelist in configuration
     - A writeable directory by the user executing Ansible on the controller
    options:
      log_folder:
        default: /var/log/ansible/hosts
        description: The folder where log files will be created.
        env:
          - name: ANSIBLE_LOG_FOLDER
        ini:
          - section: callback_babelfish_log
            key: log_folder
      format_invocation:
        default: no
        description: Whether or not to format the invocation dictionary./
        env:
          - name: ANSIBLE_LOG_FORMAT_INVOCATION
        ini:
          - section: callback_babelfish_log
            key: format_invocation
      max_bytes:
        default: 0
        description: Maximum log file size in bytes before rolling over.
        env:
          - name: ANSIBLE_LOG_MAX_BYTES
        ini:
          - section: callback_babelfish_log
            key: max_bytes
      backup_count:
        default: 0
        description: Maximum number of log files to keep before rotating files out.
        env:
          - name: ANSIBLE_LOG_BACKUP_COUNT
        ini:
          - section: callback_babelfish_log
            key: backup_count
      time_format:
        default: "%b %d %Y %H:%M:%S"
        description: Time format string.
        env:
          - name: ANSIBLE_LOG_TIME_FORMAT
        ini:
          - section: callback_babelfish_log
            key: time_format
      msg_format:
        default: "%(now)s - %(playbook)s - %(task_name)s - %(task_action)s - %(category)s - %(data)s\n\n"
        description: Format string for log messages.
        env:
          - name: ANSIBLE_LOG_MSG_FORMAT
        ini:
          - section: callback_babelfish_log
            key: msg_format
      respect_no_log:
        default: yes
        description: Whether or not to respect the no_log module argument.
        env:
          - name: ANSIBLE_LOG_RESPECT_NO_LOG
        ini:
          - section: callback_babelfish_log
            key: respect_no_log
      whitelist_dict_keys:
        default: ''
        description: Comma separated list of dictionary keys to report back, all if empty.
        env:
          - name: ANSIBLE_LOG_WHITELIST_KEYS
        ini:
          - section: callback_babel_log
            key: whitelist_dict_keys
'''

import os
import time
import json
import logging
from logging.handlers import RotatingFileHandler
from logging import ERROR, INFO, WARNING

from ansible.utils.path import makedirs_safe
from ansible.module_utils.common._collections_compat import MutableMapping
from ansible.plugins.callback import CallbackBase

DEFAULT_FOLDER = '/var/log/ansible/hosts'
DEFAULT_TIME_FORMAT = '%b %d %Y %H:%M:%S'
DEFAULT_MSG_FORMAT = '%(now)s - %(playbook)s - %(task_name)s - %(task_action)s - %(category)s - %(data)s\n\n'

# Fields to reformat output for
FIELDS = [
    'cmd',
    'command',
    'start',
    'end',
    'delta',
    'msg',
    'stdout',
    'stderr',
    'results'
]


class CallbackModule(CallbackBase):
    """
    Logs human readable playbook output, per host, to the specified directory.
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'notification'
    CALLBACK_NAME = 'constructorfleet.vogon.babelfish_log'
    CALLBACK_NEEDS_WHITELIST = True

    time_format = DEFAULT_TIME_FORMAT
    msg_format = DEFAULT_MSG_FORMAT

    log_folder = DEFAULT_FOLDER
    format_invocation = False
    respect_no_log = True
    max_bytes = 0
    backup_count = 0
    whitelist_keys = []

    loggers = {}
    play_timestamp = ''
    playbook = None

    def __init__(self):
        super(CallbackModule, self).__init__()

    def set_options(self,
                    task_keys=None,
                    var_options=None,
                    direct=None):
        super(CallbackModule, self).set_options(
            task_keys=task_keys,
            var_options=var_options,
            direct=direct
        )
        self.log_folder = self.get_option('log_folder') \
            if 'log_folder' in self._plugin_options \
            else DEFAULT_FOLDER
        self.time_format = self.get_option('time_format') \
            if 'time_format' in self._plugin_options \
            else DEFAULT_TIME_FORMAT
        self.msg_format = self.get_option('msg_format') \
            if 'msg_format' in self._plugin_options \
            else DEFAULT_MSG_FORMAT

        self.max_bytes = int(self.get_option('max_bytes')
                             if 'max_bytes' in self._plugin_options
                             else 0)
        self.backup_count = int(self.get_option('backup_count')
                                if 'backup_count' in self._plugin_options
                                else 0)

        self.format_invocation = (self.get_option('format_invocation')
                                  if 'format_invocation' in self._plugin_options
                                  else 'no').lower() == 'yes'

        self.whitelist_keys = (self.get_option('whitelist_dict_keys')
                               if 'whitelist_dict_keys' in self._plugin_options
                               else '').split(',')

        if not os.path.exists(self.log_folder):
            makedirs_safe(self.log_folder)

    def _get_logger(self, host):
        logger = logging.getLogger('babelfish_log_%s' % host)
        logger.setLevel(INFO)
        logger.addHandler(
            RotatingFileHandler(
                os.path.join(self.log_folder, host),
                maxBytes=self.max_bytes,
                backupCount=self.backup_count
            )
        )
        self.loggers[host] = logger
        return logger

    def _format_output(self, output):
        # If output is a dict
        if type(output) == dict:
            filtered_output = output.copy()
            if self.whitelist_keys:
                filtered_output = {
                    k: v
                    for k, v
                    in filtered_output.items()
                    if k in self.whitelist_keys
                }
            return json.dumps(filtered_output, indent=2, sort_keys=True)

        # If output is a list of dicts
        if type(output) == list and type(output[0]) == dict:
            # This gets a little complicated because it potentially means
            # nested results, usually because of with_items.
            real_output = list()
            for index, item in enumerate(output):
                copy = item
                if type(item) == dict:
                    for field in FIELDS:
                        if field in item.keys():
                            copy[field] = self._format_output(item[field])
                real_output.append(copy)
            return json.dumps(output, indent=2, sort_keys=True)

        # If output is a list of strings
        if type(output) == list and type(output[0]) != dict:
            # Strip newline characters
            real_output = list()
            for item in output:
                if "\n" in item:
                    for string in item.split("\n"):
                        real_output.append(string)
                else:
                    real_output.append(item)

            # Reformat lists with line breaks only if the total length is
            # >75 chars
            if len("".join(real_output)) > 75:
                return "\n" + "\n".join(real_output)
            else:
                return " ".join(real_output)

        # Otherwise it's a string, (or an int, float, etc.) just return it
        return str(output)

    def log(self, result, category, log_level):
        data = result._result
        if isinstance(data, MutableMapping) or isinstance(data, dict):
            if self.resp data.get('_ansible_no_log', False):
                return

            if '_ansible_verbose_override' in data:
                # avoid logging extraneous data
                data = 'omitted'
            else:
                data = data.copy()
                invocation = data.pop('invocation', {})
                invocation = invocation.get('module_args', None)
                data = self._format_output(data)
                if invocation is not None:
                    invocation = self._format_output(invocation) \
                        if self.format_invocation \
                        else json.dumps(invocation)
                    data = invocation + " => %s " % data

        now = time.strftime(self.time_format, time.localtime())

        msg = self.msg_format % dict(
            now=now,
            playbook=self.playbook,
            task_name=result._task.name,
            task_action=result._task.action,
            category=category,
            data=data,
        )
        host_name = result._host.get_name()
        logger = self.loggers[host_name] if host_name in self.loggers else self._get_logger(host_name)

        logger.log(log_level, msg)

    def v2_runner_on_failed(self, result, ignore_errors=False):
        self.log(result, 'FAILED', ERROR)

    def v2_runner_on_ok(self, result):
        self.log(result, 'OK', INFO)

    def v2_runner_on_skipped(self, result):
        self.log(result, 'SKIPPED', INFO)

    def v2_runner_on_unreachable(self, result):
        self.log(result, 'UNREACHABLE', WARNING)

    def v2_runner_on_async_failed(self, result):
        self.log(result, 'ASYNC_FAILED', ERROR)

    def v2_playbook_on_start(self, playbook):
        self.playbook = playbook._file_name

    def v2_playbook_on_import_for_host(self, result, imported_file):
        self.log(result, 'IMPORTED', INFO)

    def v2_playbook_on_not_import_for_host(self, result, missing_file):
        self.log(result, 'NOTIMPORTED', INFO)
