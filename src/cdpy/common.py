# -*- coding: utf-8 -*-

from datetime import datetime
from time import time, sleep
import html
import io
import json
import logging
import platform
import re
import warnings
import traceback
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from json import JSONDecodeError
from typing import Union

import cdpcli
from cdpcli import VERSION
from cdpcli.client import ClientCreator, Context
from cdpcli.credentials import Credentials
from cdpcli.endpoint import EndpointCreator, EndpointResolver
from cdpcli.exceptions import ClientError
from cdpcli.loader import Loader
from cdpcli.parser import ResponseParserFactory
from cdpcli.retryhandler import create_retry_handler
from cdpcli.translate import build_retry_config


class CdpWarning(UserWarning):
    """Class for deriving custom warnings from UserWarning"""
    def __init__(self, message):
        self.message = message


class CdpError(Exception):
    """Parser Class for Errors returned from CDP CLI SDK"""
    def __init__(self, base_error, *args):
        self.base_error = base_error
        self.ext_traceback = traceback.format_stack()[:-1]
        self.error_code = None
        self.violations = None
        self.message = None
        self.status_code = None
        self.rc = None
        self.service = None
        self.operation = None
        self.request_id = None

        if isinstance(self.base_error, ClientError):
            _CLIENT_ERROR_PATTERN = re.compile(
                r"Status Code: (.*?); Error Code: (.*?); Service: "
                r"(.*?); Operation: (.*?); Request ID: (.*?);"
            )
            _payload = re.search(_CLIENT_ERROR_PATTERN, str(self.base_error))
            try:
                _violations = json.loads(html.unescape(self.base_error.response['error']['message']))
            except JSONDecodeError:
                try:
                    _violations = self.base_error.response['error']['message']
                except KeyError:
                    _violations = self.base_error.args[0]
            try:
                self.error_code = self.base_error.response['error']['code']
            except KeyError:
                self.error_code = ''
            self.violations = _violations
            self.message = _violations
            self.status_code = _payload.group(1)
            self.rc = 1
            self.service = _payload.group(3)
            self.operation = _payload.group(4)
            self.request_id = _payload.group(5)

        super().__init__(base_error, *args)

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)

    def __str__(self):
        return self.__repr__()


class Squelch(dict):
    def __init__(self, value, field='error_code', default=None, warning=None):
        super().__init__()
        self.field = field
        self.value = value
        self.default = default
        self.warning = warning


class StaticCredentials(Credentials):
    """A credential class that simply takes a set of static credentials."""

    def __init__(self, access_key_id, private_key, access_token='', method='static'):
        super(StaticCredentials, self).__init__(
            access_key_id=access_key_id, private_key=private_key,
            access_token=access_token, method=method
        )


class CdpcliWrapper(object):
    def __init__(self, debug=False, tls_verify=False, strict_errors=False, tls_warnings=False, client_endpoint=None,
                 cdp_credentials=None, error_handler=None, warning_handler=None, scrub_inputs=True):
        # Init Params
        self.debug = debug
        self.tls_verify = tls_verify
        self.strict_errors = strict_errors
        self.tls_warnings = tls_warnings
        self.client_endpoint = client_endpoint
        self.cdp_credentials = cdp_credentials
        self.scrub_inputs = scrub_inputs

        # Setup
        self.throw_error = error_handler if error_handler else self._default_throw_error
        self.throw_warning = warning_handler if warning_handler else self._default_throw_warning
        self._clients = {}
        self.DEFAULT_PAGE_SIZE = 100

        _loader = Loader()
        _user_agent = self._make_user_agent_header()
        self._client_creator = ClientCreator(
            _loader,
            Context(),
            EndpointCreator(EndpointResolver()),
            _user_agent,
            ResponseParserFactory(),
            create_retry_handler(self._load_retry_config(_loader)))

        # Logging
        _log_format = '%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
        if debug:
            self._setup_logger(logging.DEBUG, _log_format)
            self.logger.debug("CDP SDK version: %s", _user_agent)
        else:
            self._setup_logger(logging.ERROR, _log_format)

        if self.tls_warnings is False:
            urllib3.disable_warnings(InsecureRequestWarning)

        # Warnings
        def _warning_format(message, category, filename, lineno, line=None):
            return ' %s:%s: %s:%s' % (filename, lineno, category.__name__, message)

        warnings.formatwarning = _warning_format

        # State listings
        # https://github.com/hortonworks/cloudbreak/blob/master/cluster-api/src/main/java/com/sequenceiq/
        #   cloudbreak/cluster/status/ClusterStatus.java#L8-L18
        # https://github.com/hortonworks/cloudbreak/blob/master/core-api/src/main/java/com/sequenceiq/
        #   cloudbreak/api/endpoint/v4/common/Status.java#L14-L53
        self.CREATION_STATES = [
            'REQUESTED',
            'EXTERNAL_DATABASE_CREATION_IN_PROGRESS',
            'STACK_CREATION_IN_PROGRESS',
            'CREATION_INITIATED',
            'FREEIPA_CREATION_IN_PROGRESS',
            'STARTING',
            'ENABLING',  # DF
            'provision:started',  # ML
            'installation:started'  # ML
        ]

        self.TERMINATION_STATES = [
            'EXTERNAL_DATABASE_DELETION_IN_PROGRESS',
            'STACK_DELETION_IN_PROGRESS',
            'FREEIPA_DELETE_IN_PROGRESS',
            'STOPPING',
            'deprovision:started',  # ML
            'DISABLING'  # DF
        ]

        self.STARTED_STATES = [
            'EXTERNAL_DATABASE_START_IN_PROGRESS',
            'AVAILABLE',
            'START_IN_PROGRESS',
            'RUNNING',
            'installation:finished',  # ML
            'Running',  # DW
            'GOOD_HEALTH'  # DF
        ]

        self.STOPPED_STATES = [
            'EXTERNAL_DATABASE_STOP_IN_PROGRESS',
            'STOP_IN_PROGRESS',
            'STOPPED',
            'ENV_STOPPED',
            'NOT_ENABLED'  # DF
        ]

        self.FAILED_STATES = [
            'PROVISIONING_FAILED',
            'CREATE_FAILED',
            'REJECTED',
            'FAILED',
            'TIMEDOUT',
            'DELETE_FAILED',
            'Error',  # DW
            'installation:failed',  # ML
            'deprovision:failed',  # ML
            'BAD_HEALTH'  # DF
        ]

        self.REMOVABLE_STATES = [
            'AVAILABLE', 'UPDATE_FAILED', 'CREATE_FAILED', 'ENABLE_SECURITY_FAILED', 'DELETE_FAILED',
            'DELETE_COMPLETED', 'DELETED_ON_PROVIDER_SIDE', 'STOPPED', 'START_FAILED', 'STOP_FAILED',
            'installation:failed', 'deprovision:failed', 'installation:finished',  # ML
            'Error', 'Running',  # DW
            'GOOD_HEALTH', 'CONCERNING_HEALTH', 'BAD_HEALTH',  # DF
        ]

        # common regex patterns
        self.DATAHUB_NAME_PATTERN = re.compile(r'[^a-z0-9-]')
        self.DATALAKE_NAME_PATTERN = re.compile(r'[^a-z0-9-]')
        self.ENV_NAME_PATTERN = re.compile(r'(^[^a-z0-9]|[^a-z0-9-]|^.{,4}$|^.{29,}$)')
        self.CREDENTIAL_NAME_PATTERN = re.compile(r'[^a-z0-9-]')
        self.OPERATION_REGEX = re.compile(r'operation ([0-9a-zA-Z-]{36}) running')

    @staticmethod
    def _make_user_agent_header():
        return 'CDPSDK/%s Python/%s %s/%s' % (VERSION,
                                              platform.python_version(),
                                              platform.system(),
                                              platform.release())

    @staticmethod
    def _load_retry_config(loader):
        original_config = loader.load_json('_retry.json')
        retry_config = build_retry_config(
            original_config['retry'],
            original_config.get('definitions', {}))
        return retry_config

    def _setup_logger(self, log_level, log_format):
        self.logger = logging.getLogger('CdpSdk')
        self.logger.setLevel(log_level)

        self.__log_capture = io.StringIO()
        handler = logging.StreamHandler(self.__log_capture)
        handler.setLevel(log_level)

        formatter = logging.Formatter(log_format)
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)

    def _build_client(self, service):
        if not self.cdp_credentials:
            self.cdp_credentials = self._client_creator.context.get_credentials()
        return self._client_creator.create_client(
            service,
            self.client_endpoint,
            self.tls_verify,
            self.cdp_credentials
        )

    @staticmethod
    def _default_throw_error(error: 'CdpError'):
        """
        Default Error Handler if not supplied during init
        Args:
            error (CdpError): The Error to raise, expects a CdpError

        Returns:
            None

        Raises:
            CdpError: The supplied Error
        """
        raise error

    @staticmethod
    def _default_throw_warning(warning: 'CdpWarning'):
        """
        Default Warning Handler if not supplied during init
        Args:
            warning (str): The Warning string to process

        Returns:
            None
        """
        warnings.warn(message=warning.message)

    # Public convenience Methods
    @staticmethod
    def regex_search(pattern, obj):
        return re.search(pattern, obj)

    def validate_crn(self, obj: str):
        if obj is not None and obj.startswith('crn:'):
            pass
        else:
            self.throw_error(CdpError("Supplied env_crn %s is not a valid CDP crn" % str(obj)))

    @staticmethod
    def sleep(seconds):
        sleep(seconds)

    @staticmethod
    def first_item_if_exists(obj):
        """Accepts an iterable like a list, and returns the first item if there is one"""
        return next(iter(obj), obj)

    @staticmethod
    def filter_by_key(obj, key):
        """Accepts a list of dicts and a key, returns a flat list of that key from the dicts"""
        return list(map(lambda f: f[key], obj))

    @staticmethod
    def dumps(data):
        """Perform a json.dumps, but handle datetime objects."""

        def _convert(o):
            if isinstance(o, datetime):
                return o.__str__()

        return json.dumps(data, indent=2, default=_convert)

    def _client(self, service):
        """Builds a CDP Endpoint client of a given type, and caches it against later reuse"""
        if service not in self._clients:
            self._clients[service] = self._build_client(service)
        return self._clients[service]

    def read_file(self, file_path):
        try:
            with open(str(file_path), 'r') as f:
                return f.read()
        except IOError as err:
            parsed_err = CdpError(err)
            if self.debug:
                log = self.get_log()
                parsed_err.update(sdk_out=log, sdk_out_lines=log.splitlines())
            self.throw_error(parsed_err)

    def get_log(self):
        contents = self.__log_capture.getvalue()
        self.__log_capture.truncate(0)
        return contents

    @staticmethod
    def _get_path(obj, path):
        value = obj
        for p in path:
            if isinstance(value, dict):
                value = value.get(p)
            else:
                value = None
            if value is None:
                return None
        return value

    def wait_for_state(self, describe_func, params: dict, field: Union[str, None, list] = 'status',
                       state: Union[list, str, None] = None, delay: int = 15, timeout: int = 3600,
                       ignore_failures: bool = False):
        """
        Proceses a loop waiting for a given function to achieve a given state or known failure states

        Args:
            describe_func (func): The status check function to call as it relates to the sdk object,
                e.g self.cdpy.opdb.describe_database
            params (dict): Parameters the describe_func requires to poll the status, e.g. { name=myname, env=myenv }
            field (str, None): The field to check in the describe_func output for the state. Use None to check for
                listing removal during deletion. Provide a list of strings for nested structures. Defaults to 'status'
            state (list, str, None): The state or list of states valid for return from wait function. Defaults to None
            delay (int): Delay in seconds between each poll of the describe_func. Default is 15
            timeout (int): Total wait time in seconds before the function should return a timeout. Default is 3600
            ignore_failures (bool): Whether to ignore failed states when waiting for a forced deletion

        Returns: Output of describe function received during last polling attempt.
        """
        self.logger.info("Waiting for function {0} on params [{1}] to have field {2} with state {3}"
                         .format(describe_func.__name__, str(params), field, str(state)))
        state = state if isinstance(state, list) else [state]
        if field is not None:
            field = field if isinstance(field, list) else [field]
        start_time = time()
        while time() < start_time + timeout:
            current = describe_func(**params)
            if current is None:
                if field is None:
                    return current
                else:
                    self.logger.info("Waiting for identity {0} to be returned by function {1}")
            else:
                if field is not None:
                    current_status = self._get_path(current, field)
                else:  # field not provided, therefore seek default status fields to check for failures
                    default_status_fields = [
                        ['status'],  # Datalake, DW, OpDB, Datahub
                        ['instanceStatus'],   # ML
                        ['status', 'state'],  # DF
                    ]
                    possible_status = [
                        self._get_path(current, x) for x in default_status_fields
                        if x[0] in current
                    ]
                    selected_status = [x for x in possible_status if x is not None]
                    if len(selected_status) > 0:
                        current_status = selected_status[0]
                    else:
                        current_status = None
                        self.throw_error(
                            CdpError("Could not determine default status field in response {0}".format(current))
                        )
                if current_status is None:
                    self.logger.info("Waiting to find field {0} in function {1} response"
                                     .format(field, describe_func))
                elif current_status in state:
                    return current
                elif current_status in self.FAILED_STATES:
                    status_reason = 'None provided'
                    for fail_msg_field in ['statusReason', 'failureMessage']:
                        if fail_msg_field in current:
                            status_reason = current[fail_msg_field]
                    if ignore_failures:
                        self.throw_warning(
                            CdpWarning("Ignored Failure status '%s' while waiting" % current_status)
                        )
                    else:
                        self.throw_error(
                            CdpError("Function {0} with params [{1}] encountered failed state {2} with reason {3}"
                                     .format(describe_func.__name__, str(params), current_status, status_reason)))
                else:
                    self.logger.info("Waiting for change in {0}: [{1}], current is {2}: {3}"
                                     .format(describe_func.__name__, str(params), field, current_status))
            sleep(delay)
        else:
            self.throw_error(
                CdpError("Timeout waiting for function {0} with params [{1}] to return field {2} with state {3}"
                         .format(describe_func.__name__, str(params), field, str(state))))

    def call(self, svc: str, func: str, ret_field: str = None, squelch: ['Squelch'] = None, ret_error: bool = False,
             **kwargs: Union[dict, bool, str, list]) -> Union[list, dict, 'CdpError']:
        """
        Wraps the call to an underlying CDP CLI Service, handles common errors, and parses output

        Args:
            svc (str): Name of the service, ex. iam
            func (str): Name of the function to call, ex. get-user
            ret_field (str, None): Name of the top level child field to return from results, ex. user
            squelch (list(Squelch)): list of Descriptions of Error squelching options
            ret_error (bool): Whether to return the error object if generated,
                defaults to False and raise instead
            **kwargs (dict): Keyword Args to be supplied to the Function, eg userId

        Returns (dict, list, None): Output of CDP CLI Call
        """
        try:
            call_function = getattr(self._client(service=svc), func)
            if self.scrub_inputs:
                # Remove unused submission values as the API rejects them
                payload = {x: y for x, y in kwargs.items() if y is not None}
                # Remove and issue warning for empty string submission values as the API rejects them
                _ = [self.throw_warning(
                    CdpWarning('Removing empty string arg %s from submission' % x))
                    for x, y in payload.items() if y == '']
                payload = {x: y for x, y in payload.items() if y != ''}
            else:
                payload = kwargs

            resp = call_function(**payload)

            if 'nextToken' in resp:
                while 'nextToken' in resp:
                    logging.debug("Found paged results in %s" % func)
                    token = resp.pop('nextToken')
                    next_page = call_function(
                        **payload, startingToken=token, pageSize=self.DEFAULT_PAGE_SIZE)
                    for key in next_page.keys():
                        if isinstance(next_page[key], str):
                            resp[key] = next_page[key]
                        elif isinstance(next_page[key], list):
                            resp[key] += (next_page[key])

            if ret_field is not None:
                if not resp:
                    self.throw_warning(CdpWarning('Call Response is empty, cannot return child field %s' % ret_field))
                else:
                    return resp[ret_field]

            return resp

        except Exception as err:
            # Note that the cascade of behaviors here is designed to be convenient for Ansible module development
            parsed_err = CdpError(err)
            if self.debug:
                log = self.get_log()
                parsed_err.update(sdk_out=log, sdk_out_lines=log.splitlines())
            if self.strict_errors is True:
                self.throw_error(parsed_err)
            if isinstance(err, ClientError) and squelch is not None:
                for item in squelch:
                    if item.value in str(parsed_err.__getattribute__(item.field)):
                        warning = item.warning if item.warning is not None else str(parsed_err.violations)
                        self.throw_warning(CdpWarning(warning))
                        return item.default
            if ret_error is True:
                return parsed_err
            self.throw_error(parsed_err)


class CdpSdkBase(object):
    """A base class to use for explicitly namespacing child service calls alongside the sdk"""
    def __init__(self, *args, **kwargs):
        self.sdk = CdpcliWrapper(*args, **kwargs)
