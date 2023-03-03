====
cdpy
====


A Simple Pythonic Client wrapper for Cloudera CDP CLI, designed for use with the Ansible framework

Installation
============

To install directly from latest commits, in python requirements.txt ::

    git+git://github.com/cloudera-labs/cdpy@main#egg=cdpy

For General usage, installed from cmdline ::

    pip install cdpy

To install the development branch instead of main ::

    git+git://github.com/cloudera-labs/cdpy@devel#egg=cdpy

Usage
=====
Note that this readme covers usage of this wrapper library only, for details of CDPCLI and underlying commands please see the CDPCLI documentation.

Simple Usage
------------

Basic function call with defaults where credentials are already available in user profile ::

    from cdpy.cdpy import Cdpy
    Cdpy().iam.get_user()
    > {'userId': 'de740ef9-b40e-4497-8b3b-c137481c7633', 'crn': 'crn:altus:iam:us-west-1:558bc1d2-8867-4357-8524-311d51259233:user:de740ef9-b40e-4497-8b3b-c137481c7633', 'email': 'dchaffey@cloudera.com', 'firstName': 'Daniel', 'lastName': 'Chaffelson', 'creationDate': datetime.datetime(2019, 11, 4, 11, 54, 27, 581000, tzinfo=tzutc()), 'accountAdmin': False, 'identityProviderCrn': 'crn:altus:iam:us-west-1:558bc1d2-8867-4357-8524-311d51259233:samlProvider:cloudera-okta-production/a0afd6e3-ffc1-48bd-953a-60003d82f8ae', 'lastInteractiveLogin': datetime.datetime(2020, 12, 1, 11, 32, 38, 901000, tzinfo=tzutc()), 'workloadUsername': 'dchaffey'}

Typical function calls in CDP CLI are shadowed within the relevant Classes when wrapped by cdpy, or may be called directly using the call function shown further down.

Basic function call with target that does not exist (404) ::

    from cdpy.cdpy import Cdpy
    Cdpy().iam.get_user('fakeusername')
    >> UserWarning:CDP User could not be retrieved
    > None

This Class function has been wrapped to automatically handle the NOT_FOUND error generated when requesting an unknown user, note the use of self.sdk.call within the Class ::

    def get_user(self, name=None):
        return self.sdk.call(
            # Describe base function calls
            svc='iam',  # Name of the client service
            func='get_user',  # Name of the Function within the service to call
            ret_field='user',  # Optional child field to return, often CDP CLI responses are wrapped like this
            squelch=[  # List of below Client Error Handlers
                # Describe any Client Error responses using the provided Squelch class
                Squelch(
                    field='error_code',  # CdpError Field to test
                    value='NOT_FOUND',  # String to check for in Field
                    warning='CDP User could not be retrieved',  # Warning to throw if encountered
                    default=None  # Value to return instead of Error
                )
            ],
            # Include any keyword args that may be used in the function call, None/'' args will be ignored
            userId=name  # As name is None by default, it will be ignored unless provided
        )

Basic function call with invalid value ::

    from cdpy.cdpy import Cdpy
    Cdpy().iam.get_user('')
    >> UserWarning:Removing empty string arg userId from submission
    > {'userId': 'de740ef9-b40e-4497-8b3b-c137481c7633', 'crn': 'crn:altus:iam:us-west-1:558bc1d2-8867-4357-8524-311d51259233:user:de740ef9-b40e-4497-8b3b-c137481c7633', 'email': 'dchaffey@cloudera.com', 'firstName': 'Daniel', 'lastName': 'Chaffelson', 'creationDate': datetime.datetime(2019, 11, 4, 11, 54, 27, 581000, tzinfo=tzutc()), 'accountAdmin': False, 'identityProviderCrn': 'crn:altus:iam:us-west-1:558bc1d2-8867-4357-8524-311d51259233:samlProvider:cloudera-okta-production/a0afd6e3-ffc1-48bd-953a-60003d82f8ae', 'lastInteractiveLogin': datetime.datetime(2020, 12, 1, 11, 32, 38, 901000, tzinfo=tzutc()), 'workloadUsername': 'dchaffey'}

Here the invalid Parameter for for typical Ansible parameter `name` is mapped to `userId` within CDPCLI and is scrubbed from the submission ( may not submit zero length strings), a user warning is issued.
The command is run by default with that Parameter removed, this triggering the 'list all' logic of the underlying call.

This behavior may be bypassed using the `scrub_inputs` switch and thus raise a native Python Error ::

    Cdpy(scrub_inputs=False).iam.get_user('')
    Traceback (most recent call last):
    ...

More Complex Usage
------------------

Call Wrapper execution directly for arbitrary CDP Service and Function with arbitrary keyword param. This `call` function is the same that is exposed in the more abstract Classes in earlier examples, this is the more direct usage method allowing developers to work at varying levels within the layered abstractions with relative ease ::

    from cdpy.common import CdpcliWrapper

    CdpcliWrapper().call(svc='iam', func='set_workload_password_policy', maxPasswordLifetimeDays=lifetime)

Define function to call wrapped method with prebuild payload and use custom error handling logic.
Note that the `env_config` arguments are unpacked and passed to the CDP API directly, allowing developers to work with additional parameters without waiting for the higher level abstractions to support them
This example also demonstrates bypassing the Squelch error handler to implement custom error handling logic, and the ability to revert to the provided `throw_error` capabilities (which are superable) ::

    from cdpy.common import CdpcliWrapper
    from cdpy.common import CdpError

    wrap = CdpcliWrapper()
    env_config = dict(valueOne=value, valueTwo=value)

    resp = wrap.call(
        svc='environments', func='create_aws_environment', ret_field='environment', ret_error=True,
        **env_config
    )
    if isinstance(resp, CdpError):
        if resp.error_code == 'INVALID_ARGUMENT':
            if 'constraintViolations' not in str(resp.violations):
                resp.update(message="Received violation warning:\n%s" % self.sdk.dumps(str(resp.violations)))
                self.sdk.throw_warning(resp)
        self.sdk.throw_error(resp)
    return resp

Declare custom error handling function and instantiate with it. This abstraction is specifically to allow developers to replace native Python error handling with framework specific handling, such as the typical Ansible module `fail_json` seen here ::

    from cdpy.common import CdpError

    class CdpModule(object)
        def _cdp_module_throw_error(self, error: 'CdpError'):
            """Wraps throwing Errors when used as Ansible module"""
            self.module.fail_json(msg=str(error.__dict__))

        self.sdk = Cdpy(error_handler=self._cdp_module_throw_error)

Ideally for extensive development you would make use of the metaclass Cdpy, this is currently used as the basis for the Cloudera CDP Public Cloud Ansible Collection ::

    from cdpy.cdpy import Cdpy
    client = Cdpy(debug=self.debug, tls_verify=self.tls, strict_errors=self.strict, error_handler=self._cdp_module_throw_error, warning_handler=self._cdp_module_throw_warning)
    client.sdk.call(...)
    client.sdk.iam.(...)
    client.sdk.TERMINATION_STATES
    etc.


Development
=====================

Contributing
------------

Please create a feature branch from the current development Branch then submit a PR referencing an Issue for discussion.

Please note that we require signed commits inline with Developer Certificate of Origin best-practices for Open Source Collaboration.

PyScaffold Note
===============

This project has been set up using PyScaffold 4. For details and usage
information on PyScaffold see https://pyscaffold.org/.
