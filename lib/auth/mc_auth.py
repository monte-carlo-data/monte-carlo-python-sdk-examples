import datetime
import subprocess
import configparser
import os
import boto3
import requests
import lib.helpers.constants as const
from pycognito import aws_srp
from botocore.exceptions import ClientError
from contextlib import nullcontext
from pathlib import Path
from pycarlo.core import Client, Session, Query, Mutation
from rich.prompt import Confirm, Prompt
from lib.helpers import sdk_helpers
from rich.progress import Progress
from lib.helpers.logs import LOGGER


class MCAuth(object):

    def __init__(self, configs: configparser.ConfigParser, profile: str = None, progress: Progress = None, validate: bool = False):

        self.profile = "default" if not profile else profile
        self.profile_file = os.path.expanduser("~/.mcd/profiles.ini")
        self.progress = progress
        self._configs = configs
        self._ini = self.__read_ini()

        if self._ini:
            if self._ini.has_section(self.profile):
                self.mcd_id_current = self._ini[self.profile].get('mcd_id')
                self._mcd_token_current = self._ini[self.profile].get('mcd_token')

                if not self.mcd_id_current or not self._mcd_token_current:
                    LOGGER.error("authentication id/token missing")
                    exit(1)

                self.client = Client(session=Session(mcd_id=self.mcd_id_current, mcd_token=self._mcd_token_current))
                if validate:
                    self.validate_cli()
            else:
                LOGGER.error(f"profile '{self.profile}' does not exist")
                exit(1)

    def __read_ini(self):
        """ """

        configs = None
        if Path(self.profile_file).is_file():
            configs = configparser.ConfigParser()
            configs.read(self.profile_file)

        return configs

    def validate_cli(self):

        LOGGER.info("checking montecarlo version...")
        proc = subprocess.run(["montecarlo", "--version"], capture_output=True, text=True)
        if proc.returncode != 0:
            LOGGER.info("montecarlo is not installed")
            exit(proc.returncode)
        else:
            LOGGER.info(f"montecarlo present")

        LOGGER.info("validating montecarlo connection...")
        proc = subprocess.run(
            ["montecarlo", "--profile", self.profile, "validate"], capture_output=True, text=True
        )
        if proc.returncode != 0:
            LOGGER.error("unable to validate token")
            self.__mc_create_token()
        else:
            LOGGER.info(f"validation complete")
            self.get_token_status()

    def get_token_status(self):
        """ """

        query = Query()
        get_token_metadata = query.get_token_metadata(index="user")
        get_token_metadata.__fields__("id", "expiration_time")
        res = self.client(query).get_token_metadata

        threshold = 7
        token_info = [token for token in res if token.id == self.mcd_id_current]
        token_expiration = token_info[0].expiration_time.astimezone(datetime.UTC) if len(token_info) > 0 else datetime.datetime.now(datetime.UTC)
        expires_in_seconds = (token_expiration - datetime.datetime.now(datetime.UTC)).total_seconds()

        # Ask user (threshold) days before expiration if the token should be regenerated
        if expires_in_seconds <= (86400 * threshold):
            with sdk_helpers.PauseProgress(self.progress) if self.progress else nullcontext():
                regenerate = Confirm.ask(f"The token associated with '{self.profile}' will expire in "
                                         f"{int(expires_in_seconds/3600)} hours. Do you want to create a new one?")
            if regenerate:
                self.delete_token(self.create_token())

    def create_token(self):
        """ """

        try:
            mcd_id_old = self.mcd_id_current
            mutation = Mutation()
            (mutation.create_access_token(comment="MC-SDK-Utils",
                                         expiration_in_days=int(self._configs['global']
                                                                .get('TOKEN_DURATION', "14")))
             .access_token.__fields__("id", "token"))
            client = Client(session=Session(mcd_id=self.mcd_id_current, mcd_token=self._mcd_token_current))
            res = client(mutation).create_access_token
            self.mcd_id_current = res.access_token.id
            self._mcd_token_current = res.access_token.token
            LOGGER.info("token created successfully")
            self.__store_token()
            self.client = Client(session=Session(mcd_id=self.mcd_id_current, mcd_token=self._mcd_token_current))
            return mcd_id_old
        except:
            LOGGER.error("unable to create token")
            exit(1)

    def delete_token(self, token_id: str):
        """ """

        try:
            mutation = Mutation()
            mutation.delete_access_token(token_id=token_id)
            client = Client(session=Session(mcd_id=self.mcd_id_current, mcd_token=self._mcd_token_current))
            _ = client(mutation).delete_access_token
            LOGGER.info("old token deleted successfully")
        except:
            LOGGER.error("unable to delete old token")
            exit(1)

    def __store_token(self):
        """ """

        try:
            self._ini.set(self.profile, 'mcd_id', self.mcd_id_current)
            self._ini.set(self.profile, 'mcd_token', self._mcd_token_current)
            with open(self.profile_file, 'w') as configfile:
                self._ini.write(configfile)
            LOGGER.info("token stored successfully")
        except Exception as e:
            LOGGER.error(f"unable to store token - {e}")
            exit(1)

    def __mc_create_token(self):
        """ """

        username = self._configs['global'].get('USERNAME')
        password = self._configs['global'].get('PASSWORD')

        if None in [username, password]:
            LOGGER.debug("USERNAME/PASSWORD missing in configuration file")
            with sdk_helpers.PauseProgress(self.progress) if self.progress else nullcontext():
                LOGGER.info("creating new token")
                username = Prompt.ask("[dodger_blue2]MC Username")
                password = Prompt.ask("[dodger_blue2]MC Password", password=True)

        bc = boto3.client("cognito-idp", "us-east-1")
        srp_helper = aws_srp.AWSSRP(
            username=username,
            password=password,
            pool_id=const.POOL_ID,
            client_id=const.CLIENT_ID,
            client_secret=None,
            client=bc
        )

        try:
            auth_tokens = srp_helper.authenticate_user()
        except ClientError:
            LOGGER.error("unable to authenticate user. Ensure username/password is correct")
            exit(1)

        headers = {"Authorization": f"Bearer {auth_tokens['AuthenticationResult']['IdToken']}"}
        payload = f"""mutation createAccessToken($comment: String!, $expirationInDays: Int!) {{
                       createAccessToken(expirationInDays: $expirationInDays, comment: $comment) {{
                         accessToken {{
                           id
                           token
                         }}
                       }}
                     }}"""
        variables = {"comment": "MC-SDK-Utils",
                     "expirationInDays": int(self._configs['global'].get('TOKEN_DURATION', "14"))}
        response = requests.post("https://graphql.getmontecarlo.com/graphql", verify=True,
                                 json={'query': payload, 'variables': variables}, headers=headers)
        res_json = response.json()
        self.mcd_id_current = res_json['data']['createAccessToken']['accessToken']['id']
        self._mcd_token_current = res_json['data']['createAccessToken']['accessToken']['token']
        self.__store_token()
        self.client = Client(session=Session(mcd_id=self.mcd_id_current, mcd_token=self._mcd_token_current))
