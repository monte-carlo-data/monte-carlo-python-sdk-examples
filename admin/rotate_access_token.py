#######################################################################################################################
#                                                        ABOUT                                                        #
#######################################################################################################################
# This script is intended to rotate an access token on an ongoing basis. The initial setup requires a manual creation #
# of the access token in the UI. After the execution, the token variables in the .env file will be encrypted.         #
#######################################################################################################################

#######################################################################################################################
#                                                    INSTRUCTIONS                                                     #
#######################################################################################################################
# 1. Store the mcd_id abd mcd_token variables in a .env file i.e.                                                     #
#   mcd_id=e18rgrdgscee545e7a80171c332452d0f                                                                          #
#   mcd_token=TWXcK1lPbJwsmY983rPqgP0xMUQEEv7iVbP8pEJruw8QPRts-BfsGFzs                                                #
# 2. The script will read the variables from the .env file and use them to generate a new access token. Expiration is #
# set to 1 day by default. Update variable 'expiration_in_days' in line 74 to modify the lifetime of the token        #
# 3. After the new token has been generated, the script will also delete the previous token.                          #
# 4. The new token and token id will be stored in the .env file in an encrypted format.                               #
#######################################################################################################################


import os
import re
from pycarlo.core import Client, Mutation, Session
from dotenv import load_dotenv
from pathlib import Path
from helpers.encryption import ConfigEncryption, RSA_, FERNET_


def write_vars(env_path: str, variables: dict):
    """Write token variables to .env file and encrypt it.

    Args:
        env_path (str): Path of the .env file.
        variables (dict): Dictionary containing the token id and token.

    """

    Path('/'.join(str(env_path).split('/')[:-1])).mkdir(parents=True, exist_ok=True)
    contents = f"""mcd_id={variables["mcd_id"]}
                   mcd_token={variables["mcd_token"]}"""
    encrypter = ConfigEncryption(RSA_, 'keys')
    encrypter.encrypt_to_file(plaintext_string=contents,
                              output_file=str(Path(env_path).resolve()))


def read_vars(env_path: str) -> dict:
    """Decrypt .env file if needed and extract token variables.

    Args:
        env_path (str): Path of the .env file.

    Returns:
        dict: Access token environment variables as dictionary.

    """

    env_items = os.environ.items()
    if not os.path.isfile(env_path):
        print(".env file missing")
        exit(1)
    env_payload = {}
    try:
        encryption_helper = ConfigEncryption(RSA_, 'keys')
        env = '\n'.join(
            [n for n in encryption_helper.decrypt_file(str(env_path)).split('\n') if not n.startswith('#')])
        variables = re.findall(r'([\w]+)=', env)
        values = re.findall(r'=(.*)', env)
        for i, key in enumerate(variables):
            env_payload[key] = values[i]
    except:
        ConfigEncryption(FERNET_, 'keys')
        load_dotenv(str(env_path))
        for k, v in env_items:
            env_payload[k] = v

    return env_payload


if __name__ == '__main__':

    ENV_FILE = '.env'
    token_vars = read_vars(ENV_FILE)
    mcd_id_current = token_vars.get("mcd_id", None)
    mcd_token_current = token_vars.get("mcd_token", None)

    if not mcd_id_current or not mcd_token_current:
        print("mcd_id/mcd_token values missing")
        exit(1)

    try:
        mutation = Mutation()
        mutation.create_access_token(comment="test", expiration_in_days=1).access_token.__fields__("id", "token")
        client = Client(session=Session(mcd_id=mcd_id_current, mcd_token=mcd_token_current))
        res = client(mutation).create_access_token
        mcd_id = res.access_token.id
        mcd_token = res.access_token.token
    except:
        print("Unable to create token")
        exit(1)

    try:
        mutation = Mutation()
        mutation.delete_access_token(token_id=mcd_id_current)
        client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
        res = client(mutation).delete_access_token
    except:
        print("Unable to delete token")
        exit(1)

    # Write new env vars to
    write_vars(ENV_FILE, {"mcd_id": mcd_id, "mcd_token": mcd_token})
    print("Token rotation completed successfully")



