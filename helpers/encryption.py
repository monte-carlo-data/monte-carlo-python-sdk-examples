import zlib
import base64
import os
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from pathlib import Path
from cryptography.fernet import Fernet, InvalidToken

RSA_ = "rsa"
FERNET_ = "fernet"
PEM = 'PEM'
PUBLIC = 'public'
PRIVATE = 'private'
KEY = 'key'


class ConfigEncryption(object):
    """Custom class to Encrypt and Decrypt Script Configurations."""

    def __init__(self, enc_type: str, key_location: str = None):
        """Create an instance of ConfigEncryption.

        Args:
            enc_type (str): Encryption method to use.
            key_location (str): Path to the Encryption keys.

        """

        enc_types = [RSA_, FERNET_]
        if enc_type not in enc_types:
            raise ValueError("Invalid encryption method. Expected one of: %s" % enc_types)

        self.key_location = key_location

        if self.key_location:
            if enc_type == RSA_:
                self.key_location = {PUBLIC: f"{key_location}/public.pem",
                                     PRIVATE: f"{key_location}/private.pem"}
            else:
                self.key_location = {PUBLIC: f"{key_location}/.fernet.key"}
        else:
            if enc_type == 'rsa':
                self.key_location = {PUBLIC: '.secrets/public.pem',
                                     PRIVATE: '.secrets/private.pem'}
            else:
                self.key_location = {PUBLIC: '.secrets/.fernet.key'}

        try:
            file_sizes = []
            for keys in list(self.key_location.values()):
                file_sizes.append(os.path.getsize(keys))
        except OSError:
            file_sizes = [0]

        if enc_type == RSA_:
            if 0 in file_sizes:
                self._generate_rsa_keys()
            self.public_key, self.private_key = self._load_rsa_keys()
        else:
            if 0 in file_sizes:
                self._generate_fernet_key()
            self.key = self._load_fernet_key()

    def encrypt_string(self, plaintext_string: str) -> str:
        """Encrypt a plaintext string and return encoded encrypted bytes value.

        Args:
            plaintext_string (str): String to be encrypted.

        Returns:
            str: Encrypted string value.

        """

        f = Fernet(self.key)
        encoded_string = plaintext_string.encode()
        return f.encrypt(encoded_string).decode()

    def encrypt_to_file(self, input_file: str = None, plaintext_string: str = None, output_file: str = None) -> str:
        """Encrypt contents of file and save encrypted contents to new file.

        Args:
            input_file (str): Path to the file to be encrypted.
            plaintext_string (str): String to be encrypted.
            output_file (str): Path where the encrypted file will be generated.

        Returns:
            str: Location of encrypted file.

        """

        # Import the Public Key and use for encryption using PKCS1_OAEP
        rsa_key = RSA.importKey(self.public_key)
        rsa_key = PKCS1_OAEP.new(rsa_key)

        if input_file:
            content = Path(input_file).read_bytes()
        elif plaintext_string:
            content = plaintext_string.encode()
        else:
            raise ValueError("ERROR - Missing input_file or plaintext_string")

        if output_file:
            encrypted_file_path = Path(output_file)
        elif input_file:
            encrypted_file_path = Path(f"{'.'.join(input_file.split('.')[:-1])}_encrypted.{input_file.split('.')[-1]}")
        else:
            raise ValueError("ERROR - Missing output_file")

        # compress the data first
        blob = zlib.compress(content)
        # In determining the chunk size, determine the private key length used in bytes and
        # subtract 42 bytes (when using PKCS1_OAEP). The data will be in encrypted in chunks
        chunk_size = 470
        offset = 0
        end_loop = False
        encrypted = bytearray()

        while not end_loop:
            chunk = blob[offset:offset + chunk_size]
            # If the data chunk is less than the chunk size, then we need to add padding with " ".
            # This indicates that we reached the end of the file, so we end loop here
            if len(chunk) % chunk_size != 0:
                end_loop = True
                chunk += bytes(chunk_size - len(chunk))
            # Append the encrypted chunk to the overall encrypted file
            encrypted += rsa_key.encrypt(chunk)
            # Increase the offset by chunk size
            offset += chunk_size

        # Base 64 encode the encrypted file
        Path('/'.join(str(encrypted_file_path).split('/')[:-1])).mkdir(parents=True, exist_ok=True)
        encrypted_file_path.touch(mode=0o600)
        encrypted_file_path.write_bytes(base64.b64encode(encrypted))

        return str(encrypted_file_path)

    def decrypt_string(self, encrypted_value: str) -> str:
        """Decrypt an encoded bytes value and return plaintext string.

        Args:
            encrypted_value (str): Encoded encrypted bytes value.

        Returns:
            str: Decoded Plaintext String.

        """

        if not hasattr(self, KEY):
            return encrypted_value

        f = Fernet(self.key)
        try:
            decrypted_string = f.decrypt(encrypted_value.encode())
            return decrypted_string.decode()
        except InvalidToken:
            return encrypted_value

    def decrypt_file(self, file: str) -> str:
        """Decrypt an encoded bytes value and return plaintext string.

        Args:
            file (str): Path to Encoded encrypted file.

        Returns:
            str: Decoded Plaintext String.

        """

        # Import the Private Key and use for decryption using PKCS1_OAEP
        rsa_key = RSA.importKey(self.private_key)
        rsa_key = PKCS1_OAEP.new(rsa_key)

        try:
            encrypted_blob = Path(file).read_bytes()
            # Base 64 decode the data
            encrypted_blob = base64.b64decode(encrypted_blob)
        except ValueError:
            raise ValueError("Unable to decode")

        # In determining the chunk size, determine the private key length used in bytes.
        # The data will be in decrypted in chunks
        chunk_size = 512
        offset = 0
        decrypted = bytearray()

        # keep loop going as long as we have chunks to decrypt
        while offset < len(encrypted_blob):
            # The chunk
            chunk = encrypted_blob[offset: offset + chunk_size]

            # Append the decrypted chunk to the overall decrypted file
            decrypted += rsa_key.decrypt(chunk)

            # Increase the offset by chunk size
            offset += chunk_size

        # return the decompressed decrypted data
        return zlib.decompress(decrypted).decode()

    def _generate_rsa_keys(self):
        """Generate RSA Encryption Keys and save them into pem format files."""

        # Generate a public/ private key pair using 4096 bits key length (512 bytes)
        new_key = RSA.generate(4096, e=65537)

        # The private key in PEM format
        private_key = new_key.exportKey(PEM)

        # The public key in PEM Format
        public_key = new_key.publickey().exportKey(PEM)

        # Create directories where keys will be stored
        if not os.path.exists(self.key_location[PUBLIC].split('/')[0]):
            os.mkdir(self.key_location[PUBLIC].split('/')[0])
        if not os.path.exists(self.key_location[PRIVATE].split('/')[0]):
            os.mkdir(self.key_location[PRIVATE].split('/')[0])

        private_key_path = Path(self.key_location[PRIVATE])
        private_key_path.touch(mode=0o600)
        private_key_path.write_bytes(private_key)

        public_key_path = Path(self.key_location[PUBLIC])
        public_key_path.touch(mode=0o664)
        public_key_path.write_bytes(public_key)

    def _generate_fernet_key(self):
        """Generate an Encryption Key and save it into a file."""

        key = Fernet.generate_key()
        if not os.path.exists(self.key_location[PUBLIC].split('/')[0]):
            os.mkdir(self.key_location[PUBLIC].split('/')[0])
        with open(self.key_location[PUBLIC], "wb") as key_file:
            key_file.write(key)

    # noinspection SpellCheckingInspection
    def _load_rsa_keys(self) -> tuple:
        """Load the Encryption Keys from the saved files.

        Returns:
            tuple: Public and Private encryption keys.

        """

        public_key = Path(self.key_location[PUBLIC]).read_bytes()
        private_key = Path(self.key_location[PRIVATE]).read_bytes()

        return public_key, private_key

    def _load_fernet_key(self) -> str:
        """Load the Encryption Key from the saved file.

        Returns:
            str: Fernet encryption key.

        """
        with open(self.key_location[PUBLIC], 'r') as key_file:
            return key_file.read()
