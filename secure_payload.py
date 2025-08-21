import gzip
import json
import base64
from io import BytesIO
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
import os
from fastapi.responses import JSONResponse

# Secret Key (Must be 32 bytes for AES-256)
SECRET_KEY = b"mysecretpassword120123456"  # 32-byte key

def encrypt_data(data: bytes, key: bytes, iv: bytes) -> bytes:
    """Encrypt data using AES CBC mode."""
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    encryptor = cipher.encryptor()

    # PKCS7 Padding
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data) + padder.finalize()

    return iv + encryptor.update(padded_data) + encryptor.finalize()


def gzip_and_encrypt(data: dict) -> str:
    """Compress data using gzip, encrypt using AES, and return Base64-encoded string."""
    json_data = json.dumps(data).encode("utf-8")

    # Gzip Compress
    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as f:
        f.write(json_data)
    compressed_data = buffer.getvalue()

    # Generate a new IV for each response
    iv = os.urandom(16)

    # Encrypt Data
    encrypted_data = encrypt_data(compressed_data, SECRET_KEY, iv)

    # Encode to Base64
    base64_encoded_data = base64.b64encode(encrypted_data).decode("utf-8")

    return base64_encoded_data

def decrypt_data(encrypted_data: bytes, key: bytes) -> bytes:
    """Decrypt AES encrypted data."""
    iv = encrypted_data[:16]  # Extract IV
    encrypted_content = encrypted_data[16:]

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    decrypted_padded_data = decryptor.update(encrypted_content) + decryptor.finalize()

    # Remove PKCS7 Padding
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    return unpadder.update(decrypted_padded_data) + unpadder.finalize()


def decrypt_and_decompress(response_json: dict) -> dict:
    """Decrypt and decompress the response data."""
    encrypted_base64 = response_json["encrypted_data"]
    encrypted_bytes = base64.b64decode(encrypted_base64)

    decrypted_data = decrypt_data(encrypted_bytes, SECRET_KEY)
    print(f"{ decrypted_data = }")
    # Decompress Gzip
    with gzip.GzipFile(fileobj=BytesIO(decrypted_data), mode="rb") as f:
        decompressed_data = f.read()

    return json.loads(decompressed_data)


def return_encoded_data(data):
    # print(f"encoding ===>>> {data}")
    encrypted_data = gzip_and_encrypt(data)
    return JSONResponse(content={"encode_data": encrypted_data})

def return_decoded_data(data):
    data = {"encrypted_data": data}
    decrypted_json = decrypt_and_decompress(data)
    return JSONResponse(decrypted_json)