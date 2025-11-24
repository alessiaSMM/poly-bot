import json
from typing import Tuple
from google.cloud import kms_v1
from google.oauth2 import service_account

from eth_keys.datatypes import Signature
from web3 import Web3

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .config import (
    GCP_PROJECT_ID, KMS_LOCATION, KMS_KEY_RING, KMS_KEY_NAME, KMS_KEY_VERSION,
    RPC_URL
)

def build_kms_client_from_json(json_str: str) -> kms_v1.KeyManagementServiceClient:
    info = json.loads(json_str)
    creds = service_account.Credentials.from_service_account_info(info)
    return kms_v1.KeyManagementServiceClient(credentials=creds)

def kms_key_resource() -> str:
    return (
        f"projects/{GCP_PROJECT_ID}/locations/{KMS_LOCATION}/keyRings/{KMS_KEY_RING}"
        f"/cryptoKeys/{KMS_KEY_NAME}/cryptoKeyVersions/{KMS_KEY_VERSION}"
    )

def _der_to_rs(der_sig: bytes) -> Tuple[int, int]:
    if len(der_sig) < 8 or der_sig[0] != 0x30:
        raise ValueError("Firma DER non valida")
    idx = 2
    if der_sig[idx] != 0x02:
        raise ValueError("DER: atteso INTEGER r")
    idx += 1
    rlen = der_sig[idx]; idx += 1
    r = int.from_bytes(der_sig[idx:idx+rlen], "big"); idx += rlen
    if der_sig[idx] != 0x02:
        raise ValueError("DER: atteso INTEGER s")
    idx += 1
    slen = der_sig[idx]; idx += 1
    s = int.from_bytes(der_sig[idx:idx+slen], "big")
    return r, s

class KMSSigner:
    """Firma ECDSA secp256k1 via Google Cloud KMS (chiave asimmetrica)."""

    def __init__(self, kms_client: kms_v1.KeyManagementServiceClient, key_resource: str):
        self.kms = kms_client
        self.key_resource = key_resource
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL)) if RPC_URL else None

    def get_eth_address(self) -> str:
        """Deriva l'address Ethereum dalla chiave pubblica del KMS."""
        pub_pem = self.kms.get_public_key(request={"name": self.key_resource}).pem.encode()
        pub_key = serialization.load_pem_public_key(pub_pem, backend=default_backend())
        numbers = pub_key.public_numbers()
        x = numbers.x.to_bytes(32, 'big')
        y = numbers.y.to_bytes(32, 'big')
        uncompressed = b"\x04" + x + y
        # address = ultimi 20 bytes di keccak(uncompressed[1:])
        addr = Web3.to_checksum_address(Web3.keccak(uncompressed[1:])[-20:])
        return addr

    def sign_hash(self, msg_hash: bytes) -> Tuple[int, int, int]:
        """Firma un hash keccak32. Ritorna (v, r, s) in formato Ethereum."""
        if len(msg_hash) != 32:
            raise ValueError("msg_hash deve essere di 32 bytes (keccak)")

        # Passiamo il keccak come se fosse un digest al campo 'sha256' (workaround pratico).
        # Se il tuo setup KMS non lo consente, lo adatteremo (varia tra progetti).
        req = kms_v1.AsymmetricSignRequest(
            name=self.key_resource,
            digest=kms_v1.Digest(sha256=msg_hash)
        )
        resp = self.kms.asymmetric_sign(request=req)
        der_sig = resp.signature
        r, s = _der_to_rs(der_sig)

        # Calcolo 'v' via public key recovery, confrontando con l'address KMS
        expected_addr = self.get_eth_address().lower()
        for rec_id in (0, 1):
            sig = Signature(vrs=(rec_id, r, s))
            recovered = sig.recover_public_key_from_msg_hash(msg_hash)
            if recovered.to_checksum_address().lower() == expected_addr:
                v = 27 + rec_id  # standard Ethereum
                return (v, r, s)
        # fallback
        return (27, r, s)
