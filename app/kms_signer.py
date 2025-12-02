import json
from typing import Tuple

from google.cloud import kms_v1
from google.oauth2 import service_account

from eth_keys.datatypes import Signature
from web3 import Web3

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .config import (
    PROJECT_ID,
    LOCATION_ID,
    KEYRING_ID,
    KEY_ID,
    KEY_VERSION,
    GOOGLE_APPLICATION_CREDENTIALS,
    RPC_URL,
)


def build_kms_client_from_json(_: str = "") -> kms_v1.KeyManagementServiceClient:
    """
    Versione compatibile col vecchio codice:
    - ignora il parametro json_str
    - usa il file indicato da GOOGLE_APPLICATION_CREDENTIALS (.env)
    """
    if not GOOGLE_APPLICATION_CREDENTIALS:
        # Usa ADC se non è impostato il path esplicito (non dovrebbe capitare nel tuo setup)
        return kms_v1.KeyManagementServiceClient()


    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS
    )
    return kms_v1.KeyManagementServiceClient(credentials=creds)


def kms_key_resource() -> str:
    """
    Ritorna il resource name completo della chiave KMS:
    projects/.../locations/.../keyRings/.../cryptoKeys/.../cryptoKeyVersions/...
    """
    return (
        f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/keyRings/{KEYRING_ID}"
        f"/cryptoKeys/{KEY_ID}/cryptoKeyVersions/{KEY_VERSION}"
    )



def _der_to_rs(der_sig: bytes) -> Tuple[int, int]:
    """
    Converte una firma DER (ASN.1) nei due interi (r, s).
    Implementazione minimale, sufficiente per le firme KMS.
    """
    if len(der_sig) < 8 or der_sig[0] != 0x30:
        raise ValueError("Firma DER non valida")
    idx = 2
    if der_sig[idx] != 0x02:
        raise ValueError("DER: atteso INTEGER r")
    idx += 1
    rlen = der_sig[idx]
    idx += 1
    r = int.from_bytes(der_sig[idx : idx + rlen], "big")
    idx += rlen
    if der_sig[idx] != 0x02:
        raise ValueError("DER: atteso INTEGER s")
    idx += 1
    slen = der_sig[idx]
    idx += 1
    s = int.from_bytes(der_sig[idx : idx + slen], "big")
    return r, s


class KMSSigner:
    """
    Firma ECDSA secp256k1 via Google Cloud KMS (chiave asimmetrica).

    - Usa la chiave definita in config (.env).
    - Deriva l'address Ethereum dalla chiave pubblica KMS.
    - Firma un hash (32 bytes) e restituisce (v, r, s) formato Ethereum.
    """

    def __init__(self, kms_client: kms_v1.KeyManagementServiceClient, key_resource: str):
        self.kms = kms_client
        self.key_resource = key_resource
        self.w3 = Web3(Web3.HTTPProvider(RPC_URL)) if RPC_URL else None

    def get_eth_address(self) -> str:
        """
        Deriva l'indirizzo Ethereum dalla chiave pubblica del KMS.
        """
        pub = self.kms.get_public_key(request={"name": self.key_resource})
        pub_pem = pub.pem.encode()

        pub_key = serialization.load_pem_public_key(pub_pem, backend=default_backend())
        numbers = pub_key.public_numbers()
        x = numbers.x.to_bytes(32, "big")
        y = numbers.y.to_bytes(32, "big")

        # Formato uncompressed: 0x04 || X || Y
        uncompressed = b"\x04" + x + y

        # Ethereum address = ultimi 20 bytes di keccak(uncompressed[1:])
        addr_bytes = Web3.keccak(uncompressed[1:])[-20:]
        addr = Web3.to_checksum_address(addr_bytes)
        return addr

    def sign_hash(self, msg_hash: bytes) -> Tuple[int, int, int]:
        """
        Firma un hash di 32 bytes (es. keccak del messaggio).
        Ritorna (v, r, s) in formato Ethereum.
        """
        if len(msg_hash) != 32:
            raise ValueError("msg_hash deve essere di 32 bytes (keccak o simile)")

         # ATTENZIONE:
        # La API KMS prevede un campo 'sha256' ma NON ricalcola l'hash:
        # usa semplicemente i bytes che passiamo. Quindi passiamo msg_hash
        # come se fosse il "digest SHA-256", anche se in realtà è keccak.
        req = kms_v1.AsymmetricSignRequest(
            name=self.key_resource,
            digest=kms_v1.Digest(sha256=msg_hash),
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

        # Se per qualche motivo il recovery non trova match, fallback neutro
        return (27, r, s)

