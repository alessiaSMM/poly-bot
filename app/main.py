import os
from eth_utils import keccak
from .kms_signer import KMSSigner, build_kms_client_from_json, kms_key_resource

def main():
    # Render: incolla l'intero JSON del Service Account in GCP_SA_JSON (Environment)
    sa_json = os.getenv("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Manca la variabile d'ambiente GCP_SA_JSON con il contenuto JSON del Service Account")

    # Inizializza KMS e firma di test
    kms_client = build_kms_client_from_json(sa_json)
    signer = KMSSigner(kms_client, kms_key_resource())

    # 1) Address derivato dal KMS (serve per inviare fondi al wallet operativo)
    addr = signer.get_eth_address()
    print("Address Ethereum/Polygon derivato dal KMS:", addr)

    # 2) Firma di test (verifica pipeline)
    v, r, s = signer.sign_hash(keccak(b"poly-bot self-test"))
    print("Firma OK:", {"v": v, "r": hex(r), "s": hex(s)})

if __name__ == "__main__":
    main()
