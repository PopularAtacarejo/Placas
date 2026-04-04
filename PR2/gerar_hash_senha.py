#!/usr/bin/env python
"""
Gera hash de senha no formato PBKDF2-SHA256 usado no usuarios.json remoto.
"""

from __future__ import annotations

import base64
import getpass
import hashlib
import os
import sys

PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 390000
PASSWORD_HASH_MIN_ITERATIONS = 150000


def _b64_urlsafe_sem_padding(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def gerar_hash_senha(senha_plana: str, iterations: int = PASSWORD_HASH_ITERATIONS) -> str:
    senha = str(senha_plana or "")
    if not senha:
        raise ValueError("Senha vazia nao pode ser criptografada.")
    iter_count = int(iterations)
    if iter_count < PASSWORD_HASH_MIN_ITERATIONS:
        raise ValueError(f"Iteracoes insuficientes ({iter_count}).")
    salt_bytes = os.urandom(16)
    salt = _b64_urlsafe_sem_padding(salt_bytes)
    derivado = hashlib.pbkdf2_hmac("sha256", senha.encode("utf-8"), salt_bytes, iter_count)
    assinatura = _b64_urlsafe_sem_padding(derivado)
    return f"{PASSWORD_HASH_SCHEME}${iter_count}${salt}${assinatura}"


def main() -> int:
    senha_1 = getpass.getpass("Digite a senha: ")
    senha_2 = getpass.getpass("Confirme a senha: ")
    if senha_1 != senha_2:
        print("Erro: as senhas nao conferem.", file=sys.stderr)
        return 2
    try:
        senha_hash = gerar_hash_senha(senha_1)
    except Exception as exc:
        print(f"Erro ao gerar hash: {exc}", file=sys.stderr)
        return 2
    print("senha_hash:")
    print(senha_hash)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
