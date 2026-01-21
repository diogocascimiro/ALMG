from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DiarioContext:
    # Identidade
    diario_key: str
    uf: str
    data: str                  # YYYY-MM-DD
    numero: Optional[str]
    tipo: str                  # ex.: "DL"

    # Entrada
    source: str                # url | local | upload
    pdf_path: str

    # Extração/diagnóstico
    raw_text_meta: Dict[str, Any] = field(default_factory=dict)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


def build_diario_key(*, uf: str, data: str, numero: Optional[str], tipo: str) -> str:
    """
    Chave canônica do Diário. Mantém humano-legível e determinística.
    Formato: <UF>|<YYYY-MM-DD>|<numero>|<tipo>
    """
    numero_norm = (numero or "").strip()
    return f"{uf.strip().upper()}|{data.strip()}|{numero_norm}|{tipo.strip().upper()}"


def build_diario_context(
    *,
    uf: str,
    data: str,
    numero: Optional[str] = None,
    tipo: str = "DL",
    source: str = "local",
    pdf_path: str,
) -> DiarioContext:
    diario_key = build_diario_key(uf=uf, data=data, numero=numero, tipo=tipo)
    return DiarioContext(
        diario_key=diario_key,
        uf=uf.strip().upper(),
        data=data.strip(),
        numero=(numero.strip() if isinstance(numero, str) else numero),
        tipo=tipo.strip().upper(),
        source=source.strip(),
        pdf_path=str(pdf_path),
    )
