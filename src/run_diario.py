# src/run_diario.py
from __future__ import annotations

from .context import build_diario_context
from . import legacy


def run_diario(
    *,
    uf: str,
    data: str,  # YYYY-MM-DD
    pdf_path: str,
    spreadsheet_url_or_id: str,
    numero: str | None = None,
    tipo: str = "DL",
    clear_first: bool = False,
):
    """
    Orquestrador oficial do projeto.

    - Constrói o contexto
    - Executa o pipeline legado encapsulado
    """
    ctx = build_diario_context(
        uf=uf,
        data=data,
        numero=numero,
        tipo=tipo,
        source="local",
        pdf_path=pdf_path,
    )

    return legacy.run(
        ctx,
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        clear_first=clear_first,
    )
