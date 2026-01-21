from __future__ import annotations

import csv
import re
from typing import Union

import pdfplumber

from core.context import DiarioContext

RE_PAGINA = re.compile(r"Página\s+(\d+)\s+de\s+\d+", re.IGNORECASE)


def pdf_para_csv(ctx: DiarioContext, csv_path: Union[str, "os.PathLike"]) -> str:
    """
    Lê o PDF indicado em ctx.pdf_path e gera um CSV com as colunas: pagina, texto.

    Comportamento preservado:
    - Mesmo regex de "Página X de Y"
    - Mesmo flatten de quebras de linha para espaço
    - Mesmo schema do CSV
    """
    rows = []
    pdf_path = ctx.pdf_path

    with pdfplumber.open(pdf_path) as pdf:
        # metadados úteis para diagnóstico sem afetar a lógica
        ctx.raw_text_meta["page_count"] = len(pdf.pages)

        for i, page in enumerate(pdf.pages, start=1):
            texto = page.extract_text() or ""
            m = RE_PAGINA.search(texto)
            pagina = int(m.group(1)) if m else ""

            rows.append({
                "pagina": pagina,
                "texto": texto.replace("\n", " ").strip()
            })

            # diagnóstico leve, opcional e barato
            if m is None:
                ctx.diagnostics.setdefault("pages_without_pagina_marker", []).append(i)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["pagina", "texto"])
        writer.writeheader()
        writer.writerows(rows)

    return str(csv_path)
