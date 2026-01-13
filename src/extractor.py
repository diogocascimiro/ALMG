import re
import csv
from typing import Union
import pdfplumber

RE_PAGINA = re.compile(r"Página\s+(\d+)\s+de\s+\d+", re.IGNORECASE)

def pdf_para_csv(pdf_path: Union[str, "os.PathLike"], csv_path: Union[str, "os.PathLike"]) -> str:
    """
    Lê um PDF e gera um CSV com as colunas: pagina, texto
    """
    rows = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text() or ""
            m = RE_PAGINA.search(texto)
            pagina = int(m.group(1)) if m else ""

            rows.append({
                "pagina": pagina,
                "texto": texto.replace("\n", " ").strip()
            })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["pagina", "texto"])
        writer.writeheader()
        writer.writerows(rows)

    return str(csv_path)

