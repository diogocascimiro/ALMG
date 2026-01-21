from src.context import build_diario_context
import src.legacy as legacy


def run_diario(
    *,
    uf: str,
    data: str,
    pdf_path: str,
    numero: str | None = None,
    tipo: str = "DL",
):
    ctx = build_diario_context(
        uf=uf,
        data=data,
        numero=numero,
        tipo=tipo,
        source="local",
        pdf_path=pdf_path,
    )

    # Chamada legacy (baseline)
    legacy.run(ctx)  # ou o nome real da função principal

    return ctx

