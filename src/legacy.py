# src/legacy.py
from __future__ import annotations

import re
import csv
import os
import unicodedata
from pathlib import Path

from pypdf import PdfReader

from .context import DiarioContext


# ---- 1) Regex base ----
RE_PAG = re.compile(r"\bP[ÁA]GINA\s+(\d{1,4})\b", re.IGNORECASE)

URL_BASE = "https://diariolegislativo.almg.gov.br"


# ---- 3) Extração e detecção de títulos ----
def limpa_linha(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s).strip()
    return s


def primeira_pagina_num(linhas: list[str], fallback: int) -> int:
    for ln in linhas[:220]:
        m = RE_PAG.search(ln)
        if m:
            return int(m.group(1))
    return fallback


def compact_key(s: str) -> str:
    u = s.upper()
    u = unicodedata.normalize("NFD", u)
    u = "".join(ch for ch in u if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^0-9A-Z]", "", u)


# ---- TOP detection (robusta) ----
RE_HEADER_LIXO = re.compile(
    r"(DI[ÁA]RIO\s+DO\s+LEGISLATIVO|www\.almg\.gov\.br|"
    r"Segunda-feira|Ter[aç]a-feira|Quarta-feira|Quinta-feira|Sexta-feira|S[aá]bado|Domingo|"
    r"\bP[ÁA]GINA\s+\d+\b)",
    re.IGNORECASE
)


def _linha_relevante(s: str) -> bool:
    s = limpa_linha(s)
    if not s:
        return False
    if RE_HEADER_LIXO.search(s):
        return False
    if re.fullmatch(r"[-–—_•\.\s]+", s):
        return False
    return bool(re.search(r"[A-Za-zÀ-ÿ0-9]", s))


def is_top_event(line_idx: int, linhas: list[str]) -> bool:
    for prev in linhas[:line_idx]:
        if _linha_relevante(prev):
            return False
    return True


# ---- helper: matching por janela (1–3 linhas) ----
def win_keys(linhas: list[str], i: int, w: int) -> str:
    parts = []
    for k in range(w):
        j = i + k
        if j < len(linhas):
            parts.append(compact_key(linhas[j]))
    return "".join(parts)


def win_any_in(linhas: list[str], i: int, keys: set[str]) -> bool:
    k1 = win_keys(linhas, i, 1)
    k2 = win_keys(linhas, i, 2)
    k3 = win_keys(linhas, i, 3)
    return (k1 in keys) or (k2 in keys) or (k3 in keys)


# Estruturais / contexto
C_TRAMITACAO = "TRAMITACAODEPROPOSICOES"
C_RECEBIMENTO = "RECEBIMENTODEPROPOSICOES"
C_APRESENTACAO = "APRESENTACAODEPROPOSICOES"

# CUTs de verdade (não entram no CSV)
C_ATA = "ATA"
C_ATAS = "ATAS"
C_MATERIA_ADM = "MATERIAADMINISTRATIVA"
C_QUESTAO_ORDEM = "QUESTAODEORDEM"
CUT_KEYS = {C_ATA, C_ATAS, C_MATERIA_ADM, C_QUESTAO_ORDEM}

# Contextual CORRESPONDÊNCIA: OFÍCIOS
C_CORRESP_CAB = "CORRESPONDENCIADESPACHADAPELO1SECRETARIO"
C_OFICIOS = "OFICIOS"

# OUTs “simples” (match por linha)
C_MANIFESTACAO = "MANIFESTACAO"
C_MANIFESTACOES = "MANIFESTACOES"
MANIF_KEYS = {C_MANIFESTACAO, C_MANIFESTACOES}

C_REQ_APROV = "REQUERIMENTOAPROVADO"
C_REQS_APROV = "REQUERIMENTOSAPROVADOS"
REQ_APROV_KEYS = {C_REQ_APROV, C_REQS_APROV}

C_PROPOSICOES_DE_LEI = "PROPOSICOESDELEI"
C_RESOLUCAO = "RESOLUCAO"
C_ERRATA = "ERRATA"
C_ERRATAS = "ERRATAS"
ERRATA_KEYS = {C_ERRATA, C_ERRATAS}

C_RECEB_EMENDAS_SUBST = "RECEBIMENTODEEMENDASESUBSTITUTIVO"
C_RECEB_EMENDAS_SUBSTS = "RECEBIMENTODEEMENDASESUBSTITUTIVOS"
C_RECEB_EMENDA = "RECEBIMENTODEEMENDA"
EMENDAS_KEYS = {C_RECEB_EMENDAS_SUBST, C_RECEB_EMENDAS_SUBSTS, C_RECEB_EMENDA}

# Novos OUTs
C_LEITURA_COMUNICACOES = "LEITURADECOMUNICACOES"
C_DESPACHO_REQUERIMENTOS = "DESPACHODEREQUERIMENTOS"
C_DECISAO_PRESIDENCIA = "DECISAODAPRESIDENCIA"
C_ACORDO_LIDERES = "ACORDODELIDERES"
C_COMUNIC_PRESIDENCIA = "COMUNICACAODAPRESIDENCIA"
C_PROPOSICOES_NAO_RECEBIDAS = "PROPOSICOESNAORECEBIDAS"

# APRESENTAÇÃO: gatilhos materiais
C_REQUERIMENTOS = "REQUERIMENTOS"
C_PROJETO_DE_LEI = "PROJETODELEI"
C_PROJETOS_DE_LEI = "PROJETOSDELEI"


def prefix_tramitacao(label: str, in_tramitacao: bool) -> str:
    if in_tramitacao:
        return f"TRAMITAÇÃO DE PROPOSIÇÕES: {label}"
    return label


def label_apresentacao(tipo_bloco: str, in_tramitacao: bool) -> str:
    if tipo_bloco == "PL":
        base = "APRESENTAÇÃO DE PROPOSIÇÕES: PROJETOS DE LEI"
    else:
        base = "APRESENTAÇÃO DE PROPOSIÇÕES: REQUERIMENTOS"
    return prefix_tramitacao(base, in_tramitacao)


# =========================================================
# ====================== FUNÇÃO ===========================
# =========================================================

def run(
    ctx: DiarioContext,
    *,
    spreadsheet_url_or_id: str,
    clear_first: bool = False,
):
    """
    Pipeline legado encapsulado.
    """
    pdf_path = str(ctx.pdf_path)
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF não encontrado: {pdf_path}")

    # compatibilidade com seu writer (usa YYYYMMDD)
    yyyy, mm, dd = ctx.data.split("-")
    yyyymmdd = f"{yyyy}{mm}{dd}"

    reader = PdfReader(pdf_path)

    # eventos: (pag, ordem, tipo, label_out, fim_sobreposto, top_flag)
    eventos = []
    ordem = 0

    # estados
    in_tramitacao = False
    sub_tramitacao = None          # None | C_RECEBIMENTO | C_APRESENTACAO (quando dentro de TRAMITAÇÃO)
    apresentacao_ativa = False     # True se estamos em APRESENTAÇÃO (com ou sem TRAMITAÇÃO)
    sub_apresentacao = None        # None | "PL" | "REQ"
    viu_corresp_cab = False

    pegou_leis = False
    MAX_PAG_LEIS = 40

    for i, page in enumerate(reader.pages):
        texto = page.extract_text() or ""
        linhas = [limpa_linha(x) for x in texto.splitlines() if limpa_linha(x)]
        pag_num = primeira_pagina_num(linhas, i + 1)

        for li, ln in enumerate(linhas):
            ln_up = ln.upper().strip()
            c = compact_key(ln)
            top_flag = is_top_event(li, linhas)

            # janela compactada (p/ títulos quebrados)
            k1 = win_keys(linhas, li, 1)
            k2 = win_keys(linhas, li, 2)
            k3 = win_keys(linhas, li, 3)

            # ---------------------------
            # CUTs “reais”
            # ---------------------------
            if c in CUT_KEYS:
                ordem += 1
                eventos.append((pag_num, ordem, "CUT", None, False, top_flag))
                # encerra contextos
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            if c.startswith("PARECER"):
                ordem += 1
                eventos.append((pag_num, ordem, "CUT", None, False, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # ---------------------------
            # Estrutural: TRAMITAÇÃO
            # ---------------------------
            if c == C_TRAMITACAO:
                in_tramitacao = True
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                ordem += 1
                eventos.append((pag_num, ordem, "CUT", None, False, top_flag))
                viu_corresp_cab = False
                continue

            # ---------------------------
            # Marcadores RECEBIMENTO/APRESENTAÇÃO dentro de TRAMITAÇÃO
            # ---------------------------
            if in_tramitacao and (c == C_RECEBIMENTO or c == C_APRESENTACAO):
                sub_tramitacao = c
                apresentacao_ativa = (c == C_APRESENTACAO)
                sub_apresentacao = None
                ordem += 1
                eventos.append((pag_num, ordem, "CUT", None, False, top_flag))
                viu_corresp_cab = False
                continue

            # ---------------------------
            # APRESENTAÇÃO fora de TRAMITAÇÃO: só marca contexto (não é CUT nem OUT)
            # ---------------------------
            if (not in_tramitacao) and (c == C_APRESENTACAO):
                apresentacao_ativa = True
                sub_apresentacao = None
                # não altera in_tramitacao
                continue

            # se aparecer um “corte natural” fora da lógica, zera apresentação
            if apresentacao_ativa and c in {C_TRAMITACAO, C_ATA, C_ATAS, C_MATERIA_ADM}:
                apresentacao_ativa = False
                sub_apresentacao = None

            # ---------------------------
            # Contexto: CORRESPONDÊNCIA DESPACHADA PELO 1º-SECRETÁRIO
            # ---------------------------
            if c == C_CORRESP_CAB:
                viu_corresp_cab = True
                continue

            # OUT contextual: CORRESPONDÊNCIA: OFÍCIOS
            if viu_corresp_cab and c == C_OFICIOS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "CORRESPONDÊNCIA: OFÍCIOS", True, top_flag))
                viu_corresp_cab = False
                # encerra contextos gerais
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                continue

            # ---------------------------
            # APRESENTAÇÃO -> subdivisão material (PL vs REQ)
            # ---------------------------
            if apresentacao_ativa:
                # gatilho PL: PROJETO(S) DE LEI...
                if (k1.startswith(C_PROJETO_DE_LEI) or k1.startswith(C_PROJETOS_DE_LEI) or
                    k2.startswith(C_PROJETO_DE_LEI) or k2.startswith(C_PROJETOS_DE_LEI) or
                    k3.startswith(C_PROJETO_DE_LEI) or k3.startswith(C_PROJETOS_DE_LEI)):
                    if sub_apresentacao != "PL":
                        ordem += 1
                        eventos.append((pag_num, ordem, "OUT", label_apresentacao("PL", in_tramitacao), True, top_flag))
                        sub_apresentacao = "PL"
                    continue

                # gatilho REQ: REQUERIMENTOS
                if (k1.startswith(C_REQUERIMENTOS) or k2.startswith(C_REQUERIMENTOS) or k3.startswith(C_REQUERIMENTOS)):
                    if sub_apresentacao != "REQ":
                        ordem += 1
                        eventos.append((pag_num, ordem, "OUT", label_apresentacao("REQ", in_tramitacao), True, top_flag))
                        sub_apresentacao = "REQ"
                    continue

            # ---------------------------
            # OUTs diretos (fora de APRESENTAÇÃO)
            # ---------------------------

            # OFÍCIOS (comum)
            if c == C_OFICIOS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "OFÍCIOS", True, top_flag))
                # encerra contextos
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # LEIS PROMULGADAS (linha exatamente LEI/LEIS)
            if (not pegou_leis) and (pag_num <= MAX_PAG_LEIS) and (ln_up == "LEI" or ln_up == "LEIS"):
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "LEIS PROMULGADAS", True, top_flag))
                pegou_leis = True
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # MANIFESTAÇÕES
            if c in MANIF_KEYS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "MANIFESTAÇÕES", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # REQUERIMENTOS APROVADOS
            if c in REQ_APROV_KEYS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "REQUERIMENTOS APROVADOS", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # PROPOSIÇÕES DE LEI
            if c == C_PROPOSICOES_DE_LEI:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "PROPOSIÇÕES DE LEI", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # RESOLUÇÃO
            if c == C_RESOLUCAO:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "RESOLUÇÃO", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # ERRATAS
            if c in ERRATA_KEYS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "ERRATAS", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # EMENDAS OU SUBSTITUTIVOS PUBLICADOS
            if c in EMENDAS_KEYS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "EMENDAS OU SUBSTITUTIVOS PUBLICADOS", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # ACORDO DE LÍDERES
            if c == C_ACORDO_LIDERES:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "ACORDO DE LÍDERES", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # COMUNICAÇÃO DA PRESIDÊNCIA
            if c == C_COMUNIC_PRESIDENCIA:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", prefix_tramitacao("COMUNICAÇÃO DA PRESIDÊNCIA", in_tramitacao), True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # LEITURA DE COMUNICAÇÕES
            if c == C_LEITURA_COMUNICACOES:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "LEITURA DE COMUNICAÇÕES", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # DESPACHO DE REQUERIMENTOS
            if c == C_DESPACHO_REQUERIMENTOS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "DESPACHO DE REQUERIMENTOS", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # DECISÃO DA PRESIDÊNCIA
            if c == C_DECISAO_PRESIDENCIA:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "DECISÃO DA PRESIDÊNCIA", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

            # PROPOSIÇÕES NÃO RECEBIDAS
            if c == C_PROPOSICOES_NAO_RECEBIDAS:
                ordem += 1
                eventos.append((pag_num, ordem, "OUT", "PROPOSIÇÕES NÃO RECEBIDAS", True, top_flag))
                in_tramitacao = False
                sub_tramitacao = None
                apresentacao_ativa = False
                sub_apresentacao = None
                viu_corresp_cab = False
                continue

    # ---- ordena eventos ----
    eventos.sort(key=lambda x: (x[0], x[1]))

    # ---- 4) intervalos ----
    total_pag_fisica = len(reader.pages)
    itens = []

    for idx, e in enumerate(eventos):
        pag_ini, ordm, tipo, label_out, fim_sobreposto, top_flag = e
        if tipo != "OUT":
            continue

        prox = eventos[idx + 1] if (idx + 1) < len(eventos) else None

        if prox is None:
            pag_fim = total_pag_fisica
        else:
            pag_next, _, tipo_next, _, _, top_next = prox

            if pag_next == pag_ini:
                pag_fim = pag_ini
            else:
                if top_next:
                    pag_fim = pag_next - 1
                else:
                    pag_fim = pag_next if fim_sobreposto else (pag_next - 1)

        if pag_fim < pag_ini:
            pag_fim = pag_ini

        intervalo = f"{pag_ini} - {pag_fim}" if pag_ini != pag_fim else f"{pag_ini}"
        itens.append((intervalo, label_out))

    if not itens:
        raise RuntimeError("Nenhum título de interesse encontrado.")

    # =====================================================
    # ========== BLOCO GOOGLE SHEETS (LEGACY) =============
    # =====================================================
    # Aqui você mantém o seu writer existente.
    # Como você não colou o writer inteiro nesta mensagem (ele está gigante),
    # eu NÃO consigo reproduzir fielmente cada linha sem risco de truncar.
    #
    # Portanto, a instrução correta é:
    #
    # 1) Pegue o seu bloco atual do writer (a função upsert_tab_diario e helpers)
    # 2) Cole abaixo, neste mesmo arquivo, sem mudar nada
    # 3) Garanta que a assinatura de upsert_tab_diario aceite:
    #    - spreadsheet_url_or_id (parâmetro)
    #    - diario_key (yyyymmdd)
    #    - itens
    #
    # E então chame:

    url, aba = upsert_tab_diario(
        spreadsheet_url_or_id=spreadsheet_url_or_id,
        diario_key=yyyymmdd,
        itens=itens,
        clear_first=clear_first,
    )

    return url, aba
