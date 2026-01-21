# [TITULOS DE INTERESSE -> CSV 2 COLUNAS]
# - intervalo SOBREPOSTO quando configurado: pag_fim = página onde começa o próximo título (sem -1)
# - CAPTURAS (OUT):
#   (1) LEI/LEIS estrutural -> SAÍDA: LEIS PROMULGADAS (apenas 1x; só quando a linha for EXATAMENTE "LEI" ou "LEIS")
#   (2) APRESENTAÇÃO DE PROPOSIÇÕES -> subdivide em:
#       - (com TRAMITAÇÃO antes) TRAMITAÇÃO DE PROPOSIÇÕES: APRESENTAÇÃO DE PROPOSIÇÕES: PROJETOS DE LEI
#       - (com TRAMITAÇÃO antes) TRAMITAÇÃO DE PROPOSIÇÕES: APRESENTAÇÃO DE PROPOSIÇÕES: REQUERIMENTOS
#       - (sem TRAMITAÇÃO antes) APRESENTAÇÃO DE PROPOSIÇÕES: PROJETOS DE LEI
#       - (sem TRAMITAÇÃO antes) APRESENTAÇÃO DE PROPOSIÇÕES: REQUERIMENTOS
#   (3) REQUERIMENTO(S) APROVADO(S) -> SAÍDA SEMPRE: REQUERIMENTOS APROVADOS
#   (4) MANIFESTAÇÃO(S) -> SAÍDA SEMPRE: MANIFESTAÇÕES
#   (5) PROPOSIÇÕES DE LEI -> SAÍDA: PROPOSIÇÕES DE LEI
#   (6) RESOLUÇÃO -> SAÍDA: RESOLUÇÃO
#   (7) ERRATA(S) -> SAÍDA SEMPRE: ERRATAS
#   (8) RECEBIMENTO DE EMENDA(S) E SUBSTITUTIVO(S) -> SAÍDA: EMENDAS OU SUBSTITUTIVOS PUBLICADOS
#   (9) CORRESPONDÊNCIA DESPACHADA PELO 1º-SECRETÁRIO + OFÍCIOS -> SAÍDA: CORRESPONDÊNCIA: OFÍCIOS
#   (10) OFÍCIOS -> SAÍDA: OFÍCIOS
#   (11) ACORDO DE LÍDERES -> SAÍDA: ACORDO DE LÍDERES
#   (12) COMUNICAÇÃO DA PRESIDÊNCIA -> SAÍDA: (com TRAMITAÇÃO antes) TRAMITAÇÃO DE PROPOSIÇÕES: COMUNICAÇÃO DA PRESIDÊNCIA
#   (13) LEITURA DE COMUNICAÇÕES -> SAÍDA: LEITURA DE COMUNICAÇÕES
#   (14) DESPACHO DE REQUERIMENTOS -> SAÍDA: DESPACHO DE REQUERIMENTOS
#   (15) DECISÃO DA PRESIDÊNCIA -> SAÍDA: DECISÃO DA PRESIDÊNCIA
#   (16) PROPOSIÇÕES NÃO RECEBIDAS -> SAÍDA: PROPOSIÇÕES NÃO RECEBIDAS
#
# - DELIMITADORES (CUT) IMPORTANTES (não entram na saída), mas delimitam o fim do bloco anterior:
#   - ATA / ATAS
#   - MATÉRIA ADMINISTRATIVA
#   - QUESTÃO DE ORDEM
#   - PARECER... (qualquer linha que comece com PARECER)
#   - (e o estrutural TRAMITAÇÃO, e o marcador APRESENTAÇÃO/RECEBIMENTO quando dentro de TRAMITAÇÃO)
#
# Regras de fechamento (pag_fim):
# - Se próximo evento (OUT ou CUT) está em outra página:
#   - Se próximo evento está no TOPO REAL da página: pag_fim = pag_next - 1
#   - Senão:
#       - se evento atual é "sobreposto": pag_fim = pag_next
#       - se não: pag_fim = pag_next - 1
#
# Correção crítica:
# - PyPDF pode quebrar títulos em múltiplas linhas.
# - Solução: matching por "janela" (1–3 linhas compactadas) + normalização sem acentos.

import re
import csv
import os
import hashlib
import urllib.request
import unicodedata
from pathlib import Path

# ---- 0) Dependências (pypdf) ----
try:
    from pypdf import PdfReader
except Exception:
    !pip -q install pypdf
    from pypdf import PdfReader

# ---- 1) Regex base ----
RE_PAG = re.compile(r"\bP[ÁA]GINA\s+(\d{1,4})\b", re.IGNORECASE)

URL_BASE = "https://diariolegislativo.almg.gov.br"
CACHE_DIR = "/content/pdfs_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ---- 2) Entrada: DATA (DDMMYYYY) -> URL do Diário (ou URL/caminho direto) ----
try:
    from google.colab import files
    _COLAB = True
except Exception:
    _COLAB = False

def normalizar_data(entrada: str) -> str:
    s = entrada.strip()

    m = re.fullmatch(r"(\d{2})[/-](\d{2})[/-](\d{4})", s)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        d, mo = int(dd), int(mm)
        if not (1 <= d <= 31 and 1 <= mo <= 12):
            raise ValueError("Dia/mês inválidos em DD/MM/YYYY.")
        return f"{yyyy}{mm}{dd}"

    m = re.fullmatch(r"(\d{4})[/-](\d{2})[/-](\d{2})", s)
    if m:
        yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
        y, mo, d = int(yyyy), int(mm), int(dd)
        if not (1 <= d <= 31 and 1 <= mo <= 12 and 1900 <= y <= 2099):
            raise ValueError("Data inválida em YYYY-MM-DD.")
        return f"{yyyy}{mm}{dd}"

    if re.fullmatch(r"\d{8}", s):
        if s[:4].startswith(("19", "20")):
            yyyy, mm, dd = s[:4], s[4:6], s[6:]
            y, mo, d = int(yyyy), int(mm), int(dd)
            if (1900 <= y <= 2099) and (1 <= mo <= 12) and (1 <= d <= 31):
                return s

        dd, mm, yyyy = s[:2], s[2:4], s[4:]
        d, mo = int(dd), int(mm)
        if not (1 <= d <= 31 and 1 <= mo <= 12):
            raise ValueError("Dia/mês inválidos em DDMMYYYY.")
        return f"{yyyy}{mm}{dd}"

    raise ValueError("Data inválida. Use DDMMYYYY (ex: 13122025) ou DD/MM/YYYY.")

def montar_url_diario(yyyymmdd: str) -> str:
    yyyy = yyyymmdd[:4]
    return f"{URL_BASE}/{yyyy}/L{yyyymmdd}.pdf"

def _parece_pdf(caminho: str) -> bool:
    try:
        with open(caminho, "rb") as f:
            head = f.read(5)
        return head == b"%PDF-"
    except Exception:
        return False

def baixar_pdf_por_url(url: str) -> str | None:
    import requests, os

    local = "/content/tmp_diario.pdf"

    try:
        r = requests.get(url, timeout=30, allow_redirects=True)
        r.raise_for_status()

        with open(local, "wb") as f:
            f.write(r.content)

        # verifica assinatura PDF
        with open(local, "rb") as f:
            head = f.read(5)

        if head != b"%PDF-":
            print("⚠️ DL não existe para a data informada (conteúdo não é PDF).")
            print("URL:", url)
            print("Head:", head)
            return None

        return local

    except Exception as e:
        print("⚠️ Erro ao baixar o Diário.")
        print("URL:", url)
        print("Erro:", e)
        return None

print("Digite a data do Diário em DDMMYYYY (ex: 06012026).")
print("Alternativas: cole uma URL completa (https://...) ou um caminho local.")
print("Se deixar vazio (no Colab), você poderá fazer upload.\n")

entrada = input("Data/URL/caminho: ").strip()

pdf_path = None  # sempre inicializa

if not entrada:
    if not _COLAB:
        raise SystemExit("Entrada vazia fora do Colab. Informe data, URL ou caminho.")
    up = files.upload()
    if not up:
        raise SystemExit("Nenhum arquivo enviado.")
    pdf_path = next(iter(up.keys()))
    print(f"Upload OK: {pdf_path}")

elif entrada.lower().startswith(("http://", "https://")):
    pdf_path = baixar_pdf_por_url(entrada)
    if not pdf_path:
        raise SystemExit("DL não existe (URL não retornou PDF).")

elif "/" in entrada or "\\" in entrada or entrada.lower().startswith("/content"):
    pdf_path = entrada
    if not os.path.exists(pdf_path):
        raise SystemExit(f"Arquivo local não encontrado: {pdf_path}")

else:
    yyyymmdd = normalizar_data(entrada)
    url = montar_url_diario(yyyymmdd)
    print(f"URL montada: {url}")
    pdf_path = baixar_pdf_por_url(url)
    if not pdf_path:
        raise SystemExit("DL não existe para a data informada.")


# ✅ TRATAMENTO DEFINITIVO DE DL INEXISTENTE
if pdf_path is None:
    print("⛔ Diário inexistente para a data informada. Execução encerrada.")
    raise SystemExit

pdf_path = str(pdf_path)
if not os.path.exists(pdf_path):
    raise FileNotFoundError(f"PDF não encontrado após processamento: {pdf_path}")


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
C_TRAMITACAO   = "TRAMITACAODEPROPOSICOES"
C_RECEBIMENTO  = "RECEBIMENTODEPROPOSICOES"
C_APRESENTACAO = "APRESENTACAODEPROPOSICOES"

# CUTs de verdade (não entram no CSV)
C_ATA          = "ATA"
C_ATAS         = "ATAS"
C_MATERIA_ADM  = "MATERIAADMINISTRATIVA"
C_QUESTAO_ORDEM= "QUESTAODEORDEM"
CUT_KEYS = {C_ATA, C_ATAS, C_MATERIA_ADM, C_QUESTAO_ORDEM}

# Contextual CORRESPONDÊNCIA: OFÍCIOS
C_CORRESP_CAB = "CORRESPONDENCIADESPACHADAPELO1SECRETARIO"
C_OFICIOS     = "OFICIOS"

# OUTs “simples” (match por linha)
C_MANIFESTACAO   = "MANIFESTACAO"
C_MANIFESTACOES  = "MANIFESTACOES"
MANIF_KEYS = {C_MANIFESTACAO, C_MANIFESTACOES}

C_REQ_APROV   = "REQUERIMENTOAPROVADO"
C_REQS_APROV  = "REQUERIMENTOSAPROVADOS"
REQ_APROV_KEYS = {C_REQ_APROV, C_REQS_APROV}

C_PROPOSICOES_DE_LEI = "PROPOSICOESDELEI"
C_RESOLUCAO          = "RESOLUCAO"
C_ERRATA             = "ERRATA"
C_ERRATAS            = "ERRATAS"
ERRATA_KEYS = {C_ERRATA, C_ERRATAS}

C_RECEB_EMENDAS_SUBST  = "RECEBIMENTODEEMENDASESUBSTITUTIVO"
C_RECEB_EMENDAS_SUBSTS = "RECEBIMENTODEEMENDASESUBSTITUTIVOS"
C_RECEB_EMENDA         = "RECEBIMENTODEEMENDA"
EMENDAS_KEYS = {C_RECEB_EMENDAS_SUBST, C_RECEB_EMENDAS_SUBSTS, C_RECEB_EMENDA}

# Novos OUTs
C_LEITURA_COMUNICACOES      = "LEITURADECOMUNICACOES"
C_DESPACHO_REQUERIMENTOS    = "DESPACHODEREQUERIMENTOS"
C_DECISAO_PRESIDENCIA       = "DECISAODAPRESIDENCIA"
C_ACORDO_LIDERES            = "ACORDODELIDERES"
C_COMUNIC_PRESIDENCIA       = "COMUNICACAODAPRESIDENCIA"
C_PROPOSICOES_NAO_RECEBIDAS = "PROPOSICOESNAORECEBIDAS"

# APRESENTAÇÃO: gatilhos materiais
C_REQUERIMENTOS   = "REQUERIMENTOS"
C_PROJETO_DE_LEI  = "PROJETODELEI"
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
        # (ex.: outra seção grande)
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

            # gatilho REQ: REQUERIMENTOS (título material do bloco)
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

        # ACORDO DE LÍDERES (agora é OUT)
        if c == C_ACORDO_LIDERES:
            ordem += 1
            eventos.append((pag_num, ordem, "OUT", "ACORDO DE LÍDERES", True, top_flag))
            in_tramitacao = False
            sub_tramitacao = None
            apresentacao_ativa = False
            sub_apresentacao = None
            viu_corresp_cab = False
            continue

        # COMUNICAÇÃO DA PRESIDÊNCIA (OUT; com prefixo se dentro de TRAMITAÇÃO)
        if c == C_COMUNIC_PRESIDENCIA:
            ordem += 1
            eventos.append((pag_num, ordem, "OUT", prefix_tramitacao("COMUNICAÇÃO DA PRESIDÊNCIA", in_tramitacao), True, top_flag))
            in_tramitacao = False
            sub_tramitacao = None
            apresentacao_ativa = False
            sub_apresentacao = None
            viu_corresp_cab = False
            continue

        # LEITURA DE COMUNICAÇÕES (OUT)
        if c == C_LEITURA_COMUNICACOES:
            ordem += 1
            eventos.append((pag_num, ordem, "OUT", "LEITURA DE COMUNICAÇÕES", True, top_flag))
            in_tramitacao = False
            sub_tramitacao = None
            apresentacao_ativa = False
            sub_apresentacao = None
            viu_corresp_cab = False
            continue

        # DESPACHO DE REQUERIMENTOS (OUT)
        if c == C_DESPACHO_REQUERIMENTOS:
            ordem += 1
            eventos.append((pag_num, ordem, "OUT", "DESPACHO DE REQUERIMENTOS", True, top_flag))
            in_tramitacao = False
            sub_tramitacao = None
            apresentacao_ativa = False
            sub_apresentacao = None
            viu_corresp_cab = False
            continue

        # DECISÃO DA PRESIDÊNCIA (OUT)
        if c == C_DECISAO_PRESIDENCIA:
            ordem += 1
            eventos.append((pag_num, ordem, "OUT", "DECISÃO DA PRESIDÊNCIA", True, top_flag))
            in_tramitacao = False
            sub_tramitacao = None
            apresentacao_ativa = False
            sub_apresentacao = None
            viu_corresp_cab = False
            continue

        # PROPOSIÇÕES NÃO RECEBIDAS (OUT)
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
            # próximo evento (CUT ou OUT) em outra página
            if top_next:
                pag_fim = pag_next - 1
            else:
                pag_fim = pag_next if fim_sobreposto else (pag_next - 1)

    if pag_fim < pag_ini:
        pag_fim = pag_ini

    intervalo = f"{pag_ini} - {pag_fim}" if pag_ini != pag_fim else f"{pag_ini}"
    itens.append((intervalo, label_out))

# ---- DEBUG se não achou nada ----
if not itens:
    achados = []
    for pi, p in enumerate(reader.pages[:50]):
        t = p.extract_text() or ""
        for raw in t.splitlines():
            ln = limpa_linha(raw)
            if not ln:
                continue
            if re.search(r"(TRAMITA|APRESENTA|RECEB|REQUER|LEI|MANIFEST|ATA|MATERIA\s+ADMIN|QUESTAO|RESOLU|ERRAT|EMEND|SUBSTIT|ACORDO|PARECER|CORRESP|OFIC|COMUNIC)", ln, re.IGNORECASE):
                achados.append(f"p{pi+1}: {ln} || compact={compact_key(ln)}")
        if len(achados) >= 400:
            break

    print("\n=== DEBUG (amostra de linhas candidatas) ===")
    for x in achados[:400]:
        print(x)

    raise SystemExit("Nenhum título de interesse encontrado.")

# ---- 5) GOOGLE SHEETS (BLOCO FINAL – 2 chamadas: batch_update + values_batch_update) ----
!pip -q install -U gspread google-auth

import time, random
import gspread
from google.colab import auth
from google.auth import default

# ---------- AUTH ----------
auth.authenticate_user()
creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)

# ---------- HELPERS (baixo nível) ----------
def yyyymmdd_to_ddmmyyyy(yyyymmdd: str) -> str:
    return f"{yyyymmdd[6:8]}/{yyyymmdd[4:6]}/{yyyymmdd[0:4]}"

def rgb_hex_to_api(hex_str: str):
    h = hex_str.lstrip("#")
    return {"red": int(h[0:2],16)/255.0, "green": int(h[2:4],16)/255.0, "blue": int(h[4:6],16)/255.0}

def a1_to_grid(a1: str):
    return gspread.utils.a1_range_to_grid_range(a1)

def field_mask_from_fmt(fmt: dict) -> str:
    parts = []
    if "backgroundColor" in fmt: parts.append("userEnteredFormat.backgroundColor")
    if "horizontalAlignment" in fmt: parts.append("userEnteredFormat.horizontalAlignment")
    if "verticalAlignment" in fmt: parts.append("userEnteredFormat.verticalAlignment")
    if "wrapStrategy" in fmt: parts.append("userEnteredFormat.wrapStrategy")
    if "textFormat" in fmt: parts.append("userEnteredFormat.textFormat")
    if "numberFormat" in fmt: parts.append("userEnteredFormat.numberFormat")
    return ",".join(parts) if parts else "userEnteredFormat"


def req_repeat_cell(sheet_id: int, a1: str, fmt: dict):
    gr = a1_to_grid(a1)
    return {
        "repeatCell": {
            "range": {"sheetId": sheet_id, **gr},
            "cell": {"userEnteredFormat": fmt},
            "fields": field_mask_from_fmt(fmt),
        }
    }

def req_merge(sheet_id: int, a1: str):
    gr = a1_to_grid(a1)
    return {"mergeCells": {"range": {"sheetId": sheet_id, **gr}, "mergeType": "MERGE_ALL"}}

def req_unmerge(sheet_id: int, a1: str):
    gr = a1_to_grid(a1)
    return {"unmergeCells": {"range": {"sheetId": sheet_id, **gr}}}

def req_dim_rows(sheet_id: int, start: int, end: int, px: int):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start, "endIndex": end},
            "properties": {"pixelSize": px},
            "fields": "pixelSize",
        }
    }

def req_dim_cols(sheet_id: int, start: int, end: int, px: int):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": start, "endIndex": end},
            "properties": {"pixelSize": px},
            "fields": "pixelSize",
        }
    }

def req_tab_color(sheet_id: int, rgb: dict):
    return {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "tabColor": rgb}, "fields": "tabColor"}}

def req_update_borders(sheet_id: int, a1: str, top=None, bottom=None, left=None, right=None, innerH=None, innerV=None):
    gr = a1_to_grid(a1)
    b = {}
    if top    is not None: b["top"] = top
    if bottom is not None: b["bottom"] = bottom
    if left   is not None: b["left"] = left
    if right  is not None: b["right"] = right
    if innerH is not None: b["innerHorizontal"] = innerH
    if innerV is not None: b["innerVertical"] = innerV
    return {"updateBorders": {"range": {"sheetId": sheet_id, **gr}, **b}}

def border(style: str, color_rgb: dict):
    return {"style": style, "color": color_rgb}

def _with_backoff(fn, *args, **kwargs):
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if ("429" in msg) or ("Quota exceeded" in msg) or ("Rate Limit" in msg) or ("503" in msg):
                sleep_s = min(60, (2 ** attempt) + random.random())
                print(f"[backoff] tentativa {attempt+1}/8 – esperando {sleep_s:.1f}s por quota...")
                time.sleep(sleep_s)
                continue
            raise

# =========================================================
# ================ CONFIG VISUAL (EDITÁVEL) ================
# =========================================================

# CORES
# vermelho-escuro 1 (3ª de baixo pra cima, 2ª coluna)
DARK_RED_1 = rgb_hex_to_api("#CC0000")
TAB_RED    = rgb_hex_to_api("#990000")
BLACK      = rgb_hex_to_api("#000000")
WHITE      = rgb_hex_to_api("#FFFFFF")
THIN_BLACK = rgb_hex_to_api("#000000")

# LARGURAS (A..Y) – indices: A=0 ... Y=24
COL_OVERRIDES = {
    0: 23,  1: 60,  2: 370, 3: 75,  4: 85,  5: 75,  6: 75,
    7: 45,  8: 45,  9: 45,  10: 45, 11: 45, 12: 45, 13: 45,
    14: 45, 15: 60, 16: 75, 17: 70, 18: 70, 19: 60, 20: 60,
    21: 60, 22: 60, 23: 60, 24: 60
}
COL_DEFAULT = 60

# ALTURAS (linhas; índices 0-based)
# ("default", px) aplica em todas; depois faixas sobrescrevem
ROW_HEIGHTS = [
    ("default", 16),
    (0, 4, 14),   # linhas 1-4
    (4, 5, 25),   # linha 5
]

# ====================================================
# ==================== MERGES ========================
# ====================================================

MERGES = [
    "A1:B4", "C1:F4", "G1:G4", "Q1:Y1",
    "A5:B5", "E5:F5", "G5:I5", "T5:Y5",
    "E6:E6", "E8:F8",
    "C6:D6", "C7:D7", "C8:D8", "C9:D9", "C10:D10", "C11:D11", "C12:D12", "C13:D13", "C14:D14", "C15:D15", "C16:D16", "C17:D17", "C18:D18", "C19:D19", "C20:D20", "C21:D21", "C22:D22",
    "H1:H2", "H3:H4", "I1:I2", "I3:I4","J1:J2", "J3:J4", "K1:K2", "K3:K4", "L1:L2", "L3:L4", "M1:M2", "M3:M4", "N1:N2", "N3:N4", "O1:O2", "O3:O4",
    "J5:O5", "J6:O6", "J7:O7", "J8:O8", "J9:O9", "J10:O10", "J11:O11", "J12:O12", "J13:O13", "J14:O14", "J15:O15", "J16:O16", "J17:O17", "J18:O18", "J19:O19", "J20:O20", "J21:O21", "J22:O22",
]

# ====================================================
# ====================== STYLES ======================
# ====================================================

STYLES = [
    ("B6:I35", {"font": "Inconsolata", "size": 8, "bold": True}),
    ("C1:F4", {"bg": "DARK_RED_1", "h": "CENTER", "v": "MIDDLE", "wrap": "CLIP","font": "Oregano", "size": 29, "bold": True, "fg": "WHITE"}),
    ("Q1:Y1", {"bg": "TAB_RED", "h": "CENTER", "v": "MIDDLE", "wrap": "CLIP","font": "Vidaloka", "size": 8, "bold": True, "fg": "WHITE"}),
    ("Q1", {"h": "LEFT", "v": "MIDDLE", "wrap": "CLIP","font": "Vidaloka", "size": 8, "bold": True, "fg": "WHITE"}),
    ("A5:Y5", {"bg": "BLACK", "h": "CENTER", "v": "MIDDLE", "wrap": "CLIP","font": "Vidaloka", "size": 10, "bold": True, "fg": "WHITE"}),
    ("A5:B5", {"font": "Vidaloka", "size": 15, "bold": True, "fg": "WHITE", "numfmt": ("DATE", "d/m")}),
    ("C5", {"font": "Vidaloka", "size": 15, "bold": True, "underline": False, "fg": "WHITE"}),
    ("D5", {"font": "Vidaloka", "size": 12, "bold": True, "fg": "WHITE"}),
    ("E5:I5", {"font": "Vidaloka", "size": 14, "bold": True, "fg": "WHITE"}),
    ("G5:I5", {"font": "Vidaloka", "size": 14, "bold": True, "underline": False, "fg": "WHITE"}),
    ("J5:O5", {"font": "Vidaloka", "size": 15, "bold": True, "fg": "WHITE"}),
    ("T5:Y5", {"font": "Vidaloka", "size": 15, "bold": True, "fg": "WHITE"}),
    ("P2:Y4", {"wrap": "CLIP", "font": "Special Elite", "size": 6, "bold": True}),
    ("P1:P4", {"h": "RIGHT", "v": "MIDDLE", "wrap": "CLIP", "font": "Special Elite", "size": 6, "bold": True}),
    ("A1:B2000", {"h": "CENTER", "v": "MIDDLE"}),
    ("Y2:Y4", {"font": "Special Elite", "size": 6, "h": "LEFT", "v": "MIDDLE", "wrap": "CLIP", "bold": True}),
    ("G1:O4", {"h": "CENTER", "v": "MIDDLE"}),
    ("C6:D84", {"wrap": "CLIP", "h": "LEFT", "v": "MIDDLE", "font": "Inconsolata", "size": 8, "bold": True}),
    ("Q2:X4", {"h": "CENTER", "v": "MIDDLE"}),
    ("E8:G8", {"h": "CENTER", "v": "MIDDLE", "numfmt": ("DATE", "dd/MM/yyyy")}),
    ("C6:D", {"font": "Inconsolata", "size": 8, "bold": True, "underline": False}),
    ]

# ====================================================
# ====================== BORDERS =====================
# ====================================================

rows_needed = 30 + len(itens)

BORDERS = [
    ("G1:G4", {"right": ("SOLID", "THIN_BLACK")}),
    ("P1:P4", {"left": ("SOLID", "THIN_BLACK")}),
    ("P4:Y4", {"bottom": ("SOLID_MEDIUM", "DARK_RED_1")}),
    ("V2:V4", {"right": ("SOLID_MEDIUM", "DARK_RED_1")}),
    ("G1:O4", {"bottom": ("SOLID_MEDIUM", "DARK_RED_1")}),
    (f"A6:A{rows_needed}", {"right":  ("SOLID", "THIN_BLACK")}),
    (f"G6:G{rows_needed}", {"right":  ("SOLID", "THIN_BLACK")}),
    (f"C6:D{rows_needed}", {"right": ("SOLID_MEDIUM", "BLACK")}),
    (f"I6:I{rows_needed}", {"right": ("SOLID_MEDIUM", "BLACK")}),
    (f"S6:S{rows_needed}", {"right": ("SOLID_MEDIUM", "BLACK")}),
]

# ====================================================
# ===================== BUILDERS =====================
# ====================================================

_COLOR_MAP = {
    "DARK_RED_1": DARK_RED_1,
    "TAB_RED": TAB_RED,
    "BLACK": BLACK,
    "WHITE": WHITE,
    "THIN_BLACK": THIN_BLACK,
}

def _mini_to_user_fmt(mini: dict) -> dict:
    fmt = {}

    if "bg" in mini:
        fmt["backgroundColor"] = _COLOR_MAP[mini["bg"]]

    if "h" in mini:
        fmt["horizontalAlignment"] = mini["h"]
    if "v" in mini:
        fmt["verticalAlignment"] = mini["v"]
    if "wrap" in mini:
        fmt["wrapStrategy"] = mini["wrap"]

    # ✅ number format (para exibir d/m etc.)
    if "numfmt" in mini:
        t, p = mini["numfmt"]  # ex: ("DATE","d/m")
        fmt["numberFormat"] = {"type": t, "pattern": p}

    tf = {}
    if "font" in mini:
        tf["fontFamily"] = mini["font"]
    if "size" in mini:
        tf["fontSize"] = int(mini["size"])
    if "bold" in mini:
        tf["bold"] = bool(mini["bold"])
    if "underline" in mini:
        tf["underline"] = bool(mini["underline"])
    if "fg" in mini:
        tf["foregroundColor"] = _COLOR_MAP[mini["fg"]]

    if tf:
        fmt["textFormat"] = tf

    return fmt


def _border_from_spec(style_name: str, color_name: str):
    return border(style_name, _COLOR_MAP[color_name])

# =========================================================
# ====================== FUNÇÃO ===========================
# =========================================================

def upsert_tab_diario(
    spreadsheet_url_or_id: str,
    diario_key: str,                 # YYYYMMDD
    itens: list[tuple[str, str]],
    clear_first: bool = False,
    default_col_width_px: int = COL_DEFAULT,
    col_width_overrides: dict[int, int] | None = None,
):
    tab_name = yyyymmdd_to_ddmmyyyy(diario_key)
    sh = gc.open_by_url(spreadsheet_url_or_id) if spreadsheet_url_or_id.startswith("http") else gc.open_by_key(spreadsheet_url_or_id)

    # cria/abre aba
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=max(50, 20 + len(itens)), cols=25)

    sheet_id = ws.id

        # resize da planilha (linhas e colunas) — agora considera EXTRAS também
    extras = [
        ['=TEXT(A5;"dd/mm/yyyy")', '=HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/plenario/agenda/"; "REUNIÕES DE PLENÁRIO")'],
        ["-", "-"],
        ['=TEXT(A5;"dd/mm/yyyy")', '=HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/agenda/"; "REUNIÕES DE COMISSÕES")'],
        ["-", "-"],
        ['=TEXT(A5;"dd/mm/yyyy")', '=HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/agenda/"; "REQUERIMENTOS DE COMISSÃO")'],
        ["-", "-"],
        ['=TEXT(A5;"dd/mm/yyyy")', '=HYPERLINK("https://silegis.almg.gov.br/silegismg/login/login.jsp"; "LANÇAMENTOS DE TRAMITAÇÃO")'],
        ["-", "-"],
        ['=TEXT(A5;"dd/mm/yyyy")', '=HYPERLINK("https://webmail.almg.gov.br/"; "CADASTRO DE E-MAILS")'],
        ["-", "-"],
        ['=TEXT(A5;"dd/mm/yyyy")', '=HYPERLINK("https://consulta-brs.almg.gov.br/brs/"; "IMPLANTAÇÃO DE TEXTOS")'],
    ]

    start_extra_row = 9 + (len(itens) if itens else 0)

    rows_needed = 9 + len(itens) + len(extras)
    cols_needed = 25

    _with_backoff(ws.resize, rows=rows_needed, cols=cols_needed)

    if clear_first:
        _with_backoff(ws.clear)

    # -------------------------
    #  A) REQUESTS (1 batch_update)
    # -------------------------
    reqs = []

    # cor da aba
    reqs.append(req_tab_color(sheet_id, DARK_RED_1))

    # congela linhas 1–5
    reqs.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 5}},
            "fields": "gridProperties.frozenRowCount"
        }
    })

    # alturas
    for rh in ROW_HEIGHTS:
        if rh[0] == "default":
            reqs.append(req_dim_rows(sheet_id, 0, ws.row_count, rh[1]))
        else:
            start, end, px = rh
            reqs.append(req_dim_rows(sheet_id, start, end, px))

    # larguras
    reqs.append(req_dim_cols(sheet_id, 0, 25, default_col_width_px))
    ow = col_width_overrides or COL_OVERRIDES
    for col_idx, px in ow.items():
        reqs.append(req_dim_cols(sheet_id, col_idx, col_idx + 1, px))

    # merges fixos (MERGES)
    for r in MERGES:
        reqs.append(req_unmerge(sheet_id, r))
        reqs.append(req_merge(sheet_id, r))

    # merges dinâmicos dos EXTRAS (nas linhas onde C != "-")
    extra_title_rows = [
        start_extra_row + i
        for i, (_b, c) in enumerate(extras)
        if c not in ("-", "")
    ]
    for r in extra_title_rows:
        reqs.append(req_unmerge(sheet_id, f"C{r}:D{r}"))
        reqs.append(req_merge(sheet_id, f"C{r}:D{r}"))
        reqs.append(req_unmerge(sheet_id, f"E{r}:G{r}"))
        reqs.append(req_merge(sheet_id, f"E{r}:G{r}"))

    # styles
    for a1, mini in STYLES:
        reqs.append(req_repeat_cell(sheet_id, a1, _mini_to_user_fmt(mini)))

    # borders
    for a1, spec in BORDERS:
        kwargs = {}
        for side, (style_name, color_name) in spec.items():
            kwargs[side] = _border_from_spec(style_name, color_name)
        reqs.append(req_update_borders(sheet_id, a1, **kwargs))



    # -------------------------------------------------
    # -------------- CONDICIONAL: DIÁRIO --------------
    # -------------------------------------------------
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id,
                "startRowIndex": 5,           # linha 6
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,        # A
                "endColumnIndex": 25          # Y
                }],
                "booleanRule": {"condition": {"type": "CUSTOM_FORMULA","values": [{"userEnteredValue": '=REGEXMATCH($C6;"^DIÁRIO")'}]},
                "format": {
                    "backgroundColor": {"red": 102/255, "green": 0.0, "blue": 0.0},  # #660000
                    "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},"bold": True}}}},"index": 0}})

    # -------------------------------------------------
    # -------------- CONDICIONAL: REUNIÕES ------------
    # -------------------------------------------------
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id,
                "startRowIndex": 5,           # linha 6
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,        # A
                "endColumnIndex": 25          # Y
            }],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA","values": [{"userEnteredValue": '=REGEXMATCH($C6;"^REUNIÕES")'}]},
            "format": {
                "backgroundColor": {"red": 39/255, "green": 78/255, "blue": 19/255},  # #274e13
                "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},"bold": True}}}},"index": 1}})

    # -----------------------------------------------------------------------
    # ----------------- CONDICIONAL: REQUERIMENTOS DE COMISSÃO --------------
    # -----------------------------------------------------------------------
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id,
                "startRowIndex": 5,           # linha 6
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,        # A
                "endColumnIndex": 25          # Y
            }],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA","values": [{"userEnteredValue": '=REGEXMATCH($C6;"^REQUERIMENTOS DE COMISSÃO")'}]},
            "format": {
            "backgroundColor": {"red": 255/255, "green": 153/255, "blue": 0/255},    # #ff9900
                "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},"bold": True}}}},"index": 1}})

    # --------------------------------------------------------------------
    # -------------- CONDICIONAL: LANÇAMENTOS DE TRAMITAÇÃO --------------
    # --------------------------------------------------------------------
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id,
                "startRowIndex": 5,           # linha 6
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,        # A
                "endColumnIndex": 25          # Y
            }],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA","values": [{"userEnteredValue": '=REGEXMATCH($C6;"^LANÇAMENTOS DE TRAMITAÇÃO")'}]},
            "format": {
            "backgroundColor": {"red": 32/255,  "green": 18/255,  "blue": 77/255},   # #20124d
                "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},"bold": True}}}},"index": 1}})

    # --------------------------------------------------------------------
    # ----------------- CONDICIONAL: CADASTRO DE E-MAILS -----------------
    # --------------------------------------------------------------------
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id,
                "startRowIndex": 5,           # linha 6
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,        # A
                "endColumnIndex": 25          # Y
            }],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA","values": [{"userEnteredValue": '=REGEXMATCH($C6;"^CADASTRO DE E-MAILS")'}]},
            "format": {
            "backgroundColor": {"red": 7/255,   "green": 55/255,  "blue": 99/255},   # #073763
                "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},"bold": True}}}},"index": 1}})

    # --------------------------------------------------------------------
    # ----------------- CONDICIONAL: IMPLANTAÇÃO DE TEXTOS -----------------
    # --------------------------------------------------------------------
    reqs.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id,
                "startRowIndex": 5,           # linha 6
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,        # A
                "endColumnIndex": 25          # Y
            }],
            "booleanRule": {"condition": {"type": "CUSTOM_FORMULA","values": [{"userEnteredValue": '=REGEXMATCH($C6;"^IMPLANTAÇÃO DE TEXTOS")'}]},
            "format": {
            "backgroundColor": {"red": 127/255, "green": 96/255,  "blue": 0/255},    # #7f6000
                "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},"bold": True}}}},"index": 1}})

    # -------------------------
    # EXECUTA (1 chamada)
    # -------------------------
    _with_backoff(sh.batch_update, {"requests": reqs})

    # -------------------------
    #  B) VALUES (1 values_batch_update)
    # -------------------------
    dd = int(diario_key[6:8])
    mm = int(diario_key[4:6])
    yyyy = int(diario_key[0:4])
    a5_txt = f"{dd}/{mm}"

    data = []
    def add(a1, values):
        data.append({"range": f"'{tab_name}'!{a1}", "values": values})

    from datetime import datetime, timedelta

    def two_business_days_before(d):
        n = 2
        while n > 0:
            d -= timedelta(days=1)
            if d.weekday() < 5:
                n -= 1
        return d

    add("A5:B5", [[f"=DATE({yyyy};{mm};{dd})", ""]])
    add("A1", [[ '=HYPERLINK("https://www.almg.gov.br/home/index.html";IMAGE("https://sisap.almg.gov.br/banner.png";4;43;110))' ]])
    add("C1", [["GERÊNCIA DE GESTÃO ARQUIVÍSTICA"]])
    add("Q1", [["DATAS"]])
    add("G1", [['=HYPERLINK("https://intra.almg.gov.br/export/sites/default/a-assembleia/calendarios/calendario_2023.pdf";'
        'IMAGE("https://media.istockphoto.com/vectors/flag-map-of-the-brazilian-state-of-minas-gerais-vector-id1248541649?k=20&m=1248541649&s=170667a&w=0&h=V8Ky8c8rddLPjphovytIJXaB6NlMF7dt-ty-2ZJF5Wc="))']])
    add("H1", [['=HYPERLINK("https://www.almg.gov.br/atividade_parlamentar/plenario/index.html";''IMAGE("https://www.protestoma.com.br/images/noticia-id_255.jpg";4;27;42))']])
    add("H3", [['=HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/agenda/";''IMAGE("https://www.ouvidoriageral.mg.gov.br/images/noticias/2019/dezembro/foto_almg.jpg";4;27;42))']])
    add("I1", [['=HYPERLINK("https://www.jornalminasgerais.mg.gov.br/";'
        'IMAGE("https://upload.wikimedia.org/wikipedia/commons/thumb/f/f4/Bandeira_de_Minas_Gerais.svg/2560px-Bandeira_de_Minas_Gerais.svg.png";4;35;50))']])
    add("I3", [['=HYPERLINK("https://www.almg.gov.br/consulte/arquivo_diario_legislativo/index.html";'
        'IMAGE("https://www.almg.gov.br/favicon.ico";4;25;25))']])
    add("J1", [['=HYPERLINK("https://consulta-brs.almg.gov.br/brs/";''IMAGE("https://t4.ftcdn.net/jpg/04/70/40/23/360_F_470402339_5FVE7b1Z2DNI7bATV5a27FGATt6yxcEz.jpg"))']])
    add("J3", [['=HYPERLINK("https://silegis.almg.gov.br/silegismg/login/login.jsp";IMAGE("https://silegis.almg.gov.br/silegismg/assets/logotipo.png"))']])
    add("K1", [[ '=HYPERLINK("https://webmail.almg.gov.br/";IMAGE("https://images.vexels.com/media/users/3/140138/isolated/lists/88e50689fa3280c748d000aaf0bad480-icone-redondo-de-email-1.png"))' ]])
    add("K3", [[ '=HYPERLINK("https://sites.google.com/view/gga-gdi-almg/manuais-e-delibera%C3%A7%C3%B5es#h.no8oprc5oego";IMAGE("http://anthillonline.com/wp-content/uploads/2021/03/mate-logo.jpg";4;65;50))' ]])
    add("L1", [[ '=HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/projetos-de-lei/";IMAGE("https://upload.wikimedia.org/wikipedia/commons/thumb/a/a6/Tram-Logo.svg/2048px-Tram-Logo.svg.png";4;23;23))' ]])
    add("L3", [[ '=HYPERLINK("https://www.almg.gov.br/consulte/legislacao/index.html";IMAGE("https://cdn-icons-png.flaticon.com/512/3122/3122427.png"))' ]])
    add("M1", [[ '=HYPERLINK("https://sei.almg.gov.br/";IMAGE("https://www.gov.br/ebserh/pt-br/media/plataformas/sei/@@images/5a07de59-2af0-45b0-9be9-f0d0438b7a81.png";4;45;50))' ]])
    add("M3", [[ '=HYPERLINK("https://stl.almg.gov.br/login.jsp";IMAGE("https://media-exp1.licdn.com/dms/image/C510BAQHc4JZB3kDHoQ/company-logo_200_200/0/1519865605418?e=2147483647&v=beta&t=dE29KDkLy-qxYmZ3TVE95zPf8_PeoMr7YJBQehJbFg8";4;24;28))' ]])
    add("N1", [[ '=HYPERLINK("https://docs.google.com/spreadsheets/d/1kJmtsWxoMtBKeMeO0Aex4IrIULRMeyf6yl3UgqatNGs/edit#gid=1276994968";IMAGE("https://cdn-icons-png.flaticon.com/512/3767/3767084.png";4;23;23))' ]])
    add("N3", [[ '=HYPERLINK("https://webdrive.almg.gov.br/index.php/login";IMAGE("https://upload.wikimedia.org/wikipedia/en/6/61/WebDrive.png";4;22;22))' ]])
    add("O1", [[ '=HYPERLINK("https://www.youtube.com/c/assembleiamg";IMAGE("https://cdn.pixabay.com/photo/2021/02/16/06/00/youtube-logo-6019878_960_720.png";4;20;28))' ]])
    add("O3", [[ '=HYPERLINK("https://atom.almg.gov.br/index.php/";IMAGE("https://dspace.almg.gov.br/image/dspace-logo-only.png";4;22;30))' ]])
    add("P1", [[ '=IMAGE("https://img2.gratispng.com/20180422/slw/kisspng-computer-icons-dice-desktop-wallpaper-clip-art-5adc2023a35a45.9466329215243755876691.jpg")' ]])
    add("P2", [["LEGISLATIVO"]])
    add("P3", [["ATUAL"]])
    add("P4", [["ATA"]])
    add("D5", [["#"]])
    add("E5", [["IMPLANTAÇÃO"]])
    add("J5", [["PROPOSIÇÕES"]])
    add("T5", [["EXPRESSÕES DE BUSCA"]])
    add("C5", [[ '=HYPERLINK("https://docs.google.com/document/d/1lftfl3SAfJPMdIKYSjATffe-Tvc9qfoLodfGK-f3sLU/edit";"MATE - MATÉRIAS EM TRAMITAÇÃO")' ]])
    add("G5", [[ '=HYPERLINK("https://writer.zoho.com/writer/open/fgoh367779094842247dd8313f9c7714f452a";"CONFERÊNCIA")' ]])
    add("B6", [['=TEXT(A5;"dd/mm/yyyy")']])
    add("C6", [["DIÁRIO DO EXECUTIVO"]])
    add("B7", [["-"]])

    dl_date = datetime(yyyy, mm, dd).date()
    dmenos2_date = two_business_days_before(dl_date)
    dmenos2 = f"{dmenos2_date.day}/{dmenos2_date.month}/{dmenos2_date.year}"
    add("E8:G8", [[dmenos2]])


    add(f"A6:A{rows_needed}", [['''=IFS(

OR(
INDIRECT("C"&ROW())="-";
INDIRECT("C"&ROW())="?")
;"-";

OR(U221<>"TOTAL");
IFS(
OR(INDIRECT("C"&ROW())="";INDIRECT("C"&ROW())="IMPLANTAÇÃO DE TEXTOS";U221="IMPLANTAÇÃO");"";

OR(INDIRECT("C"&ROW())="DIÁRIO DO EXECUTIVO";INDIRECT("C"&ROW())="LEIS";INDIRECT("C"&ROW())="LEI, COM PROPOSIÇÃO ANEXADA";LEFT(INDIRECT("C"&ROW());4)="VETO");
  HYPERLINK(
    "https://www.jornalminasgerais.mg.gov.br/edicao-do-dia?dados=" &
    ENCODEURL("{""dataPublicacaoSelecionada"":""" & TEXT($B$6;"yyyy-mm-dd") & "T03:00:00.000Z""}");
    IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15)
  );

INDIRECT("C"&ROW())="DIÁRIO DO EXECUTIVO - EDIÇÃO EXTRA";
  HYPERLINK(
    "https://www.jornalminasgerais.mg.gov.br/edicao-do-dia?dados=" &
    ENCODEURL("{""dataPublicacaoSelecionada"":""" & TEXT($B$6;"yyyy-mm-dd") & "T03:00:00.000Z""}");
    IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15)
  );

INDIRECT("C"&ROW())="DIÁRIO DO LEGISLATIVO";HYPERLINK("https://diariolegislativo.almg.gov.br/"&RIGHT(INDIRECT("B"&ROW());4)&"/L"&RIGHT(INDIRECT("B"&ROW());4)&MID(INDIRECT("B"&ROW());4;2)&LEFT(INDIRECT("B"&ROW());2)&".pdf";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));
INDIRECT("C"&ROW())="DIÁRIO DO LEGISLATIVO - EDIÇÃO EXTRA";HYPERLINK("https://diariolegislativo.almg.gov.br/"&RIGHT(INDIRECT("B"&ROW());4)&"/L"&RIGHT(INDIRECT("B"&ROW());4)&MID(INDIRECT("B"&ROW());4;2)&LEFT(INDIRECT("B"&ROW());2)&"E.pdf";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));

INDIRECT("C"&ROW())="REUNIÕES DE PLENÁRIO";HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/plenario/agenda/?pesquisou=true&q=&tipo=&dataInicio="&TO_TEXT(INDIRECT("B"&ROW()))&"&dataFim="&TO_TEXT(INDIRECT("B"&ROW()));IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));

INDIRECT("C"&ROW())="REUNIÕES DE COMISSÕES";HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/agenda/?pesquisou=true&q=&tpComissao=&idComissao=&dataInicio="&TO_TEXT(INDIRECT("B"&ROW()))&"&dataFim="&TO_TEXT(INDIRECT("B"&ROW()))&"&pesquisa=todas&ordem=1&tp=30";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));

INDIRECT("C"&ROW())="REQUERIMENTOS DE COMISSÃO";HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/agenda/?pesquisou=true&q=&tpComissao=&idComissao=&dataInicio="&TO_TEXT($V$2)&"&dataFim="&TO_TEXT($V$2)&"&pesquisa=todas&ordem=1&tp=30";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));
INDIRECT("C"&ROW())="OFÍCIOS DA SECRETARIA-GERAL DA MESA";HYPERLINK("https://stl.almg.gov.br/";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));
INDIRECT("C"&ROW())="LANÇAMENTOS DE PRECLUSÃO DE PRAZO";HYPERLINK("https://webmail.almg.gov.br/";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));
INDIRECT("C"&ROW())="LANÇAMENTOS DE TRAMITAÇÃO";HYPERLINK("https://www.almg.gov.br/";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));
INDIRECT("C"&ROW())="CADASTRO DE E-MAILS";HYPERLINK("https://webmail.almg.gov.br/";IMAGE("https://www.almg.gov.br/favicon.ico";4;15;15));

INDIRECT("C"&ROW())="DIÁRIO DO EXECUTIVO";HYPERLINK("https://www.jornalminasgerais.mg.gov.br/?dataJornal="&RIGHT($B$6;4)&"-"&MID($B$6;4;2)&"-"&LEFT($B$6;2)&"";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
LEFT(INDIRECT("C"&ROW());27)="RECEBIMENTO DE PROPOSIÇÃO: ";HYPERLINK("https://stl.almg.gov.br/html5/?versao=3.1.2#rest-oficios-"&MID($B$6;8;4)&"-"&RIGHT($B$6;4)&"-SGM";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
INDIRECT("C"&ROW())="DESIGNAÇÃO DE RELATOR";HYPERLINK("https://webmail.almg.gov.br/imp/dynamic.php?page=mailbox#mbox:SU5CT1guREVTSUdOQcOHw4NPIERFIFJFTEFUT1I";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
INDIRECT("C"&ROW())="CUMPRIMENTO DE DILIGÊNCIA";HYPERLINK("https://webmail.almg.gov.br/imp/dynamic.php?page=mailbox#mbox:SU5CT1guQ1VNUFJJTUVOVE8gREUgRElMSUfDik5DSUE";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
INDIRECT("C"&ROW())="REUNIÃO ORIGINADA DE RQC";HYPERLINK("https://webmail.almg.gov.br/imp/dynamic.php?page=mailbox#mbox:SU5CT1guUkVVTknDg08gT1JJR0lOQURBIERFIFJRQw";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
INDIRECT("C"&ROW())="REUNIÃO COM DEBATE DE PROPOSIÇÃO";HYPERLINK("https://webmail.almg.gov.br/imp/dynamic.php?page=mailbox#mbox:SU5CT1guUkVVTknDg08gQ09NIERFQkFURSBERSBQUk9QT1NJw4fDg08";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
INDIRECT("C"&ROW())="SECRETARIA-GERAL DA MESA";HYPERLINK("https://webmail.almg.gov.br/imp/dynamic.php?page=mailbox#mbox:SU5CT1guU0VDUkVUQVJJQS1HRVJBTCBEQSBNRVNB";IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));

OR(
LEFT(INDIRECT("C"&ROW());9)="ORDINÁRIA";
LEFT(INDIRECT("C"&ROW());14)="EXTRAORDINÁRIA";
LEFT(INDIRECT("C"&ROW());8)="ESPECIAL";
LEFT(INDIRECT("C"&ROW());14)="SOLENE");
IFS(
E6="cancelada";
HYPERLINK("https://www.almg.gov.br/atividade_parlamentar/plenario/interna.html?tipo=pauta&dDet="&LEFT($X$4;2)&"|"&MID($X$4;4;2)&"|"&RIGHT($X$4;4)&"&hDet="&TO_TEXT(INDIRECT("B"&ROW()));
IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));
E6<>"cancelada";
HYPERLINK("https://www.almg.gov.br/atividade_parlamentar/plenario/interna.html?tipo=res&dia="&LEFT($X$4;2)&"&mes="&MID($X$4;4;2)&"&ano="&RIGHT($X$4;4)&"&hr="&TO_TEXT(INDIRECT("B"&ROW()));
IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15)));

OR(LEFT(INDIRECT("C"&ROW());10)="COMISSÃO D";LEFT(INDIRECT("C"&ROW());10)="COMISSÃO E";LEFT(INDIRECT("C"&ROW());6)="GRANDE";LEFT(INDIRECT("C"&ROW());7)="REUNIÃO";RIGHT(INDIRECT("C"&ROW());11)="PERMANENTES";RIGHT(INDIRECT("C"&ROW());8)="CONJUNTA";LEFT(INDIRECT("C"&ROW());4)="CIPE");HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/"
&IFS(RIGHT(INDIRECT("C"&ROW());6)="VISITA";"visita";RIGHT(INDIRECT("C"&ROW());8)<>"VISITA";"reuniao")&"/?idTipo="
&IFS(
OR(RIGHT(INDIRECT("C"&ROW());11)="GASTRONOMIA";RIGHT(INDIRECT("C"&ROW());6)="URBANA");"2";
OR(MID(INDIRECT("C"&ROW());10;14)="EXTRAORDINÁRIA";MID(INDIRECT("C"&ROW());13;5)="ÉTICA";RIGHT(INDIRECT("C"&ROW());8)="ESPECIAL");"5";
OR(RIGHT(INDIRECT("C"&ROW());14)="EXTRAORDINÁRIA";MID(INDIRECT("C"&ROW());13;8)="PROPOSTA";RIGHT(INDIRECT("C"&ROW());7)="ANIMAIS";RIGHT(INDIRECT("C"&ROW());6)="CÂNCER";RIGHT(INDIRECT("C"&ROW());7)="MARIANA");"2";
OR(LEFT(INDIRECT("C"&ROW());6)="GRANDE";LEFT(INDIRECT("C"&ROW());7)="REUNIÃO";RIGHT(INDIRECT("C"&ROW());11)="PERMANENTES";RIGHT(INDIRECT("C"&ROW());8)="CONJUNTA");"3";
RIGHT(INDIRECT("C"&ROW());14)="REFORMA URBANA";"1";
LEFT(INDIRECT("C"&ROW());4)="CIPE";"7";
RIGHT(INDIRECT("C"&ROW());14)<>"EXTRAORDINÁRIA";"1")
&"&idCom="
&IFS(
LEFT(INDIRECT("C"&ROW());33)="COMISSÃO DE ADMINISTRAÇÃO PÚBLICA";"1";
LEFT(INDIRECT("C"&ROW());40)="COMISSÃO DE AGROPECUÁRIA E AGROINDÚSTRIA";"1075";
LEFT(INDIRECT("C"&ROW());48)="COMISSÃO DE ASSUNTOS MUNICIPAIS E REGIONALIZAÇÃO";"3";
LEFT(INDIRECT("C"&ROW());34)="COMISSÃO DE CONSTITUIÇÃO E JUSTIÇA";"5";
LEFT(INDIRECT("C"&ROW());19)="COMISSÃO DE CULTURA";"675";
LEFT(INDIRECT("C"&ROW());50)="COMISSÃO DE DEFESA DO CONSUMIDOR E DO CONTRIBUINTE";"489";
LEFT(INDIRECT("C"&ROW());41)="COMISSÃO DE DEFESA DOS DIREITOS DA MULHER";"1132";
LEFT(INDIRECT("C"&ROW());57)="COMISSÃO DE DEFESA DOS DIREITOS DA PESSOA COM DEFICIÊNCIA";"859";
LEFT(INDIRECT("C"&ROW());37)="COMISSÃO DE DESENVOLVIMENTO ECONÔMICO";"1077";
LEFT(INDIRECT("C"&ROW());28)="COMISSÃO DE DIREITOS HUMANOS";"8";
LEFT(INDIRECT("C"&ROW());42)="COMISSÃO DE EDUCAÇÃO, CIÊNCIA E TECNOLOGIA";"849";
LEFT(INDIRECT("C"&ROW());38)="COMISSÃO DE ESPORTE, LAZER E JUVENTUDE";"850";
LEFT(INDIRECT("C"&ROW());50)="COMISSÃO DE FISCALIZAÇÃO FINANCEIRA E ORÇAMENTÁRIA";"10";
LEFT(INDIRECT("C"&ROW());55)="COMISSÃO DE MEIO AMBIENTE E DESENVOLVIMENTO SUSTENTÁVEL";"799";
LEFT(INDIRECT("C"&ROW());27)="COMISSÃO DE MINAS E ENERGIA";"800";
LEFT(INDIRECT("C"&ROW());32)="COMISSÃO DE PARTICIPAÇÃO POPULAR";"585";
LEFT(INDIRECT("C"&ROW());63)="COMISSÃO DE PREVENÇÃO E COMBATE AO USO DE CRACK E OUTRAS DROGAS";"959";
LEFT(INDIRECT("C"&ROW());19)="COMISSÃO DE REDAÇÃO";"13";
LEFT(INDIRECT("C"&ROW());17)="COMISSÃO DE SAÚDE";"14";
LEFT(INDIRECT("C"&ROW());29)="COMISSÃO DE SEGURANÇA PÚBLICA";"508";
LEFT(INDIRECT("C"&ROW());60)="COMISSÃO DO TRABALHO, DA PREVIDÊNCIA E DA ASSISTÊNCIA SOCIAL";"1076";
LEFT(INDIRECT("C"&ROW());52)="COMISSÃO DE TRANSPORTE, COMUNICAÇÃO E OBRAS PÚBLICAS";"12";
LEFT(INDIRECT("C"&ROW());17)="COMISSÃO DE ÉTICA";"578";
LEFT(INDIRECT("C"&ROW());71)="COMISSÃO EXTRAORDINÁRIA DAS ENERGIAS RENOVÁVEIS E DOS RECURSOS HÍDRICOS";"1211";
LEFT(INDIRECT("C"&ROW());62)="COMISSÃO EXTRAORDINÁRIA DE ACOMPANHAMENTO DO ACORDO DE MARIANA";"1232";
LEFT(INDIRECT("C"&ROW());66)="COMISSÃO EXTRAORDINÁRIA DE DEFESA DA HABITAÇÃO E DA REFORMA URBANA";"1260";
LEFT(INDIRECT("C"&ROW());62)="COMISSÃO EXTRAORDINÁRIA DE PREVENÇÃO E ENFRENTAMENTO AO CÂNCER";"1258";
LEFT(INDIRECT("C"&ROW());47)="COMISSÃO EXTRAORDINÁRIA DE PROTEÇÃO AOS ANIMAIS";"1230";
LEFT(INDIRECT("C"&ROW());41)="COMISSÃO EXTRAORDINÁRIA DAS PRIVATIZAÇÕES";"1212";
LEFT(INDIRECT("C"&ROW());48)="COMISSÃO EXTRAORDINÁRIA DE TURISMO E GASTRONOMIA";"1261";
LEFT(INDIRECT("C"&ROW());46)="COMISSÃO EXTRAORDINÁRIA PRÓ-FERROVIAS MINEIRAS";"1217";
LEFT(INDIRECT("C"&ROW());15)="GRANDE COMISSÃO";"10";
LEFT(INDIRECT("C"&ROW());53)="COMISSÃO DE PROPOSTA DE EMENDA À CONSTITUIÇÃO 42 2024";"1279";
LEFT(INDIRECT("C"&ROW());53)="COMISSÃO DE PROPOSTA DE EMENDA À CONSTITUIÇÃO 24 2023";"1280";
LEFT(INDIRECT("C"&ROW());53)="COMISSÃO DE PROPOSTA DE EMENDA À CONSTITUIÇÃO 58 2025";"1281";
LEFT(INDIRECT("C"&ROW());45)="COMISSÃO DE MEMBROS DAS COMISSÕES PERMANENTES";"10";
RIGHT(INDIRECT("C"&ROW());9)="PCD + SPU";959;
RIGHT(INDIRECT("C"&ROW());9)="CTU + DEC";675;
LEFT(INDIRECT("C"&ROW());16)="REUNIÃO CONJUNTA";"1";
LEFT(INDIRECT("C"&ROW());4)="CIPE";"811";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 18 2025";"1265";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 19 2025";"1264";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 20 2025";"1267";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 21 2025";"1262";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 22 2025";"1266";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 23 2025";"1263";
LEFT(INDIRECT("C"&ROW());24)="COMISSÃO DE VETO 24 2025";"1270"
)&"&dia="&IFS(MID($A$5;2;1)="/";LEFT($A$5;1);MID($A$5;2;1)<>"/";LEFT($A$5;2))&"&mes="&IFS(MID($A$5;3;1)="/";IFS(MID($A$5;4;1)<>"1";RIGHT($A$5;1);MID($A$5;4;1)="1";IFS(MID($A$5;5;1)="";RIGHT($A$5;1);MID($A$5;5;1)<>"";RIGHT($A$5;2)));MID($A$5;2;1)="/";IFS(MID($A$5;3;1)<>"1";RIGHT($A$5;1);MID($A$5;3;1)="1";IFS(MID($A$5;4;1)="";RIGHT($A$5;1);MID($A$5;4;1)<>"";RIGHT($A$5;2))))&"&ano="&RIGHT($B$6;4)&"&hr="&TO_TEXT(INDIRECT("B"&ROW()))&"&tpCom="&IFS(LEFT(INDIRECT("C"&ROW());45)="COMISSÃO DE MEMBROS DAS COMISSÕES PERMANENTES";"3";LEFT(INDIRECT("C"&ROW());45)<>"COMISSÃO DE MEMBROS DAS COMISSÕES PERMANENTES";"2")&"&aba=js_tabResultado";
IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));



OR(LEFT(INDIRECT("C"&ROW());5)="RQC: ");HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/reuniao/?idTipo="
&IFS(
OR(RIGHT(INDIRECT("C"&ROW());11)="GASTRONOMIA";RIGHT(INDIRECT("C"&ROW());6)="URBANA");"2";
OR(RIGHT(INDIRECT("C"&ROW());14)="EXTRAORDINÁRIA";RIGHT(INDIRECT("C"&ROW());25)="EXTRAORDINÁRIA, APROVADOS";RIGHT(INDIRECT("C"&ROW());26)="EXTRAORDINÁRIA - APROVADOS";RIGHT(INDIRECT("C"&ROW());25)="EXTRAORDINÁRIA, RECEBIDOS";RIGHT(INDIRECT("C"&ROW());26)="EXTRAORDINÁRIA - RECEBIDOS";RIGHT(INDIRECT("C"&ROW());37)="EXTRAORDINÁRIA, RECEBIDOS E APROVADOS";RIGHT(INDIRECT("C"&ROW());38)="EXTRAORDINÁRIA - RECEBIDOS E APROVADOS";MID(INDIRECT("C"&ROW());13;8)="PROPOSTA";RIGHT(INDIRECT("C"&ROW());7)="ANIMAIS";RIGHT(INDIRECT("C"&ROW());6)="CÂNCER";RIGHT(INDIRECT("C"&ROW());7)="MARIANA");"2";
OR(LEFT(INDIRECT("C"&ROW());6)="GRANDE";LEFT(INDIRECT("C"&ROW());7)="REUNIÃO";RIGHT(INDIRECT("C"&ROW());11)="PERMANENTES";RIGHT(INDIRECT("C"&ROW());8)="CONJUNTA";RIGHT(INDIRECT("C"&ROW());19)="CONJUNTA, APROVADOS";RIGHT(INDIRECT("C"&ROW());19)="CONJUNTA, RECEBIDOS");"3";
OR(MID(INDIRECT("C"&ROW());10;14)="EXTRAORDINÁRIA";RIGHT(INDIRECT("C"&ROW());8)="ESPECIAL");"5";
LEFT(INDIRECT("C"&ROW());4)="CIPE";"6";
RIGHT(INDIRECT("C"&ROW());14)<>"EXTRAORDINÁRIA";"1")
&"&idCom="
&IFS(
LEFT(INDIRECT("C"&ROW());26)="RQC: ADMINISTRAÇÃO PÚBLICA";"1";
LEFT(INDIRECT("C"&ROW());33)="RQC: AGROPECUÁRIA E AGROINDÚSTRIA";"1075";
LEFT(INDIRECT("C"&ROW());41)="RQC: ASSUNTOS MUNICIPAIS E REGIONALIZAÇÃO";"3";
LEFT(INDIRECT("C"&ROW());27)="RQC: CONSTITUIÇÃO E JUSTIÇA";"5";
LEFT(INDIRECT("C"&ROW());12)="RQC: CULTURA";"675";
LEFT(INDIRECT("C"&ROW());43)="RQC: DEFESA DO CONSUMIDOR E DO CONTRIBUINTE";"489";
LEFT(INDIRECT("C"&ROW());34)="RQC: DEFESA DOS DIREITOS DA MULHER";"1132";
LEFT(INDIRECT("C"&ROW());50)="RQC: DEFESA DOS DIREITOS DA PESSOA COM DEFICIÊNCIA";"859";
LEFT(INDIRECT("C"&ROW());30)="RQC: DESENVOLVIMENTO ECONÔMICO";"1077";
LEFT(INDIRECT("C"&ROW());21)="RQC: DIREITOS HUMANOS";"8";
LEFT(INDIRECT("C"&ROW());35)="RQC: EDUCAÇÃO, CIÊNCIA E TECNOLOGIA";"849";
LEFT(INDIRECT("C"&ROW());31)="RQC: ESPORTE, LAZER E JUVENTUDE";"850";
LEFT(INDIRECT("C"&ROW());43)="RQC: FISCALIZAÇÃO FINANCEIRA E ORÇAMENTÁRIA";"10";
LEFT(INDIRECT("C"&ROW());48)="RQC: MEIO AMBIENTE E DESENVOLVIMENTO SUSTENTÁVEL";"799";
LEFT(INDIRECT("C"&ROW());20)="RQC: MINAS E ENERGIA";"800";
LEFT(INDIRECT("C"&ROW());25)="RQC: PARTICIPAÇÃO POPULAR";"585";
LEFT(INDIRECT("C"&ROW());56)="RQC: PREVENÇÃO E COMBATE AO USO DE CRACK E OUTRAS DROGAS";"959";
LEFT(INDIRECT("C"&ROW());12)="RQC: REDAÇÃO";"13";
LEFT(INDIRECT("C"&ROW());10)="RQC: SAÚDE";"14";
LEFT(INDIRECT("C"&ROW());22)="RQC: SEGURANÇA PÚBLICA";"508";
LEFT(INDIRECT("C"&ROW());53)="RQC: TRABALHO, DA PREVIDÊNCIA E DA ASSISTÊNCIA SOCIAL";"1076";
LEFT(INDIRECT("C"&ROW());45)="RQC: TRANSPORTE, COMUNICAÇÃO E OBRAS PÚBLICAS";"12";
LEFT(INDIRECT("C"&ROW());67)="RQC: EXTRAORDINÁRIA DAS ENERGIAS RENOVÁVEIS E DOS RECURSOS HÍDRICOS";"1211";
LEFT(INDIRECT("C"&ROW());58)="RQC: EXTRAORDINÁRIA DE ACOMPANHAMENTO DO ACORDO DE MARIANA";"1232";
LEFT(INDIRECT("C"&ROW());62)="RQC: EXTRAORDINÁRIA DE DEFESA DA HABITAÇÃO E DA REFORMA URBANA";"1260";
LEFT(INDIRECT("C"&ROW());58)="RQC: EXTRAORDINÁRIA DE PREVENÇÃO E ENFRENTAMENTO AO CÂNCER";"1258";
LEFT(INDIRECT("C"&ROW());43)="RQC: EXTRAORDINÁRIA DE PROTEÇÃO AOS ANIMAIS";"1230";
LEFT(INDIRECT("C"&ROW());37)="RQC: EXTRAORDINÁRIA DAS PRIVATIZAÇÕES";"1212";
LEFT(INDIRECT("C"&ROW());44)="RQC: EXTRAORDINÁRIA DE TURISMO E GASTRONOMIA";"1261";
LEFT(INDIRECT("C"&ROW());42)="RQC: EXTRAORDINÁRIA PRÓ-FERROVIAS MINEIRAS";"1217";
LEFT(INDIRECT("C"&ROW());38)="RQC: PROPOSTA DE EMENDA À CONSTITUIÇÃO";"1234";
LEFT(INDIRECT("C"&ROW());38)="RQC: PROPOSTA DE EMENDA À CONSTITUIÇÃO";"1227";
LEFT(INDIRECT("C"&ROW());38)="RQC: PROPOSTA DE EMENDA À CONSTITUIÇÃO";"1218";
LEFT(INDIRECT("C"&ROW());45)="RQC: MEMBROS DAS COMISSÕES PERMANENTES";"10";
LEFT(INDIRECT("C"&ROW());18)="RQC: CIPE RIO DOCE";"811"
)&"&dia="&IFS(MID($A$5;2;1)="/";LEFT($A$5;1);MID($A$5;2;1)<>"/";LEFT($A$5;2))&"&mes="&IFS(MID($A$5;3;1)="/";IFS(MID($A$5;4;1)<>"1";RIGHT($A$5;1);MID($A$5;4;1)="1";IFS(MID($A$5;5;1)="";RIGHT($A$5;1);MID($A$5;5;1)<>"";RIGHT($A$5;2)));MID($A$5;2;1)="/";IFS(MID($A$5;3;1)<>"1";RIGHT($A$5;1);MID($A$5;3;1)="1";IFS(MID($A$5;4;1)="";RIGHT($A$5;1);MID($A$5;4;1)<>"";RIGHT($A$5;2))))&"&ano="&RIGHT($B$6;4)&"&hr="&TO_TEXT(INDIRECT("B"&ROW()))&"&tpCom="&IFS(LEFT(INDIRECT("C"&ROW());45)="RQC: MEMBROS DAS COMISSÕES PERMANENTES";"3";LEFT(INDIRECT("C"&ROW());45)<>"RQC: MEMBROS DAS COMISSÕES PERMANENTES";"2")&"&aba=js_tabResultado";
IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));



OR(LEFT(INDIRECT("C"&ROW());19)="AUDIÊNCIA PÚBLICA: ");HYPERLINK("https://www.almg.gov.br/atividade-parlamentar/comissoes/reuniao/?idTipo="
&IFS(
OR(RIGHT(INDIRECT("C"&ROW());11)="GASTRONOMIA";RIGHT(INDIRECT("C"&ROW());6)="URBANA");"2";
OR(RIGHT(INDIRECT("C"&ROW());14)="EXTRAORDINÁRIA";RIGHT(INDIRECT("C"&ROW());25)="EXTRAORDINÁRIA, APROVADOS";RIGHT(INDIRECT("C"&ROW());25)="EXTRAORDINÁRIA, RECEBIDOS";MID(INDIRECT("C"&ROW());13;8)="PROPOSTA";RIGHT(INDIRECT("C"&ROW());7)="ANIMAIS";RIGHT(INDIRECT("C"&ROW());6)="CÂNCER";RIGHT(INDIRECT("C"&ROW());7)="MARIANA");"2";
OR(LEFT(INDIRECT("C"&ROW());6)="GRANDE";LEFT(INDIRECT("C"&ROW());7)="REUNIÃO";RIGHT(INDIRECT("C"&ROW());11)="PERMANENTES";RIGHT(INDIRECT("C"&ROW());8)="CONJUNTA";RIGHT(INDIRECT("C"&ROW());19)="CONJUNTA, APROVADOS";RIGHT(INDIRECT("C"&ROW());19)="CONJUNTA, RECEBIDOS");"3";
OR(MID(INDIRECT("C"&ROW());10;14)="EXTRAORDINÁRIA";RIGHT(INDIRECT("C"&ROW());8)="ESPECIAL");"5";
LEFT(INDIRECT("C"&ROW());4)="CIPE";"6";
RIGHT(INDIRECT("C"&ROW());14)<>"EXTRAORDINÁRIA";"1")
&"&idCom="
&IFS(
MID(INDIRECT("C"&ROW());20;3)="APU";"1";
MID(INDIRECT("C"&ROW());20;3)="AAG";"1075";
MID(INDIRECT("C"&ROW());20;3)="AMR";"3";
MID(INDIRECT("C"&ROW());20;3)="CJU";"5";
MID(INDIRECT("C"&ROW());20;3)="CTU";"675";
MID(INDIRECT("C"&ROW());20;3)="DCC";"489";
MID(INDIRECT("C"&ROW());20;3)="DDM";"1132";
MID(INDIRECT("C"&ROW());20;3)="DPD";"859";
MID(INDIRECT("C"&ROW());20;3)="DEC";"1077";
MID(INDIRECT("C"&ROW());20;3)="DHU";"8";
MID(INDIRECT("C"&ROW());20;3)="ECT";"849";
MID(INDIRECT("C"&ROW());20;3)="ELJ";"850";
MID(INDIRECT("C"&ROW());20;3)="FFO";"10";
MID(INDIRECT("C"&ROW());20;3)="MAD";"799";
MID(INDIRECT("C"&ROW());20;3)="MEN";"800";
MID(INDIRECT("C"&ROW());20;3)="PPO";"585";
MID(INDIRECT("C"&ROW());20;3)="PCD";"959";
MID(INDIRECT("C"&ROW());20;3)="RED";"13";
MID(INDIRECT("C"&ROW());20;3)="SAU";"14";
MID(INDIRECT("C"&ROW());20;3)="SPU";"508";
MID(INDIRECT("C"&ROW());20;3)="TPA";"1076";
MID(INDIRECT("C"&ROW());20;3)="TCO";"12"
)&"&dia="&MID(INDIRECT("C"&ROW());25;2)
&"&mes="&MID(INDIRECT("C"&ROW());28;2)
&"&ano="&MID(INDIRECT("C"&ROW());31;4)
&"&hr="&TO_TEXT(INDIRECT("B"&ROW()))&"&tpCom="&IFS(LEFT(INDIRECT("C"&ROW());45)="RQC: MEMBROS DAS COMISSÕES PERMANENTES";"3";LEFT(INDIRECT("C"&ROW());45)<>"RQC: MEMBROS DAS COMISSÕES PERMANENTES";"2")&"&aba=js_tabResultado";
IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15));



OR(INDIRECT("C"&ROW())<>"REUNIÕES DE PLENÁRIO");
HYPERLINK("https://www.almg.gov.br/export/sites/default/consulte/arquivo_diario_legislativo/pdfs/"&RIGHT($B$6;4)&"/"&MID($B$6;4;2)&"/L"&RIGHT($B$6;4)&MID($B$6;4;2)&LEFT($B$6;2)&".pdf#page="&IFS(MID(INDIRECT("B"&ROW());3;1)="";IFS(LEFT(INDIRECT("B"&ROW());1)=0;LEFT(INDIRECT("B"&ROW());2);LEFT(INDIRECT("B"&ROW());1)<>0;LEFT(INDIRECT("B"&ROW());2));MID(INDIRECT("B"&ROW());3;1)<>"";LEFT(INDIRECT("B"&ROW());3));

IMAGE("https://seeklogo.com/images/B/bandeira-minas-gerais-logo-AD7B6F3604-seeklogo.com.png";4;15;15))
))''']] * (rows_needed - 5))

    # Fórmulas Q..Y
    data += [
        {"range": f"'{tab_name}'!Q2", "values": [["=B6"]]},
        {"range": f"'{tab_name}'!Q3", "values": [["=TODAY()"]]},
        {"range": f"'{tab_name}'!Q4", "values": [['=QUERY(C6:G8;"SELECT E WHERE C MATCHES \'.*DIÁRIO DO LEGISLATIVO.*\'";0)']]},
        {"range": f"'{tab_name}'!S2", "values": [['=TEXT(DATE(RIGHT($B$6;4);MID($B$6;4;2);LEFT($B$6;2));"\'dd\' \'MM\' yyyy")']]},
        {"range": f"'{tab_name}'!S3", "values": [['=TEXT(TODAY();"\'d\' \'MM\' yyyy")']]},
        {"range": f"'{tab_name}'!S4", "values": [['=IFERROR(TEXT(DATE(RIGHT($Q$4;4);MID($Q$4;4;2);LEFT($Q$4;2));"\'dd\' \'MM\' yyyy");"")']]},
        {"range": f"'{tab_name}'!T2", "values": [[r'="''"&IFS(LEFT($B$6;1)="0";MID($B$6;2;1);LEFT($B$6;1)<>"0";LEFT($B$6;2))&"'' ''"&IFS(MID($B$6;4;1)="0";MID($B$6;5;1);MID($B$6;4;1)<>"0";MID($B$6;4;2))&"'' "&RIGHT($B$6;4)']]},
        {"range": f"'{tab_name}'!T3", "values": [[r'="''"&IFS(LEFT($Q$3;1)="0";MID($Q$3;2;1);LEFT($Q$3;1)<>"0";LEFT($Q$3;2))&"'' ''"&IFS(MID($Q$3;4;1)="0";MID($Q$3;5;1);MID($Q$3;4;1)<>"0";MID($Q$3;4;2))&"'' "&RIGHT($Q$3;4)']]},
        {"range": f"'{tab_name}'!T4", "values": [[r'="''"&IFS(LEFT($Q$4;1)="0";MID($Q$4;2;1);LEFT($Q$4;1)<>"0";LEFT($Q$4;2))&"'' ''"&IFS(MID($Q$4;4;1)="0";MID($Q$4;5;1);MID($Q$4;4;1)<>"0";MID($Q$4;4;2))&"'' "&RIGHT($Q$4;4)']]},
        {"range": f"'{tab_name}'!U2", "values": [["=B6"]]},
        {"range": f"'{tab_name}'!U3", "values": [["=Q3"]]},
        {"range": f"'{tab_name}'!V2", "values": [["=Q2"]]},
        {"range": f"'{tab_name}'!W2", "values": [[r'="''"&TEXT(DATE(RIGHT($B$6;4);MID($B$6;4;2);LEFT($B$6;2))-1;"d MM yyyy")&"''"']]},
        {"range": f"'{tab_name}'!W3", "values": [['=IFERROR(QUERY(C6:G13;"SELECT E WHERE C MATCHES ''.*DIÁRIO DO LEGISLATIVO - EDIÇÃO EXTRA.*''";0);"SEM EXTRA")']]},
        {"range": f"'{tab_name}'!W4", "values": [['=IFERROR("\'"&TEXT(QUERY(B6:G33;"SELECT B WHERE C MATCHES ''REQUERIMENTOS DE COMISSÃO''";0);"dd MM yyyy")&"\'";"")']]},
        {"range": f"'{tab_name}'!X4", "values": [['=IFERROR(TEXT(QUERY(B6:G33;"SELECT B WHERE C MATCHES ''REQUERIMENTOS DE COMISSÃO''";0);"dd/MM/yyyy");"")']]},
        {"range": f"'{tab_name}'!Y2", "values": [["REUNIÃO"]]},
        {"range": f"'{tab_name}'!Y3", "values": [["EXTRA"]]},
        {"range": f"'{tab_name}'!Y4", "values": [["RQC"]]},
    ]

    # ✅ EXECUTA O BLOCO PRINCIPAL (o que você apagou antes)
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    _with_backoff(sh.values_batch_update, body)

    # ==========================
    # TÍTULOS (B/C) + EXTRAS
    # ==========================
    data2 = []
    data2.append({"range": f"'{tab_name}'!B8:C8", "values": [[tab_name, "DIÁRIO DO LEGISLATIVO"]]})

    if itens:
        data2.append({
            "range": f"'{tab_name}'!B9:C{9 + len(itens) - 1}",
            "values": [[a, b] for a, b in itens]
        })

    # NÃO recalcula start_extra_row aqui
    # NÃO redefine extras aqui

    data2.append({
        "range": f"'{tab_name}'!B{start_extra_row}:C{start_extra_row + len(extras) - 1}",
        "values": extras
    })

    body2 = {"valueInputOption": "USER_ENTERED", "data": data2}
    _with_backoff(sh.values_batch_update, body2)

    return sh.url, ws.title


# ---- CHAMADA ----
SPREADSHEET = "https://docs.google.com/spreadsheets/d/1HKM8MxedZF8uS_Art5pn1MlZjSiEtXZGSKbRRHunUtE/edit"
diario_key = yyyymmdd  # já existe no seu fluxo

url, aba = upsert_tab_diario(
    spreadsheet_url_or_id=SPREADSHEET,
    diario_key=diario_key,
    itens=itens,
    clear_first=False,
    default_col_width_px=COL_DEFAULT,
    col_width_overrides=COL_OVERRIDES
)

print("Planilha atualizada:", url)
print("Aba:", aba)
