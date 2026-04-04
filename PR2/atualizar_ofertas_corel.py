#!/usr/bin/env python
"""
Atualiza 4 ofertas em um template do CorelDRAW (.cdr).

Requisitos:
- Windows
- CorelDRAW instalado
- Python 3.10+
- pywin32 (`pip install pywin32`)
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from itertools import combinations, permutations
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import win32com.client  # type: ignore
except ImportError as exc:
    raise SystemExit(
        "pywin32 nao encontrado. Instale com: pip install pywin32"
    ) from exc

# Corel enums (valores estaveis)
CDR_TEXT_SHAPE = 6
CDR_GROUP_SHAPE = 7
CDR_OLE_SHAPE = 12
CDR_UNIT_CENTIMETER = 4
CDR_REF_CENTER = 9

def obter_app_corel():
    try:
        app = win32com.client.GetActiveObject("CorelDRAW.Application")
    except Exception:
        app = win32com.client.Dispatch("CorelDRAW.Application")
    app.Visible = True
    return app


def definir_visibilidade_shape(shape: object, visivel: bool) -> None:
    if shape is None:
        return
    try:
        shape.Visible = bool(visivel)
    except Exception:
        pass


@dataclass
class Produto:
    descricao: str
    unidade: str
    preco_inteiro: str
    preco_decimal: str
    validade_oferta: str = ""
    codigo_barras: str = ""
    usar_codigo_barras: bool = False


@dataclass
class ShapeInfo:
    shape: object
    name: str
    text: str
    x: float
    y: float
    w: float
    h: float


_MAPA_DOCUMENTO_CACHE: Dict[str, Tuple[object, Dict[str, object]]] = {}


def carregar_config(caminho: Path) -> dict:
    with caminho.open("r", encoding="utf-8-sig") as f:
        dados = json.load(f)
    if not isinstance(dados, dict):
        raise ValueError("Arquivo JSON invalido: raiz deve ser um objeto")
    return dados


def normalizar_preco(valor: object) -> Tuple[str, str]:
    bruto = str(valor).strip().replace("R$", "").replace(" ", "")
    bruto = bruto.replace(".", ",")
    if not bruto:
        raise ValueError("Preco vazio")

    if "," in bruto:
        inteiro, decimal = bruto.split(",", 1)
    else:
        inteiro, decimal = bruto, "00"

    inteiro = re.sub(r"\D", "", inteiro)
    decimal = re.sub(r"\D", "", decimal)

    if not inteiro:
        inteiro = "0"
    if not decimal:
        decimal = "00"
    decimal = (decimal + "00")[:2]

    return inteiro, decimal


def normalizar_codigo_barras(valor: object) -> str:
    digitos = re.sub(r"\D", "", str(valor or ""))
    if not digitos:
        return ""
    if len(digitos) not in (12, 13):
        raise ValueError("Codigo de barras deve ter 12 ou 13 digitos.")
    return digitos


def calcular_digito_ean13(base_12: str) -> int:
    soma = 0
    for idx, digito in enumerate(base_12[:12], start=1):
        soma += int(digito) * (3 if (idx % 2) == 0 else 1)
    return (10 - (soma % 10)) % 10


def preparar_codigo_barras_ean13(valor: object) -> str:
    codigo = normalizar_codigo_barras(valor)
    if not codigo:
        return ""
    base = codigo[:12]
    return f"{base}{calcular_digito_ean13(base)}"


def normalizar_flag_codigo_barras(valor: object, codigo_barras: object = "") -> bool:
    if isinstance(valor, bool):
        return valor
    txt = str(valor or "").strip().lower()
    if txt in {"0", "false", "falso", "nao", "não", "off", "desativado"}:
        return False
    if txt in {"1", "true", "verdadeiro", "sim", "on", "ativado"}:
        return True
    return bool(normalizar_codigo_barras(codigo_barras))


def parse_produtos(config: dict) -> List[Produto]:
    produtos_brutos = config.get("produtos")
    if not isinstance(produtos_brutos, list) or len(produtos_brutos) != 4:
        raise ValueError("O JSON precisa conter exatamente 4 produtos em 'produtos'.")

    permitir_vazio = bool(config.get("permitir_produto_vazio", False))
    produtos: List[Produto] = []
    for i, item in enumerate(produtos_brutos, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Produto {i} invalido: esperado objeto.")

        descricao = str(item.get("descricao", "")).strip()
        unidade = str(item.get("unidade", "")).strip()
        validade_oferta = str(item.get("validade_oferta", "")).strip()

        if not descricao:
            if not permitir_vazio:
                raise ValueError(f"Produto {i} sem descricao.")
            produtos.append(
                Produto(
                    descricao="",
                    unidade=unidade,
                    preco_inteiro="",
                    preco_decimal="",
                    validade_oferta="",
                    codigo_barras="",
                    usar_codigo_barras=False,
                )
            )
            continue

        if not unidade:
            unidade = "Unid."

        try:
            preco_i, preco_d = normalizar_preco(item.get("preco", ""))
        except ValueError:
            if not permitir_vazio:
                raise
            preco_i, preco_d = "", ""

        codigo_barras = normalizar_codigo_barras(item.get("codigo_barras", ""))
        produtos.append(
            Produto(
                descricao=descricao,
                unidade=unidade,
                preco_inteiro=preco_i,
                preco_decimal=preco_d,
                validade_oferta=validade_oferta,
                codigo_barras=codigo_barras,
                usar_codigo_barras=normalizar_flag_codigo_barras(item.get("usar_codigo_barras", True), codigo_barras),
            )
        )

    return produtos


def snapshot_shapes(shapes_collection) -> List[object]:
    shapes: List[object] = []
    try:
        count = int(shapes_collection.Count)
    except Exception:
        return shapes

    for i in range(1, count + 1):
        try:
            shapes.append(shapes_collection.Item(i))
        except Exception:
            # O Corel pode reordenar/remover shapes enquanto o documento e atualizado.
            # Nesse caso, segue com o snapshot parcial em vez de abortar a automacao.
            continue
    return shapes


def iter_shapes(shapes_collection) -> Iterable[object]:
    for shp in snapshot_shapes(shapes_collection):
        yield shp
        try:
            if int(shp.Type) == CDR_GROUP_SHAPE:
                yield from iter_shapes(shp.Shapes)
        except Exception:
            continue


def iter_top_level_shapes(page) -> Iterable[object]:
    yield from snapshot_shapes(page.Shapes)


def get_story_text(shape: object) -> str:
    try:
        txt = str(shape.Text.Story)
    except Exception:
        try:
            txt = str(shape.Text.Story.Text)
        except Exception:
            txt = ""
    return txt.strip()


def set_story_text(shape: object, value: str) -> None:
    try:
        shape.Text.Story.Text = value
        return
    except Exception:
        pass
    shape.Text.Story = value


def set_story_font_size(
    shape: object,
    font_name: Optional[str] = None,
    size_pt: Optional[float] = None,
    reset_vert_shift: bool = False,
) -> None:
    story = shape.Text.Story
    if font_name:
        story.Font = font_name
    if size_pt is not None:
        story.Size = float(size_pt)

    try:
        texto = str(story.Text)
    except Exception:
        try:
            texto = str(story)
        except Exception:
            texto = ""

    if not texto:
        return

    try:
        rng = story.Range(0, len(texto))
        if font_name:
            rng.Font = font_name
        if size_pt is not None:
            rng.Size = float(size_pt)
        if reset_vert_shift:
            rng.VertShift = 0
    except Exception:
        pass


def montar_texto_validade_oferta(texto_atual: str, data_oferta: str) -> str:
    data_limpa = str(data_oferta or "").strip()
    if not data_limpa:
        return ""

    texto_base = str(texto_atual or "").strip()
    if not texto_base:
        return f"Oferta Valida ate o dia {data_limpa}\rou enquanto durarem os estoques"

    token = normalizar_token_placeholder(texto_base)
    base, _ = extrair_base_e_indice_token(token)
    if campo_por_base_token(base) == "validade":
        return f"Oferta Valida ate o dia {data_limpa}\rou enquanto durarem os estoques"

    atualizado = re.sub(
        r"(?:XX|DD|\d{1,2})[/-](?:XX|MM|\d{1,2})[/-](?:XXXX|AAAA|\d{2,4})",
        data_limpa,
        texto_base,
        count=1,
        flags=re.IGNORECASE,
    )
    if atualizado != texto_base:
        return atualizado

    return f"Oferta Valida ate o dia {data_limpa}\rou enquanto durarem os estoques"


def to_shape_info(shape: object) -> ShapeInfo:
    name = ""
    try:
        name = str(shape.Name).strip()
    except Exception:
        pass

    text = get_story_text(shape)
    x = float(shape.PositionX)
    y = float(shape.PositionY)
    w = float(shape.SizeWidth)
    h = float(shape.SizeHeight)
    return ShapeInfo(shape=shape, name=name, text=text, x=x, y=y, w=w, h=h)


def listar_textos(page) -> List[ShapeInfo]:
    infos: List[ShapeInfo] = []
    for shp in iter_shapes(page.Shapes):
        try:
            if int(shp.Type) != CDR_TEXT_SHAPE:
                continue
        except Exception:
            continue
        infos.append(to_shape_info(shp))
    return infos


def encontrar_shapes_por_nome(page, nome: str) -> List[object]:
    nome_cmp = str(nome).strip().lower()
    encontrados: List[object] = []
    if not nome_cmp:
        return encontrados

    for shp in iter_shapes(page.Shapes):
        try:
            atual = str(shp.Name).strip().lower()
        except Exception:
            continue
        if atual and atual == nome_cmp:
            encontrados.append(shp)
    return encontrados


def get_by_name(page, nome: str):
    nome_cmp = str(nome).strip().lower()
    try:
        shp = page.FindShape("@name='{}'".format(nome.replace("'", "''")))
        if shp is not None:
            try:
                atual = str(shp.Name).strip().lower()
                if not nome_cmp or atual == nome_cmp:
                    return shp
            except Exception:
                return shp
    except Exception:
        pass

    # Fallback robusto: varre todos os textos/grupos quando FindShape nao resolve.
    for shp in iter_shapes(page.Shapes):
        try:
            atual = str(shp.Name).strip().lower()
        except Exception:
            continue
        if atual and atual == nome_cmp:
            return shp
    return None


def get_unique_by_name(page, nome: str):
    encontrados = encontrar_shapes_por_nome(page, nome)
    if len(encontrados) == 1:
        return encontrados[0]
    return None


def delete_shapes_by_name(page, nome: str) -> None:
    for shp in encontrar_shapes_por_nome(page, nome):
        try:
            _try_delete_shape(shp)
        except Exception:
            continue


def nomes_slot_barcode(indice: int) -> Tuple[str, ...]:
    return (
        f"BARCODE_{indice}",
        f"BARCODE_SLOT_{indice}",
        f"CODIGO_BARRAS_{indice}",
        f"CODIGO_BARRAS_SLOT_{indice}",
        f"BARRAS_{indice}",
        f"EAN13_{indice}",
    )


def nomes_slot_icone_preco(indice: int) -> Tuple[str, ...]:
    return (
        f"PRECO_ICON_{indice}",
        f"ICONE_PRECO_{indice}",
        f"MOEDA_{indice}",
        f"RS_{indice}",
        f"CIFRAO_{indice}",
    )


def resolver_shape_slot_barcode(page: object, shape_slot: object, indice: int):
    for nome in nomes_slot_barcode(indice):
        shp = get_unique_by_name(page, nome)
        if shp is not None and _shape_tem_bbox_valido(shp):
            return shp
    if shape_slot is not None and _shape_tem_bbox_valido(shape_slot):
        return shape_slot
    return None


def tentar_mapa_por_nome(page) -> Optional[Dict[str, object]]:
    mapa: Dict[str, object] = {}
    for idx in range(1, 5):
        nome_desc = f"DESC_{idx}"
        nome_unid = f"UNID_{idx}"
        shp_desc = get_unique_by_name(page, nome_desc)
        shp_unid = get_unique_by_name(page, nome_unid)
        if shp_desc is None or shp_unid is None:
            return None

        mapa[f"desc_{idx}"] = shp_desc
        mapa[f"unid_{idx}"] = shp_unid

        shp_preco_full = get_unique_by_name(page, f"PRECO_{idx}")
        if shp_preco_full is not None:
            mapa[f"preco_full_{idx}"] = shp_preco_full
        else:
            shp_preco_int = get_unique_by_name(page, f"PRECO_INT_{idx}")
            shp_preco_dec = get_unique_by_name(page, f"PRECO_DEC_{idx}")
            if shp_preco_int is None or shp_preco_dec is None:
                return None
            mapa[f"preco_int_{idx}"] = shp_preco_int
            mapa[f"preco_dec_{idx}"] = shp_preco_dec

        for nome_barcode in (
            f"BARCODE_{idx}",
            f"BARCODE_SLOT_{idx}",
            f"CODIGO_BARRAS_{idx}",
            f"CODIGO_BARRAS_SLOT_{idx}",
            f"BARRAS_{idx}",
            f"EAN13_{idx}",
        ):
            shp_barcode = get_unique_by_name(page, nome_barcode)
            if shp_barcode is not None:
                mapa[f"barcode_{idx}"] = shp_barcode
                break

        for nome_validade in (
            f"VALIDADE_{idx}",
            f"VALIDADE_OFERTA_{idx}",
            f"DATA_VALIDADE_{idx}",
            f"DATA_OFERTA_{idx}",
            f"OFERTA_VALIDADE_{idx}",
        ):
            shp_validade = get_unique_by_name(page, nome_validade)
            if shp_validade is not None:
                mapa[f"validade_{idx}"] = shp_validade
                break
    return mapa


def normalizar_token_placeholder(texto: str) -> str:
    t = str(texto or "").strip()
    if not t:
        return ""
    while len(t) >= 2 and t.startswith("{") and t.endswith("}"):
        t = t[1:-1].strip()
    t = t.upper().replace("-", "_").replace(".", "")
    t = re.sub(r"\s+", "", t)
    t = re.sub(r"_+", "_", t).strip("_")
    return t


def extrair_base_e_indice_token(token: str) -> Tuple[str, Optional[int]]:
    m = re.fullmatch(r"([A-Z0-9_]+?)(?:_(\d{1,2}))?", token or "")
    if not m:
        return "", None
    base = str(m.group(1) or "").strip("_")
    idx_txt = m.group(2)
    if not idx_txt:
        m_fallback = re.fullmatch(r"([A-Z_]+?)(\d{1,2})", base or "")
        if m_fallback:
            base = str(m_fallback.group(1) or "").strip("_")
            idx_txt = m_fallback.group(2)
    if not idx_txt:
        return base, None
    try:
        idx = int(idx_txt)
    except Exception:
        return base, None
    if 1 <= idx <= 4:
        return base, idx
    return base, None


def campo_por_base_token(base: str) -> str:
    b = str(base or "").strip().upper()
    b_flat = b.replace("_", "")

    if b in {"DESC", "DESCRICAO", "NOME", "PRODUTO", "ITEM"} or b_flat in {
        "DESCRICAOPRODUTO",
        "NOMEPRODUTO",
        "NOMEDOPRODUTO",
        "DESCRICAODOPRODUTO",
    }:
        return "desc"

    if b in {"UNID", "UND", "UNIDADE", "MEDIDA", "UNIT"} or b_flat in {
        "TIPOUNIDADE",
        "TIPODEUNIDADE",
        "KGOUUNID",
        "UNIDADEMEDIDA",
        "UNIDADEDEMEDIDA",
        "UNITLABEL",
    }:
        return "unid"

    if b in {"PRECO", "PRICE", "VALOR", "OFERTA"} or b_flat in {"PRECOCOMPLETO"}:
        return "preco_full"

    if b in {"PRECO_INT", "PRECOINT", "INTEIRO"} or b_flat in {
        "PRECOINTEIRO",
        "PRECOPARTEINTEIRA",
        "VALORINTEIRO",
    }:
        return "preco_int"

    if b in {"PRECO_DEC", "PRECODEC", "CENTAVOS", "DECIMAL"} or b_flat in {
        "PRECOCENTAVOS",
        "VALORCENTAVOS",
    }:
        return "preco_dec"

    if b in {"BARCODE", "EAN13", "CODBARRAS", "CODIGO_BARRAS", "BARRAS"} or b_flat in {
        "CODIGODEBARRAS",
        "CODIGOBARRAS",
        "AREABARCODE",
        "AREACODIGOBARRAS",
    }:
        return "barcode"

    if b in {"VALIDADE", "DATA", "DATA_OFERTA", "VALIDADE_OFERTA"} or b_flat in {
        "DATAVALIDADE",
        "DATAOFERTA",
        "VALIDADEOFERTA",
        "OFERTAVALIDADE",
    }:
        return "validade"

    return ""


def ordenar_grade_campos(itens: List[ShapeInfo]) -> List[ShapeInfo]:
    if len(itens) < 4:
        return []
    try:
        return ordenar_grade_2x2(itens)
    except Exception:
        return sorted(itens, key=lambda s: (-s.y, s.x))[:4]


def mapa_completo(mapa: Dict[str, object]) -> bool:
    for idx in range(1, 5):
        if f"desc_{idx}" not in mapa or f"unid_{idx}" not in mapa:
            return False
        tem_full = f"preco_full_{idx}" in mapa
        tem_sep = f"preco_int_{idx}" in mapa and f"preco_dec_{idx}" in mapa
        if not tem_full and not tem_sep:
            return False
    return True


def chave_cache_documento(doc: object) -> str:
    try:
        return str(Path(str(doc.FullFileName)).resolve()).lower()
    except Exception:
        return f"doc:{id(doc)}"


def shape_acessivel(shape: object) -> bool:
    try:
        _ = float(shape.PositionX)
        _ = float(shape.PositionY)
        _ = float(shape.SizeWidth)
        _ = float(shape.SizeHeight)
        return True
    except Exception:
        return False


def mapa_campos_acessivel(page: object, mapa: Dict[str, object]) -> bool:
    try:
        _ = int(page.Index)
    except Exception:
        return False

    if not mapa_completo(mapa):
        return False

    for chave, shp in mapa.items():
        if not shape_acessivel(shp):
            return False
    return True


def mapa_tem_slots_icone_preco(mapa: Dict[str, object]) -> bool:
    return any(chave.startswith("preco_icon_") for chave in mapa)


def mapa_nomeado_consistente(mapa: Dict[str, object]) -> bool:
    ancoras: List[ShapeInfo] = []
    for idx in range(1, 5):
        shp = mapa.get(f"preco_full_{idx}") or mapa.get(f"preco_int_{idx}") or mapa.get(f"desc_{idx}")
        if shp is None or not shape_acessivel(shp):
            return False
        ancoras.append(
            ShapeInfo(
                shape=shp,
                name=str(idx),
                text="",
                x=float(shp.PositionX),
                y=float(shp.PositionY),
                w=float(shp.SizeWidth),
                h=float(shp.SizeHeight),
            )
        )

    try:
        ordenados = ordenar_grade_2x2(ancoras)
    except Exception:
        return False

    for posicao_esperada, info in enumerate(ordenados, start=1):
        try:
            indice_real = int(info.name)
        except Exception:
            return False
        if indice_real != posicao_esperada:
            return False
    return True


def mapear_indices_explicitos_por_texto(infos: List[ShapeInfo]) -> Dict[str, object]:
    mapa: Dict[str, object] = {}
    for info in infos:
        token = normalizar_token_placeholder(info.text)
        if not token:
            continue
        base, idx = extrair_base_e_indice_token(token)
        if idx is None:
            continue
        campo = campo_por_base_token(base)
        if not campo:
            continue
        mapa[f"{campo}_{idx}"] = info.shape
    return mapa


def tentar_mapa_hibrido(page) -> Optional[Dict[str, object]]:
    infos = listar_textos(page)
    if not infos:
        return None

    mapa = mapear_indices_explicitos_por_texto(infos)

    for idx in range(1, 5):
        mapa.setdefault(f"desc_{idx}", get_unique_by_name(page, f"DESC_{idx}"))
        mapa.setdefault(f"unid_{idx}", get_unique_by_name(page, f"UNID_{idx}"))

        shp_preco_full = get_unique_by_name(page, f"PRECO_{idx}")
        shp_preco_int = get_unique_by_name(page, f"PRECO_INT_{idx}")
        shp_preco_dec = get_unique_by_name(page, f"PRECO_DEC_{idx}")

        if shp_preco_full is not None:
            mapa.setdefault(f"preco_full_{idx}", shp_preco_full)
        else:
            mapa.setdefault(f"preco_int_{idx}", shp_preco_int)
            mapa.setdefault(f"preco_dec_{idx}", shp_preco_dec)

    mapa = {chave: shp for chave, shp in mapa.items() if shp is not None}
    if mapa_completo(mapa):
        return mapa

    try:
        mapa_auto = montar_mapa_automatico(page)
    except Exception:
        mapa_auto = None

    if mapa_auto is not None:
        for chave, shp in mapa_auto.items():
            mapa.setdefault(chave, shp)

    if mapa_completo(mapa):
        return mapa
    return None


def tentar_mapa_por_placeholder(page) -> Optional[Dict[str, object]]:
    infos = listar_textos(page)
    if not infos:
        return None

    mapa: Dict[str, object] = {}
    pools: Dict[str, List[ShapeInfo]] = {
        "desc": [],
        "unid": [],
        "preco_full": [],
        "preco_int": [],
        "preco_dec": [],
        "barcode": [],
        "validade": [],
    }

    for info in infos:
        token = normalizar_token_placeholder(info.text)
        if not token:
            continue

        base, idx = extrair_base_e_indice_token(token)
        if not base:
            continue

        campo = campo_por_base_token(base)
        if not campo:
            continue

        if idx is not None:
            mapa[f"{campo}_{idx}"] = info.shape
        else:
            pools[campo].append(info)

    if mapa_completo(mapa):
        return mapa

    # Preenche por grade apenas quando o campo nao tem nenhum indice explicito.
    for campo in ("desc", "unid", "preco_full", "preco_int", "preco_dec", "barcode", "validade"):
        chaves = [f"{campo}_{i}" for i in range(1, 5)]
        qtd_diretas = sum(1 for c in chaves if c in mapa)
        if qtd_diretas != 0:
            continue
        ordenados = ordenar_grade_campos(pools.get(campo, []))
        if len(ordenados) < 4:
            continue
        for idx, info in enumerate(ordenados[:4], start=1):
            mapa[f"{campo}_{idx}"] = info.shape

    if mapa_completo(mapa):
        return mapa
    return None


def classificar_textos(
    infos: List[ShapeInfo],
) -> Tuple[List[ShapeInfo], List[ShapeInfo], List[ShapeInfo], List[ShapeInfo], List[ShapeInfo]]:
    descs: List[ShapeInfo] = []
    unids: List[ShapeInfo] = []
    ints: List[ShapeInfo] = []
    decs: List[ShapeInfo] = []
    precos_full: List[ShapeInfo] = []

    for info in infos:
        t = info.text.replace("\n", " ").replace("\r", " ").strip()
        t_norm = re.sub(r"\s+", "", t.lower())
        t_alpha = re.sub(r"[^a-z]", "", t.lower())
        t_upper = re.sub(r"[^A-Z]", "", t)
        token = normalizar_token_placeholder(t)
        base_token, _ = extrair_base_e_indice_token(token)

        if not t:
            continue

        if campo_por_base_token(base_token) == "validade":
            continue

        if (
            len(t_norm) >= 12
            and (
                ("oferta" in t.lower() and ("valida" in t.lower() or "válida" in t.lower()))
                or "durarem os estoques" in t.lower()
                or re.search(
                    r"(?:xx|dd|\d{1,2})[/-](?:xx|mm|\d{1,2})[/-](?:xxxx|aaaa|\d{2,4})",
                    t.lower(),
                )
            )
        ):
            continue

        if t_alpha in ("unid", "und", "kg", "quilo", "quilos"):
            unids.append(info)
            continue

        # Alguns templates usam placeholders curtos em caixa alta, como "BDJ.".
        if (
            " " not in t
            and len(t_alpha) >= 2
            and len(t_alpha) <= 6
            and len(t_upper) == len(t_alpha)
            and info.w <= 2.5
            and info.h <= 0.9
        ):
            unids.append(info)
            continue

        if re.fullmatch(r"\d{1,4}[.,]\d{2}", t_norm):
            precos_full.append(info)
            continue

        # Em layout separado, os centavos normalmente ficam como ",99".
        if re.fullmatch(r"[.,]\d{2}", t_norm):
            decs.append(info)
            continue

        if re.fullmatch(r"\d{1,4}", t_norm):
            ints.append(info)
            continue

        if "r$" in t_norm:
            continue

        if re.search(r"[a-zA-Z]", t):
            descs.append(info)

    return descs, unids, ints, decs, precos_full


def distancia(a: ShapeInfo, b: ShapeInfo) -> float:
    return math.hypot(a.x - b.x, a.y - b.y)


def area_texto(info: ShapeInfo) -> float:
    return max(0.01, float(info.w)) * max(0.01, float(info.h))


def ordenar_grade_2x2(itens: List[ShapeInfo]) -> List[ShapeInfo]:
    if len(itens) < 4:
        raise ValueError("Quantidade insuficiente para grade 2x2")

    ranked = sorted(itens, key=lambda s: (area_texto(s), s.w, s.h), reverse=True)
    pool = ranked[: min(8, len(ranked))]
    if len(pool) < 4:
        raise ValueError("Nao ha candidatos suficientes para ancoras de preco")

    xs = [s.x for s in pool]
    ys = [s.y for s in pool]
    targets = [
        (min(xs), max(ys)),  # 1: topo esquerdo
        (max(xs), max(ys)),  # 2: topo direito
        (min(xs), min(ys)),  # 3: baixo esquerdo
        (max(xs), min(ys)),  # 4: baixo direito
    ]

    best_perm: Optional[Tuple[ShapeInfo, ShapeInfo, ShapeInfo, ShapeInfo]] = None
    best_score = float("inf")

    for combo in combinations(pool, 4):
        combo_area = sum(area_texto(s) for s in combo)
        for perm in permutations(combo, 4):
            dist_total = 0.0
            for idx, shp in enumerate(perm):
                tx, ty = targets[idx]
                dist_total += math.hypot(shp.x - tx, shp.y - ty)
            score = dist_total - (combo_area * 0.12)
            if score < best_score:
                best_score = score
                best_perm = perm

    if best_perm is None:
        raise ValueError("Nao foi possivel montar grade 2x2 de precos")
    return list(best_perm)


def escolher_por_score(candidatos: List[ShapeInfo], score_fn) -> ShapeInfo:
    if not candidatos:
        raise ValueError("Lista de candidatos vazia")
    melhor = min(candidatos, key=score_fn)
    candidatos.remove(melhor)
    return melhor


def montar_mapa_automatico(page) -> Dict[str, object]:
    infos = listar_textos(page)
    descs, unids, ints, decs, precos_full = classificar_textos(infos)
    tem_preco_full = len(precos_full) >= 4
    tem_preco_separado = len(ints) >= 4 and len(decs) >= 4

    if len(unids) < 4 or len(descs) < 4 or (not tem_preco_full and not tem_preco_separado):
        raise ValueError(
            "Nao consegui identificar automaticamente todos os campos. "
            "Nomeie os objetos como DESC_1..4, UNID_1..4 e PRECO_1..4 "
            "(ou PRECO_INT_1..4 + PRECO_DEC_1..4). "
            f"[detectado: desc={len(descs)} unid={len(unids)} int={len(ints)} "
            f"dec={len(decs)} preco_full={len(precos_full)}]"
        )

    base_precos = precos_full if tem_preco_full else ints
    base_precos_sorted = ordenar_grade_2x2(base_precos)
    mapa: Dict[str, object] = {}

    unids_pool = unids[:]
    descs_pool = descs[:]
    decs_pool = decs[:] if tem_preco_separado else []

    for idx, price_base in enumerate(base_precos_sorted, start=1):
        def score_desc(cand: ShapeInfo) -> float:
            score = distancia(price_base, cand)
            # Descricao deve ficar acima do preco.
            if cand.y < price_base.y:
                score += 45.0
            # Evita usar descricao de outro quadrante.
            if abs(cand.x - price_base.x) > 6.0:
                score += 10.0
            # Prefere textos mais longos/largos para descricao.
            txt_len = len(re.sub(r"\s+", "", cand.text))
            score -= min(txt_len, 50) * 0.08
            score -= min(cand.w, 12.0) * 0.4
            return score

        desc = escolher_por_score(descs_pool, score_desc)
        mapa[f"desc_{idx}"] = desc.shape

        if tem_preco_full:
            mapa[f"preco_full_{idx}"] = price_base.shape
            ref_unid = price_base
        else:
            def score_dec(cand: ShapeInfo) -> float:
                score = distancia(price_base, cand)
                # Decimal costuma estar a direita do inteiro.
                if cand.x < price_base.x:
                    score += 25.0
                # Evita decimais de outras linhas.
                if abs(cand.y - price_base.y) > 2.0:
                    score += 12.0
                return score

            dec = escolher_por_score(decs_pool, score_dec)
            mapa[f"preco_int_{idx}"] = price_base.shape
            mapa[f"preco_dec_{idx}"] = dec.shape
            ref_unid = dec

        def score_unid(cand: ShapeInfo) -> float:
            score = distancia(ref_unid, cand)
            # Unidade normalmente fica abaixo/ao lado dos centavos.
            if cand.y >= ref_unid.y:
                score += 30.0
            if cand.y > ref_unid.y - 0.20:
                score += 16.0
            if cand.x < ref_unid.x - 0.8:
                score += 10.0
            return score

        unid = escolher_por_score(unids_pool, score_unid)
        mapa[f"unid_{idx}"] = unid.shape

    return mapa


def nomear_campos_template(mapa: Dict[str, object]) -> None:
    for chave, shp in mapa.items():
        nome = None
        if chave.startswith("desc_"):
            nome = "DESC_" + chave.split("_", 1)[1]
        elif chave.startswith("unid_"):
            nome = "UNID_" + chave.split("_", 1)[1]
        elif chave.startswith("preco_full_"):
            nome = "PRECO_" + chave.split("_", 2)[2]
        elif chave.startswith("preco_int_"):
            nome = "PRECO_INT_" + chave.split("_", 2)[2]
        elif chave.startswith("preco_dec_"):
            nome = "PRECO_DEC_" + chave.split("_", 2)[2]
        elif chave.startswith("preco_icon_"):
            nome = "PRECO_ICON_" + chave.split("_", 2)[2]
        elif chave.startswith("barcode_"):
            nome = "BARCODE_" + chave.split("_", 1)[1]
        elif chave.startswith("validade_"):
            nome = "VALIDADE_" + chave.split("_", 1)[1]
        if not nome:
            continue
        try:
            shp.Name = nome
        except Exception:
            continue


def detectar_slots_barcode(page) -> Dict[str, object]:
    mapa: Dict[str, object] = {}

    for idx in range(1, 5):
        for nome in (
            f"BARCODE_{idx}",
            f"BARCODE_SLOT_{idx}",
            f"CODIGO_BARRAS_{idx}",
            f"CODIGO_BARRAS_SLOT_{idx}",
            f"BARRAS_{idx}",
            f"EAN13_{idx}",
        ):
            shp = get_by_name(page, nome)
            if shp is not None and _shape_tem_bbox_valido(shp):
                mapa[f"barcode_{idx}"] = shp
                break

    for idx in range(1, 5):
        if f"barcode_{idx}" in mapa:
            continue
        bbox_render = _bbox_barcode_renderizado(page, idx)
        if bbox_render is None:
            continue
        shape_validade = get_unique_by_name(page, f"VALIDADE_{idx}")
        shape_slot = _criar_slot_barcode_por_bbox(page, idx, bbox_render, shape_validade)
        if shape_slot is not None:
            mapa[f"barcode_{idx}"] = shape_slot

    if len(mapa) == 4:
        return _garantir_slots_barcode_validos(page, mapa)

    candidatos_ole: List[ShapeInfo] = []
    candidatos_outros: List[ShapeInfo] = []
    for shp in iter_top_level_shapes(page):
        if not _shape_tem_bbox_valido(shp):
            continue
        try:
            stype = int(shp.Type)
        except Exception:
            continue
        try:
            nome = str(shp.Name or "").strip().upper()
        except Exception:
            nome = ""
        if stype == CDR_OLE_SHAPE:
            if not nome:
                candidatos_ole.append(to_shape_info(shp))
            continue
        if nome.startswith(
            (
                "BARCODE_",
                "BARCODE_SLOT_",
                "CODIGO_BARRAS_",
                "CODIGO_BARRAS_SLOT_",
                "EAN13_",
            )
        ):
            candidatos_outros.append(to_shape_info(shp))

    if not mapa:
        candidatos = candidatos_ole if len(candidatos_ole) >= 4 else (candidatos_ole + candidatos_outros)
        ordenados = ordenar_grade_campos(candidatos)
        for idx, info in enumerate(ordenados[:4], start=1):
            mapa.setdefault(f"barcode_{idx}", info.shape)
    return _garantir_slots_barcode_validos(page, mapa)


def detectar_slots_validade(page) -> Dict[str, object]:
    mapa: Dict[str, object] = {}

    for idx in range(1, 5):
        for nome in (
            f"VALIDADE_{idx}",
            f"VALIDADE_OFERTA_{idx}",
            f"DATA_VALIDADE_{idx}",
            f"DATA_OFERTA_{idx}",
            f"OFERTA_VALIDADE_{idx}",
        ):
            shp = get_by_name(page, nome)
            if shp is not None:
                mapa[f"validade_{idx}"] = shp
                break

    if len(mapa) == 4:
        return mapa

    candidatos: List[ShapeInfo] = []
    for info in listar_textos(page):
        texto = str(info.text or "").strip()
        if not texto:
            continue
        token = normalizar_token_placeholder(texto)
        base, idx = extrair_base_e_indice_token(token)
        if campo_por_base_token(base) == "validade":
            if idx is not None:
                mapa[f"validade_{idx}"] = info.shape
            else:
                candidatos.append(info)
            continue

        texto_upper = texto.upper()
        if re.search(r"(XX|DD|\d{1,2})[/-](XX|MM|\d{1,2})[/-](XXXX|AAAA|\d{2,4})", texto_upper):
            candidatos.append(info)

    if len(mapa) == 4:
        return mapa

    ordenados = ordenar_grade_campos(candidatos)
    for idx, info in enumerate(ordenados[:4], start=1):
        mapa.setdefault(f"validade_{idx}", info.shape)

    for idx in range(1, 5):
        shp = mapa.get(f"validade_{idx}")
        if shp is None:
            continue
        try:
            shp.Name = f"VALIDADE_{idx}"
        except Exception:
            continue

    return mapa


def _texto_parece_icone_preco(texto: str) -> bool:
    txt = re.sub(r"\s+", "", str(texto or "").upper())
    return bool(re.fullmatch(r"R\$[:]?", txt))


def detectar_slots_icone_preco(page) -> Dict[str, object]:
    mapa: Dict[str, object] = {}

    for idx in range(1, 5):
        for nome in nomes_slot_icone_preco(idx):
            shp = get_unique_by_name(page, nome)
            if shp is not None:
                mapa[f"preco_icon_{idx}"] = shp
                break

    if len(mapa) == 4:
        return mapa

    candidatos: List[ShapeInfo] = []
    for info in listar_textos(page):
        if _texto_parece_icone_preco(info.text):
            candidatos.append(info)

    ordenados = ordenar_grade_campos(candidatos)
    for idx, info in enumerate(ordenados[:4], start=1):
        mapa.setdefault(f"preco_icon_{idx}", info.shape)

    return mapa


def selecionar_pagina_e_mapa(doc) -> Tuple[object, Dict[str, object]]:
    cache_key = chave_cache_documento(doc)
    cache_hit = _MAPA_DOCUMENTO_CACHE.get(cache_key)
    if cache_hit is not None:
        page_cache, mapa_cache = cache_hit
        if mapa_campos_acessivel(page_cache, mapa_cache):
            mapa_cache.update(detectar_slots_icone_preco(page_cache))
            mapa_cache.update(detectar_slots_barcode(page_cache))
            mapa_cache.update(detectar_slots_validade(page_cache))
            return page_cache, mapa_cache
        _MAPA_DOCUMENTO_CACHE.pop(cache_key, None)

    try:
        total_paginas = int(doc.Pages.Count)
    except Exception:
        total_paginas = 1

    try:
        ativa = int(doc.ActivePage.Index)
    except Exception:
        ativa = 1

    ordem = [ativa] + [i for i in range(1, total_paginas + 1) if i != ativa]
    ultimo_erro: Optional[Exception] = None

    for idx in ordem:
        try:
            page = doc.Pages.Item(idx)
        except Exception:
            continue

        mapa_hibrido = tentar_mapa_hibrido(page)
        if mapa_hibrido is not None:
            mapa_hibrido.update(detectar_slots_icone_preco(page))
            mapa_hibrido.update(detectar_slots_barcode(page))
            mapa_hibrido.update(detectar_slots_validade(page))
            nomear_campos_template(mapa_hibrido)
            _MAPA_DOCUMENTO_CACHE[cache_key] = (page, mapa_hibrido)
            return page, mapa_hibrido

        # Quando o template ainda expõe placeholders textuais (ex.: "Produto 1"),
        # eles são mais confiáveis que nomes persistidos de execuções anteriores.
        mapa_placeholder = tentar_mapa_por_placeholder(page)
        if mapa_placeholder is not None:
            mapa_placeholder.update(detectar_slots_icone_preco(page))
            mapa_placeholder.update(detectar_slots_barcode(page))
            mapa_placeholder.update(detectar_slots_validade(page))
            nomear_campos_template(mapa_placeholder)
            _MAPA_DOCUMENTO_CACHE[cache_key] = (page, mapa_placeholder)
            return page, mapa_placeholder

        mapa_nome = tentar_mapa_por_nome(page)
        if mapa_nome is not None and mapa_nomeado_consistente(mapa_nome):
            mapa_nome.update(detectar_slots_icone_preco(page))
            mapa_nome.update(detectar_slots_barcode(page))
            mapa_nome.update(detectar_slots_validade(page))
            nomear_campos_template(mapa_nome)
            _MAPA_DOCUMENTO_CACHE[cache_key] = (page, mapa_nome)
            return page, mapa_nome

        try:
            mapa_auto = montar_mapa_automatico(page)
            mapa_auto.update(detectar_slots_icone_preco(page))
            mapa_auto.update(detectar_slots_barcode(page))
            mapa_auto.update(detectar_slots_validade(page))
            nomear_campos_template(mapa_auto)
            _MAPA_DOCUMENTO_CACHE[cache_key] = (page, mapa_auto)
            return page, mapa_auto
        except Exception as exc:
            ultimo_erro = exc
            continue

    if ultimo_erro:
        raise ultimo_erro
    raise ValueError("Nao consegui localizar pagina com campos de produto/preco no documento.")


def gerar_quebras(texto: str, linhas: int) -> List[str]:
    palavras = texto.split()
    if len(palavras) <= 1 or linhas < 2:
        return []

    candidatos: List[Tuple[int, str]] = []

    if linhas == 2:
        for i in range(1, len(palavras)):
            p1 = " ".join(palavras[:i]).strip()
            p2 = " ".join(palavras[i:]).strip()
            if not p1 or not p2:
                continue
            score = max(len(p1), len(p2))
            candidatos.append((score, f"{p1}\r{p2}"))

    if linhas == 3 and len(palavras) >= 3:
        for i in range(1, len(palavras) - 1):
            for j in range(i + 1, len(palavras)):
                p1 = " ".join(palavras[:i]).strip()
                p2 = " ".join(palavras[i:j]).strip()
                p3 = " ".join(palavras[j:]).strip()
                if not p1 or not p2 or not p3:
                    continue
                score = max(len(p1), len(p2), len(p3))
                candidatos.append((score, f"{p1}\r{p2}\r{p3}"))

    candidatos_ordenados = sorted(candidatos, key=lambda c: c[0])
    if not candidatos_ordenados:
        return []
    # Retorna mais de uma opcao para a etapa de medicao escolher a melhor no shape.
    return [c[1] for c in candidatos_ordenados[:2]]


def aplicar_descricao(
    shape: object,
    texto: str,
    fonte: str,
    tamanho_min: float,
    tamanho_max: float,
    largura_max_cm: float,
    altura_max_cm: Optional[float] = None,
    max_linhas_quebra: int = 2,
    preferir_duas_linhas: bool = True,
    palavras_min_duas_linhas: int = 4,
    tamanho_min_emergencia: float = 14.0,
) -> None:
    texto_limpo = re.sub(r"\s+", " ", texto).strip()
    palavras = texto_limpo.split()
    tentar_duas_linhas = (
        bool(preferir_duas_linhas)
        and int(max_linhas_quebra) >= 2
        and len(palavras) >= int(palavras_min_duas_linhas)
    )

    # Para descricoes longas, prioriza quebra em 2 linhas para manter legibilidade
    # e evitar estourar horizontalmente para fora do quadro.
    testou_duas_linhas = False
    if tentar_duas_linhas:
        quebras = gerar_quebras(texto_limpo, linhas=2)
        for tamanho in range(int(tamanho_max), int(tamanho_min) - 1, -1):
            for texto_quebrado in quebras:
                set_story_text(shape, texto_quebrado)
                set_story_font_size(shape, font_name=fonte, size_pt=float(tamanho))
                cabe_largura = float(shape.SizeWidth) <= largura_max_cm
                cabe_altura = (
                    True
                    if altura_max_cm is None
                    else float(shape.SizeHeight) <= float(altura_max_cm)
                )
                if cabe_largura and cabe_altura:
                    return
        testou_duas_linhas = True

    for tamanho in range(int(tamanho_max), int(tamanho_min) - 1, -1):
        set_story_text(shape, texto_limpo)
        set_story_font_size(shape, font_name=fonte, size_pt=float(tamanho))
        cabe_largura = float(shape.SizeWidth) <= largura_max_cm
        cabe_altura = (
            True
            if altura_max_cm is None
            else float(shape.SizeHeight) <= float(altura_max_cm)
        )
        if cabe_largura and cabe_altura:
            return

    inicio_linhas = 3 if testou_duas_linhas else 2
    for linhas in range(inicio_linhas, max(2, int(max_linhas_quebra)) + 1):
        quebras = gerar_quebras(texto_limpo, linhas=linhas)
        for tamanho in range(int(tamanho_max), int(tamanho_min) - 1, -1):
            for texto_quebrado in quebras:
                set_story_text(shape, texto_quebrado)
                set_story_font_size(shape, font_name=fonte, size_pt=float(tamanho))
                cabe_largura = float(shape.SizeWidth) <= largura_max_cm
                cabe_altura = (
                    True
                    if altura_max_cm is None
                    else float(shape.SizeHeight) <= float(altura_max_cm)
                )
                if cabe_largura and cabe_altura:
                    return

    # Ultimo recurso: reduz abaixo do tamanho minimo padrao para garantir enquadramento.
    emergencia = max(8, int(min(float(tamanho_min), float(tamanho_min_emergencia))))
    fallback = gerar_quebras(texto_limpo, linhas=2) or [texto_limpo]
    for tamanho in range(int(tamanho_min) - 1, emergencia - 1, -1):
        for texto_quebrado in fallback:
            set_story_text(shape, texto_quebrado)
            set_story_font_size(shape, font_name=fonte, size_pt=float(tamanho))
            cabe_largura = float(shape.SizeWidth) <= largura_max_cm
            cabe_altura = (
                True
                if altura_max_cm is None
                else float(shape.SizeHeight) <= float(altura_max_cm)
            )
            if cabe_largura and cabe_altura:
                return

    texto_fallback = fallback[0]
    set_story_text(shape, texto_fallback)
    set_story_font_size(shape, font_name=fonte, size_pt=float(emergencia))


def obter_estilo_centavos(
    shape: object,
    fator_padrao: float,
    vshift_padrao: int,
) -> Tuple[float, int]:
    texto = get_story_text(shape)
    if "," not in texto:
        return fator_padrao, vshift_padrao
    idx = texto.find(",")
    if idx <= 0 or idx >= len(texto) - 1:
        return fator_padrao, vshift_padrao

    try:
        story = shape.Text.Story
        range_int = story.Range(0, idx)
        range_cent = story.Range(idx, len(texto))
        size_int = float(range_int.Size)
        size_cent = float(range_cent.Size)
        vshift = int(range_cent.VertShift)
        if size_int <= 0 or size_cent <= 0:
            return fator_padrao, vshift_padrao
        fator = max(0.2, min(0.8, size_cent / size_int))
        return fator, vshift
    except Exception:
        return fator_padrao, vshift_padrao


def calcular_offsets_alinhamento(shape_ref: object, shape_alvo: object) -> Tuple[float, float, float, float]:
    ref_x = float(shape_ref.PositionX)
    ref_y = float(shape_ref.PositionY)
    ref_w = float(shape_ref.SizeWidth)
    ref_h = float(shape_ref.SizeHeight)
    alvo_x = float(shape_alvo.PositionX)
    alvo_y = float(shape_alvo.PositionY)
    alvo_w = float(shape_alvo.SizeWidth)

    ref_right = ref_x + (ref_w / 2.0)
    alvo_right = alvo_x + (alvo_w / 2.0)
    delta_right = ref_right - alvo_right
    delta_y = alvo_y - ref_y
    return delta_right, delta_y, ref_w, ref_h


def escalar_offsets_alinhamento(
    delta_right: float,
    delta_y: float,
    ref_w_base: float,
    ref_h_base: float,
    shape_ref_atual: object,
) -> Tuple[float, float]:
    ref_h_atual = float(shape_ref_atual.SizeHeight)
    escala_h = (ref_h_atual / ref_h_base) if ref_h_base > 0 else 1.0
    # Mantem o deslocamento horizontal do template para evitar arrasto da unidade
    # para cima do inteiro quando o preco aumenta muito.
    return float(delta_right), float(delta_y) * escala_h


def alinhar_shape_por_offsets(
    shape_ref: object,
    shape_alvo: object,
    delta_right: float,
    delta_y: float,
) -> None:
    ref_x = float(shape_ref.PositionX)
    ref_y = float(shape_ref.PositionY)
    ref_w = float(shape_ref.SizeWidth)
    alvo_w = float(shape_alvo.SizeWidth)

    ref_right = ref_x + (ref_w / 2.0)
    alvo_right = ref_right - float(delta_right)
    alvo_x = alvo_right - (alvo_w / 2.0)
    alvo_y = ref_y + float(delta_y)

    shape_alvo.SetPosition(float(alvo_x), float(alvo_y))


def evitar_sobreposicao(shape_ref: object, shape_alvo: object, margem_cm: float = 0.06) -> None:
    rx = float(shape_ref.PositionX)
    ry = float(shape_ref.PositionY)
    rw = float(shape_ref.SizeWidth)
    rh = float(shape_ref.SizeHeight)

    ax = float(shape_alvo.PositionX)
    ay = float(shape_alvo.PositionY)
    aw = float(shape_alvo.SizeWidth)
    ah = float(shape_alvo.SizeHeight)

    r_left, r_right = rx - (rw / 2.0), rx + (rw / 2.0)
    r_bottom, r_top = ry - (rh / 2.0), ry + (rh / 2.0)
    a_left, a_right = ax - (aw / 2.0), ax + (aw / 2.0)
    a_bottom, a_top = ay - (ah / 2.0), ay + (ah / 2.0)

    inter_x = min(r_right, a_right) - max(r_left, a_left)
    inter_y = min(r_top, a_top) - max(r_bottom, a_bottom)
    if inter_x > 0 and inter_y > 0:
        novo_y = ay - (inter_y + float(margem_cm))
        shape_alvo.SetPosition(ax, novo_y)


def posicionar_unidade_abaixo_centavos(
    shape_centavos: object,
    shape_unidade: object,
    deslocamento_x_cm: float = 0.02,
    gap_min_cm: float = 0.252,
    gap_fator_altura: float = 0.0,
) -> None:
    cx = float(shape_centavos.PositionX)
    cy = float(shape_centavos.PositionY)
    ch = float(shape_centavos.SizeHeight)
    uh = float(shape_unidade.SizeHeight)

    alvo_x = cx + float(deslocamento_x_cm)

    cent_bottom = cy - (ch / 2.0)
    base_h = max(ch, uh)
    gap = max(float(gap_min_cm), base_h * float(gap_fator_altura))
    alvo_y = cent_bottom - gap - (uh / 2.0)

    shape_unidade.SetPosition(alvo_x, alvo_y)


def acomodar_unidade_com_codigo_barras(
    shape_unidade: object,
    shape_barcode: object,
    margem_lateral_cm: float = 0.08,
    margem_vertical_cm: float = 0.02,
) -> None:
    ux = float(shape_unidade.PositionX)
    uy = float(shape_unidade.PositionY)
    uw = float(shape_unidade.SizeWidth)
    uh = float(shape_unidade.SizeHeight)

    bx = float(shape_barcode.PositionX)
    by = float(shape_barcode.PositionY)
    bw = float(shape_barcode.SizeWidth)
    bh = float(shape_barcode.SizeHeight)

    unidade_bottom = uy - (uh / 2.0)
    barcode_top = by + (bh / 2.0)

    if unidade_bottom >= (barcode_top + float(margem_vertical_cm)):
        return

    barcode_left = bx - (bw / 2.0)
    unidade_right = ux + (uw / 2.0)
    limite_right = barcode_left - float(margem_lateral_cm)

    if unidade_right > limite_right:
        novo_x = limite_right - (uw / 2.0)
        shape_unidade.SetPosition(float(novo_x), uy)


def calcular_tamanho_unidade(
    tamanho_centavos: float,
    fator_tamanho_unidade: float,
    tamanho_unidade_max: Optional[float],
) -> float:
    tamanho = max(1.0, float(tamanho_centavos) * max(0.1, float(fator_tamanho_unidade)))
    if tamanho_unidade_max is not None:
        tamanho = min(tamanho, max(1.0, float(tamanho_unidade_max)))
    return float(tamanho)


def garantir_shape_abaixo(
    shape_ref: object,
    shape_alvo: object,
    margem_cm: float = 0.03,
) -> None:
    ry = float(shape_ref.PositionY)
    rh = float(shape_ref.SizeHeight)
    ax = float(shape_alvo.PositionX)
    ay = float(shape_alvo.PositionY)
    ah = float(shape_alvo.SizeHeight)

    ref_bottom = ry - (rh / 2.0)
    alvo_top = ay + (ah / 2.0)
    limite_top = ref_bottom - float(margem_cm)

    if alvo_top > limite_top:
        novo_y = limite_top - (ah / 2.0)
        shape_alvo.SetPosition(ax, novo_y)


def garantir_shape_acima(
    shape_ref: object,
    shape_alvo: object,
    margem_cm: float = 0.03,
) -> None:
    ry = float(shape_ref.PositionY)
    rh = float(shape_ref.SizeHeight)
    ax = float(shape_alvo.PositionX)
    ay = float(shape_alvo.PositionY)
    ah = float(shape_alvo.SizeHeight)

    ref_top = ry + (rh / 2.0)
    alvo_bottom = ay - (ah / 2.0)
    limite_bottom = ref_top + float(margem_cm)

    if alvo_bottom < limite_bottom:
        novo_y = limite_bottom + (ah / 2.0)
        shape_alvo.SetPosition(ax, novo_y)


def centralizar_shape_x(shape: object, alvo_x: float) -> None:
    shape.SetPosition(float(alvo_x), float(shape.PositionY))


def centralizar_par_preco_x(shape_a: object, shape_b: object, alvo_x: float) -> None:
    ax = float(shape_a.PositionX)
    ay = float(shape_a.PositionY)
    aw = float(shape_a.SizeWidth)
    bx = float(shape_b.PositionX)
    by = float(shape_b.PositionY)
    bw = float(shape_b.SizeWidth)

    left = min(ax - (aw / 2.0), bx - (bw / 2.0))
    right = max(ax + (aw / 2.0), bx + (bw / 2.0))
    centro = (left + right) / 2.0
    dx = float(alvo_x) - centro

    shape_a.SetPosition(ax + dx, ay)
    shape_b.SetPosition(bx + dx, by)


def posicionar_centavos_ao_lado_inteiro(
    shape_inteiro: object,
    shape_centavos: object,
    gap_cm: float = 0.10,
) -> None:
    ix = float(shape_inteiro.PositionX)
    iw = float(shape_inteiro.SizeWidth)
    cy = float(shape_centavos.PositionY)
    cw = float(shape_centavos.SizeWidth)

    int_right = ix + (iw / 2.0)
    alvo_x = int_right + float(gap_cm) + (cw / 2.0)
    shape_centavos.SetPosition(alvo_x, cy)


def resolver_largura_limite_preco(
    largura_configurada_cm: Optional[float],
    largura_shape_atual_cm: float,
    largura_referencia_card_cm: float,
    fator_auto: float,
) -> float:
    if largura_configurada_cm is not None:
        return max(0.05, float(largura_configurada_cm))

    fator = max(0.20, min(1.00, float(fator_auto)))
    largura_shape = max(0.05, float(largura_shape_atual_cm))
    largura_card = max(0.05, float(largura_referencia_card_cm))
    return max(largura_shape, largura_card * fator)


def aplicar_preco_full(
    shape: object,
    preco_inteiro: str,
    preco_decimal: str,
    fonte: str,
    tamanho_preco_min: float,
    tamanho_preco_max: float,
    fator_centavos: float,
    vshift_centavos: int,
    largura_max_preco_cm: Optional[float] = None,
    altura_max_preco_cm: Optional[float] = None,
) -> Tuple[float, float]:
    if not preco_inteiro and not preco_decimal:
        set_story_text(shape, "")
        return float(tamanho_preco_min), max(1.0, float(tamanho_preco_min) * float(fator_centavos))

    texto = f"{preco_inteiro},{preco_decimal}"
    largura_limite = float(largura_max_preco_cm) if largura_max_preco_cm is not None else float(shape.SizeWidth)

    set_story_text(shape, texto)
    story = shape.Text.Story
    story.Font = fonte

    idx = texto.find(",")
    if idx <= 0:
        for tamanho in range(int(tamanho_preco_max), int(tamanho_preco_min) - 1, -1):
            story.Size = float(tamanho)
            cabe_largura = float(shape.SizeWidth) <= largura_limite
            cabe_altura = (
                True
                if altura_max_preco_cm is None
                else float(shape.SizeHeight) <= float(altura_max_preco_cm)
            )
            if cabe_largura and cabe_altura:
                return float(tamanho), float(tamanho)
        story.Size = float(tamanho_preco_min)
        return float(tamanho_preco_min), float(tamanho_preco_min)

    range_int = story.Range(0, idx)
    range_cent = story.Range(idx, len(texto))
    escolhido = float(tamanho_preco_min)

    for tamanho in range(int(tamanho_preco_max), int(tamanho_preco_min) - 1, -1):
        range_int.Size = float(tamanho)
        range_int.VertShift = 0
        range_cent.Size = max(1.0, float(tamanho) * float(fator_centavos))
        range_cent.VertShift = int(vshift_centavos)
        cabe_largura = float(shape.SizeWidth) <= largura_limite
        cabe_altura = (
            True
            if altura_max_preco_cm is None
            else float(shape.SizeHeight) <= float(altura_max_preco_cm)
        )
        if cabe_largura and cabe_altura:
            escolhido = float(tamanho)
            break

    range_int.Size = escolhido
    range_int.VertShift = 0
    range_cent.Size = max(1.0, escolhido * float(fator_centavos))
    range_cent.VertShift = int(vshift_centavos)
    return escolhido, float(range_cent.Size)


def aplicar_preco_separado(
    shape_int: object,
    shape_dec: object,
    preco_inteiro: str,
    preco_decimal: str,
    fonte: str,
    tamanho_preco_min: float,
    tamanho_preco_max: float,
    fator_centavos: float,
    vshift_centavos: int,
    largura_max_preco_int_cm: Optional[float] = None,
    altura_max_preco_int_cm: Optional[float] = None,
) -> float:
    if not preco_inteiro and not preco_decimal:
        set_story_text(shape_int, "")
        set_story_text(shape_dec, "")
        return max(1.0, float(tamanho_preco_min) * float(fator_centavos))

    largura_limite = (
        float(largura_max_preco_int_cm)
        if largura_max_preco_int_cm is not None
        else float(shape_int.SizeWidth)
    )

    set_story_text(shape_int, preco_inteiro or "")
    set_story_font_size(shape_int, font_name=fonte)

    escolhido = float(tamanho_preco_min)
    for tamanho in range(int(tamanho_preco_max), int(tamanho_preco_min) - 1, -1):
        set_story_font_size(shape_int, size_pt=float(tamanho))
        cabe_largura = float(shape_int.SizeWidth) <= largura_limite
        cabe_altura = (
            True
            if altura_max_preco_int_cm is None
            else float(shape_int.SizeHeight) <= float(altura_max_preco_int_cm)
        )
        if cabe_largura and cabe_altura:
            escolhido = float(tamanho)
            break

    set_story_text(shape_dec, f",{preco_decimal}" if preco_decimal else "")
    story_dec = shape_dec.Text.Story
    story_dec.Font = fonte
    story_dec.Size = max(1.0, escolhido * float(fator_centavos))
    try:
        story_dec.VertShift = int(vshift_centavos)
    except Exception:
        pass
    return float(story_dec.Size)


def definir_shape_sem_contorno_preenchido(shape: object) -> None:
    try:
        shape.Outline.SetNoOutline()
    except Exception:
        pass
    try:
        shape.Fill.UniformColor.RGBAssign(0, 0, 0)
    except Exception:
        pass


def _safe_attr(obj: object, attr_name: str):
    try:
        return getattr(obj, attr_name)
    except Exception:
        return None


def _get_shape_bbox(shape: object) -> Optional[Dict[str, float]]:
    try:
        center_x = float(_safe_attr(shape, "PositionX"))
        center_y = float(_safe_attr(shape, "PositionY"))
        width = float(_safe_attr(shape, "SizeWidth"))
        height = float(_safe_attr(shape, "SizeHeight"))
    except Exception:
        return None

    if width <= 0 or height <= 0:
        return None

    left = center_x - (width / 2.0)
    right = center_x + (width / 2.0)
    top = center_y + (height / 2.0)
    bottom = center_y - (height / 2.0)
    return {
        "left": left,
        "right": right,
        "top": top,
        "bottom": bottom,
        "width": width,
        "height": height,
        "center_x": center_x,
        "center_y": center_y,
    }


def _shape_tem_bbox_valido(
    shape: object,
    largura_min_cm: float = 0.05,
    altura_min_cm: float = 0.05,
) -> bool:
    bbox = _get_shape_bbox(shape)
    if bbox is None:
        return False
    return bool(
        float(bbox["width"]) >= float(largura_min_cm)
        and float(bbox["height"]) >= float(altura_min_cm)
    )


def _bbox_uniao(*bboxes: Optional[Dict[str, float]]) -> Optional[Dict[str, float]]:
    validos = [bbox for bbox in bboxes if bbox is not None]
    if not validos:
        return None

    left = min(float(bbox["left"]) for bbox in validos)
    right = max(float(bbox["right"]) for bbox in validos)
    top = max(float(bbox["top"]) for bbox in validos)
    bottom = min(float(bbox["bottom"]) for bbox in validos)
    width = right - left
    height = top - bottom
    if width <= 0 or height <= 0:
        return None
    return {
        "left": left,
        "right": right,
        "top": top,
        "bottom": bottom,
        "width": width,
        "height": height,
        "center_x": left + (width / 2.0),
        "center_y": bottom + (height / 2.0),
    }


def _mediana_float(valores: List[float]) -> Optional[float]:
    dados = sorted(float(v) for v in valores)
    if not dados:
        return None
    meio = len(dados) // 2
    if len(dados) % 2 == 1:
        return float(dados[meio])
    return float((dados[meio - 1] + dados[meio]) / 2.0)


def _bbox_barcode_renderizado(page: object, indice: int) -> Optional[Dict[str, float]]:
    shape_gen = get_unique_by_name(page, f"BARCODE_GEN_{indice}")
    shape_txt = get_unique_by_name(page, f"BARCODE_TEXT_{indice}")
    return _bbox_uniao(_get_shape_bbox(shape_gen), _get_shape_bbox(shape_txt))


def _obter_layer_ancora(page: object, *shapes_referencia: object):
    for shp in shapes_referencia:
        layer = _safe_attr(shp, "Layer")
        if layer is not None:
            return layer
    for shp in iter_top_level_shapes(page):
        layer = _safe_attr(shp, "Layer")
        if layer is not None:
            return layer
    return None


def _renomear_barcodes_invalidos(page: object, indice: int) -> None:
    for pos, shp in enumerate(encontrar_shapes_por_nome(page, f"BARCODE_{indice}"), start=1):
        if _shape_tem_bbox_valido(shp):
            continue
        try:
            shp.Name = f"BARCODE_STALE_{indice}_{pos}"
        except Exception:
            continue


def _criar_slot_barcode_por_bbox(
    page: object,
    indice: int,
    bbox: Dict[str, float],
    *shapes_referencia: object,
):
    existente = get_unique_by_name(page, f"BARCODE_{indice}")
    if existente is not None and _shape_tem_bbox_valido(existente):
        return existente

    _renomear_barcodes_invalidos(page, indice)
    layer = _obter_layer_ancora(page, *shapes_referencia)
    if layer is None:
        return None

    try:
        shape = layer.CreateRectangle2(
            float(bbox["left"]),
            float(bbox["bottom"]),
            float(bbox["width"]),
            float(bbox["height"]),
        )
    except Exception:
        return None

    definir_shape_sem_contorno_preenchido(shape)
    definir_visibilidade_shape(shape, False)
    try:
        shape.Name = f"BARCODE_{indice}"
    except Exception:
        pass
    return shape


def _inferir_bbox_slot_barcode(
    page: object,
    indice: int,
    seeds: Dict[int, Dict[str, float]],
) -> Optional[Dict[str, float]]:
    if indice in seeds:
        return seeds[indice]

    mapa_validade = detectar_slots_validade(page)
    shape_validade = mapa_validade.get(f"validade_{indice}")
    bbox_validade = _get_shape_bbox(shape_validade)
    if bbox_validade is None:
        return None

    pares_mesma_coluna = {1: 3, 2: 4, 3: 1, 4: 2}
    pares_mesma_linha = {1: 2, 2: 1, 3: 4, 4: 3}
    bbox_coluna = seeds.get(pares_mesma_coluna[indice])
    bbox_linha = seeds.get(pares_mesma_linha[indice])

    dxs: List[float] = []
    dys: List[float] = []
    larguras: List[float] = []
    alturas: List[float] = []
    for idx_ref, bbox_ref in seeds.items():
        shp_ref = mapa_validade.get(f"validade_{idx_ref}")
        bbox_validade_ref = _get_shape_bbox(shp_ref)
        if bbox_validade_ref is None:
            continue
        dxs.append(float(bbox_ref["center_x"]) - float(bbox_validade_ref["center_x"]))
        dys.append(float(bbox_ref["center_y"]) - float(bbox_validade_ref["center_y"]))
        larguras.append(float(bbox_ref["width"]))
        alturas.append(float(bbox_ref["height"]))

    largura = float(bbox_coluna["width"]) if bbox_coluna is not None else _mediana_float(larguras)
    altura = float(bbox_linha["height"]) if bbox_linha is not None else _mediana_float(alturas)
    center_x = (
        float(bbox_coluna["center_x"])
        if bbox_coluna is not None
        else (
            float(bbox_validade["center_x"]) + float(_mediana_float(dxs))
            if _mediana_float(dxs) is not None
            else None
        )
    )
    center_y = (
        float(bbox_linha["center_y"])
        if bbox_linha is not None
        else (
            float(bbox_validade["center_y"]) + float(_mediana_float(dys))
            if _mediana_float(dys) is not None
            else None
        )
    )

    if None in {center_x, center_y, largura, altura}:
        return None

    left = float(center_x) - (float(largura) / 2.0)
    bottom = float(center_y) - (float(altura) / 2.0)
    return {
        "left": left,
        "right": left + float(largura),
        "top": bottom + float(altura),
        "bottom": bottom,
        "width": float(largura),
        "height": float(altura),
        "center_x": left + (float(largura) / 2.0),
        "center_y": bottom + (float(altura) / 2.0),
    }


def _garantir_slots_barcode_validos(page: object, mapa: Dict[str, object]) -> Dict[str, object]:
    seeds: Dict[int, Dict[str, float]] = {}
    for idx in range(1, 5):
        shape_slot = mapa.get(f"barcode_{idx}")
        bbox_slot = _get_shape_bbox(shape_slot)
        if bbox_slot is not None:
            seeds[idx] = bbox_slot
            continue
        bbox_render = _bbox_barcode_renderizado(page, idx)
        if bbox_render is not None:
            seeds[idx] = bbox_render

    for idx in range(1, 5):
        shape_slot = mapa.get(f"barcode_{idx}")
        if shape_slot is not None and _shape_tem_bbox_valido(shape_slot):
            continue

        bbox_inferido = _inferir_bbox_slot_barcode(page, idx, seeds)
        if bbox_inferido is None:
            continue

        mapa_validade = detectar_slots_validade(page)
        shape_validade = mapa_validade.get(f"validade_{idx}")
        novo_slot = _criar_slot_barcode_por_bbox(page, idx, bbox_inferido, shape_validade, shape_slot)
        if novo_slot is None:
            continue
        mapa[f"barcode_{idx}"] = novo_slot
        seeds[idx] = bbox_inferido

    return mapa


def _build_ean13_bits(value: str) -> str:
    codigo = preparar_codigo_barras_ean13(value)
    if not codigo or len(codigo) != 13 or not codigo.isdigit():
        raise ValueError("Codigo EAN-13 invalido para gerar imagem.")

    l_codes = ("0001101", "0011001", "0010011", "0111101", "0100011", "0110001", "0101111", "0111011", "0110111", "0001011")
    g_codes = ("0100111", "0110011", "0011011", "0100001", "0011101", "0111001", "0000101", "0010001", "0001001", "0010111")
    r_codes = ("1110010", "1100110", "1101100", "1000010", "1011100", "1001110", "1010000", "1000100", "1001000", "1110100")
    parity = ("LLLLLL", "LLGLGG", "LLGGLG", "LLGGGL", "LGLLGG", "LGGLLG", "LGGGLL", "LGLGLG", "LGLGGL", "LGGLGL")

    bits = ["101"]
    paridade = parity[int(codigo[0])]
    for idx_local in range(1, 7):
        digito = int(codigo[idx_local])
        bits.append(l_codes[digito] if paridade[idx_local - 1] == "L" else g_codes[digito])
    bits.append("01010")
    for idx_local in range(7, 13):
        bits.append(r_codes[int(codigo[idx_local])])
    bits.append("101")
    return "".join(bits)


def _write_ean13_png(value: str, target_file: Path) -> None:
    try:
        from PIL import Image, ImageDraw  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Pillow nao encontrado para gerar imagem de EAN-13. "
            "Instale com: python -m pip install pillow"
        ) from exc

    bits = _build_ean13_bits(value)
    quiet_modules = 11
    module_px = 3
    height_px = 170

    width_modules = (quiet_modules * 2) + len(bits)
    width_px = max(10, width_modules * module_px)

    image = Image.new("RGB", (width_px, height_px), "white")
    draw = ImageDraw.Draw(image)

    x = quiet_modules * module_px
    for bit in bits:
        if bit == "1":
            draw.rectangle((x, 0, x + module_px - 1, height_px - 1), fill="black")
        x += module_px

    target_file.parent.mkdir(parents=True, exist_ok=True)
    image.save(target_file, format="PNG")


def _extract_imported_shape(import_result: object):
    if import_result is None:
        return None
    if _get_shape_bbox(import_result) is not None:
        return import_result

    range_obj = _safe_attr(import_result, "ShapeRange")
    if range_obj is not None:
        import_result = range_obj

    count_raw = _safe_attr(import_result, "Count")
    if count_raw is not None:
        try:
            count = int(count_raw)
        except Exception:
            count = 0
        if count > 0:
            try:
                last_item = import_result.Item(count)
                if _get_shape_bbox(last_item) is not None:
                    return last_item
            except Exception:
                pass
    return None


def _shape_fingerprint(shape: object) -> Optional[str]:
    bbox = _get_shape_bbox(shape)
    if bbox is None:
        return None
    shape_type = _safe_attr(shape, "Type")
    return (
        f"{shape_type}|"
        f"{bbox['center_x']:.4f}|{bbox['center_y']:.4f}|"
        f"{bbox['width']:.4f}|{bbox['height']:.4f}"
    )


def _snapshot_shape_counter(shapes_collection) -> Counter[str]:
    snapshot: Counter[str] = Counter()
    for shape in iter_shapes(shapes_collection):
        fp = _shape_fingerprint(shape)
        if fp:
            snapshot[fp] += 1
    return snapshot


def _detect_new_shape_after_import(shapes_collection, before_counter: Counter[str]):
    remaining = Counter(before_counter)
    extras: List[object] = []
    for shape in iter_shapes(shapes_collection):
        fp = _shape_fingerprint(shape)
        if not fp:
            continue
        if remaining.get(fp, 0) > 0:
            remaining[fp] -= 1
        else:
            extras.append(shape)
    if not extras:
        return None
    extras.sort(key=lambda shp: (_get_shape_bbox(shp) or {}).get("width", 0.0) * (_get_shape_bbox(shp) or {}).get("height", 0.0), reverse=True)
    return extras[0]


def _import_image_shape(reference_shape: object, image_path: Path):
    layer = _safe_attr(reference_shape, "Layer")
    if layer is None:
        raise RuntimeError("Shape de referencia sem layer para importar o barcode.")

    import_attempts = [
        lambda: layer.Import(str(image_path), 0, None),
        lambda: layer.Import(str(image_path), 0),
        lambda: layer.Import(str(image_path)),
        lambda: layer.ImportEx(str(image_path)),
    ]
    import_errors: List[str] = []

    for import_call in import_attempts:
        before_counter = _snapshot_shape_counter(layer.Shapes)
        try:
            imported = import_call()
        except Exception as exc:
            import_errors.append(str(exc))
            continue

        shape = _extract_imported_shape(imported)
        if shape is None:
            shape = _detect_new_shape_after_import(layer.Shapes, before_counter)
        if shape is not None:
            return shape

    errors_tail = "; ".join(import_errors[-3:]) if import_errors else "sem detalhe"
    raise RuntimeError(
        f"Nao foi possivel importar o barcode: {image_path} | detalhes: {errors_tail}"
    )


def _try_set_shape_size(shape: object, width: float, height: float) -> None:
    for set_call in (
        lambda: shape.SetSize(width, height),
        lambda: setattr(shape, "SizeWidth", width),
        lambda: setattr(shape, "Width", width),
    ):
        try:
            set_call()
            break
        except Exception:
            continue

    for set_call in (
        lambda: setattr(shape, "SizeHeight", height),
        lambda: setattr(shape, "Height", height),
    ):
        try:
            set_call()
            return
        except Exception:
            continue


def _try_set_shape_center(shape: object, center_x: float, center_y: float) -> None:
    for set_call in (
        lambda: setattr(shape, "CenterX", center_x),
        lambda: setattr(shape, "PositionX", center_x),
        lambda: shape.SetPosition(center_x, center_y),
    ):
        try:
            set_call()
            break
        except Exception:
            continue

    for set_call in (
        lambda: setattr(shape, "CenterY", center_y),
        lambda: setattr(shape, "PositionY", center_y),
    ):
        try:
            set_call()
            return
        except Exception:
            continue


def _try_delete_shape(shape: object) -> None:
    for delete_call in (
        lambda: shape.Delete(),
        lambda: shape.Remove(),
    ):
        try:
            delete_call()
            return
        except Exception:
            continue


def _bbox_overlap(a: Dict[str, float], b: Dict[str, float], margem_cm: float = 0.01) -> bool:
    return not (
        (a["right"] + margem_cm) < b["left"]
        or (b["right"] + margem_cm) < a["left"]
        or (a["top"] + margem_cm) < b["bottom"]
        or (b["top"] + margem_cm) < a["bottom"]
    )


def _texto_parece_codigo_barras(shape: object) -> bool:
    texto = re.sub(r"\s+", "", get_story_text(shape))
    return bool(re.fullmatch(r"\d{12,14}", texto))


def limpar_barcode_no_slot(page: object, shape_slot: object, indice: int) -> None:
    shape_slot = resolver_shape_slot_barcode(page, shape_slot, indice)
    if shape_slot is None:
        return
    slot_bbox = _get_shape_bbox(shape_slot)
    delete_shapes_by_name(page, f"BARCODE_GEN_{indice}")
    delete_shapes_by_name(page, f"BARCODE_TEXT_{indice}")
    if slot_bbox is None:
        return

    for shp in iter_top_level_shapes(page):
        if shp is shape_slot:
            continue
        bbox = _get_shape_bbox(shp)
        if bbox is None or not _bbox_overlap(slot_bbox, bbox, margem_cm=0.02):
            continue
        try:
            nome = str(shp.Name or "").strip().upper()
        except Exception:
            nome = ""
        if nome in {n.upper() for n in nomes_slot_barcode(indice)}:
            continue
        if nome.startswith(("BARCODE_", "BARCODE_GEN_", "CODIGO_BARRAS_", "CODIGO_BARRAS_GEN_", "EAN13_")):
            _try_delete_shape(shp)
            continue
        try:
            stype = int(shp.Type)
        except Exception:
            stype = 0
        if stype != CDR_TEXT_SHAPE:
            _try_delete_shape(shp)
            continue
        if _texto_parece_codigo_barras(shp):
            _try_delete_shape(shp)


def desenhar_bits_barras(
    layer: object,
    inicio_x: float,
    base_y: float,
    largura_modulo_cm: float,
    altura_cm: float,
    bits: str,
) -> List[object]:
    shapes: List[object] = []
    for idx, bit in enumerate(bits):
        if bit != "1":
            continue
        shp = layer.CreateRectangle2(
            float(inicio_x + (idx * largura_modulo_cm)),
            float(base_y),
            float(largura_modulo_cm),
            float(altura_cm),
        )
        definir_shape_sem_contorno_preenchido(shp)
        shapes.append(shp)
    return shapes


def limpar_codigo_barras_renderizado(
    page: object,
    shape_slot: object,
    indice: int,
) -> None:
    shape_slot = resolver_shape_slot_barcode(page, shape_slot, indice)
    if shape_slot is None:
        return
    limpar_barcode_no_slot(page, shape_slot, indice)
    definir_visibilidade_shape(shape_slot, False)


def gerar_grupo_codigo_barras(
    page: object,
    shape_slot: object,
    codigo_barras: str,
    indice: int,
) -> Optional[object]:
    shape_slot = resolver_shape_slot_barcode(page, shape_slot, indice)
    if shape_slot is None:
        return None
    codigo_final = preparar_codigo_barras_ean13(codigo_barras)
    limpar_codigo_barras_renderizado(page, shape_slot, indice)

    if not codigo_final:
        return None

    bbox = _get_shape_bbox(shape_slot)
    if bbox is None:
        return None
    layer = shape_slot.Layer

    try:
        tipo_shape = int(shape_slot.Type)
    except Exception:
        tipo_shape = 0
    try:
        nome_shape = str(shape_slot.Name or "").upper()
    except Exception:
        nome_shape = ""

    if nome_shape.startswith(("BARCODE_GEN_", "CODIGO_BARRAS_GEN_")):
        try:
            _try_delete_shape(shape_slot)
        except Exception:
            pass

    largura = float(bbox["width"])
    altura = float(bbox["height"])
    cx = float(bbox["center_x"])
    bottom = float(bbox["bottom"])

    margem_inferior_cm = max(0.03, min(0.08, altura * 0.07))
    margem_superior_cm = max(0.02, min(0.05, altura * 0.05))
    altura_texto_cm = max(0.08, min(0.16, altura * 0.14))
    gap_texto_cm = max(0.01, min(0.03, altura * 0.03))
    altura_barras_cm = max(
        0.16,
        altura - altura_texto_cm - gap_texto_cm - margem_inferior_cm - margem_superior_cm,
    )
    centro_barras_y = (
        bottom
        + margem_inferior_cm
        + altura_texto_cm
        + gap_texto_cm
        + (altura_barras_cm / 2.0)
    )

    with tempfile.TemporaryDirectory(prefix="ofertas_code128_") as temp_dir:
        barcode_image = Path(temp_dir) / f"ean13_{indice}.png"
        _write_ean13_png(codigo_final, barcode_image)
        imported_shape = _import_image_shape(shape_slot, barcode_image)

    _try_set_shape_size(imported_shape, largura, altura_barras_cm)
    _try_set_shape_center(imported_shape, cx, centro_barras_y)
    try:
        imported_shape.Name = f"BARCODE_GEN_{indice}"
    except Exception:
        pass
    definir_visibilidade_shape(imported_shape, True)
    if imported_shape is not shape_slot:
        definir_visibilidade_shape(shape_slot, False)

    texto_barcode = None
    try:
        texto_barcode = layer.CreateArtisticText(float(bbox["left"]), float(bottom), str(codigo_final))
    except Exception:
        texto_barcode = None
    if texto_barcode is not None:
        tamanho_texto_base = max(5.0, min(10.0, altura_texto_cm * 22.0))
        tamanho_escolhido = tamanho_texto_base
        for tamanho in range(int(math.ceil(tamanho_texto_base)), 3, -1):
            set_story_font_size(
                texto_barcode,
                font_name="Arial",
                size_pt=float(tamanho),
                reset_vert_shift=True,
            )
            if (
                float(texto_barcode.SizeWidth) <= max(0.10, largura * 0.98)
                and float(texto_barcode.SizeHeight) <= max(0.04, altura_texto_cm)
            ):
                tamanho_escolhido = float(tamanho)
                break
            tamanho_escolhido = float(tamanho)
        set_story_font_size(
            texto_barcode,
            font_name="Arial",
            size_pt=float(tamanho_escolhido),
            reset_vert_shift=True,
        )
        try:
            texto_barcode.SetPosition(float(cx), float(bottom + margem_inferior_cm + (altura_texto_cm / 2.0)))
        except Exception:
            pass
        try:
            texto_barcode.Name = f"BARCODE_TEXT_{indice}"
        except Exception:
            pass

    return imported_shape


def aplicar_produtos_na_pagina(
    page,
    produtos: List[Produto],
    fonte: str,
    tamanho_min: float,
    tamanho_max: float,
    largura_max_cm: float,
    altura_max_descricao_cm: Optional[float],
    max_linhas_descricao: int,
    preferir_desc_duas_linhas: bool,
    desc_palavras_min_duas_linhas: int,
    desc_tamanho_min_emergencia: float,
    tamanho_preco_min: float,
    tamanho_preco_max: float,
    fator_centavos: float,
    vshift_centavos: int,
    largura_max_preco_cm: Optional[float],
    altura_max_preco_cm: Optional[float],
    largura_max_preco_int_cm: Optional[float],
    altura_max_preco_int_cm: Optional[float],
    reposicionar_campos: bool = False,
    alinhar_centavos_preco: bool = True,
    gap_preco_centavos_cm: float = 0.10,
    centralizar_preco: bool = True,
    fator_tamanho_unidade: float = 0.45,
    tamanho_unidade_max: Optional[float] = None,
    alinhar_unidade_centavos: bool = True,
    ajustar_colisao_unidade: bool = False,
    deslocamento_x_unidade_cm: float = 0.02,
    gap_unidade_min_cm: float = 0.252,
    gap_unidade_fator_altura: float = 0.0,
    fator_largura_preco_auto: float = 0.80,
    fator_largura_preco_int_auto: float = 0.62,
    mapa: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    mapa_campos = mapa
    if mapa_campos is None:
        mapa_campos = tentar_mapa_por_nome(page)
        if mapa_campos is None:
            mapa_campos = montar_mapa_automatico(page)
    if not mapa_tem_slots_icone_preco(mapa_campos):
        mapa_campos.update(detectar_slots_icone_preco(page))
    mapa_campos.update(detectar_slots_barcode(page))
    mapa_campos.update(detectar_slots_validade(page))

    for i, produto in enumerate(produtos, start=1):
        shape_desc = mapa_campos[f"desc_{i}"]
        shape_unid = mapa_campos[f"unid_{i}"]
        shape_validade = mapa_campos.get(f"validade_{i}")
        shape_preco_icon = mapa_campos.get(f"preco_icon_{i}")
        slot_tem_produto = bool(str(produto.descricao or "").strip())
        slot_tem_preco = bool(slot_tem_produto and (produto.preco_inteiro or produto.preco_decimal))
        y_unidade_ancora: Optional[float] = None
        desc_x = float(shape_desc.PositionX)

        if slot_tem_produto:
            aplicar_descricao(
                shape=shape_desc,
                texto=produto.descricao,
                fonte=fonte,
                tamanho_min=tamanho_min,
                tamanho_max=tamanho_max,
                largura_max_cm=largura_max_cm,
                altura_max_cm=altura_max_descricao_cm,
                max_linhas_quebra=max_linhas_descricao,
                preferir_duas_linhas=preferir_desc_duas_linhas,
                palavras_min_duas_linhas=desc_palavras_min_duas_linhas,
                tamanho_min_emergencia=desc_tamanho_min_emergencia,
            )
            definir_visibilidade_shape(shape_desc, True)
        else:
            set_story_text(shape_desc, "")
            set_story_font_size(shape_desc, font_name=fonte, size_pt=float(tamanho_min))
            definir_visibilidade_shape(shape_desc, False)

        largura_referencia_card = max(float(largura_max_cm), float(shape_desc.SizeWidth))
        modo_preco_full = f"preco_full_{i}" in mapa_campos

        if modo_preco_full:
            shape_preco_full = mapa_campos[f"preco_full_{i}"]
            if reposicionar_campos:
                delta_right, delta_y, ref_w_base, ref_h_base = calcular_offsets_alinhamento(
                    shape_preco_full, shape_unid
                )

            fator_local, vshift_local = obter_estilo_centavos(
                shape=shape_preco_full,
                fator_padrao=fator_centavos,
                vshift_padrao=vshift_centavos,
            )
            largura_limite_preco_full = resolver_largura_limite_preco(
                largura_configurada_cm=largura_max_preco_cm,
                largura_shape_atual_cm=float(shape_preco_full.SizeWidth),
                largura_referencia_card_cm=largura_referencia_card,
                fator_auto=fator_largura_preco_auto,
            )
            _, tamanho_centavos = aplicar_preco_full(
                shape=shape_preco_full,
                preco_inteiro=produto.preco_inteiro,
                preco_decimal=produto.preco_decimal,
                fonte=fonte,
                tamanho_preco_min=tamanho_preco_min,
                tamanho_preco_max=tamanho_preco_max,
                fator_centavos=fator_local,
                vshift_centavos=vshift_local,
                largura_max_preco_cm=largura_limite_preco_full,
                altura_max_preco_cm=altura_max_preco_cm,
            )
            definir_visibilidade_shape(shape_preco_full, slot_tem_preco)
            if slot_tem_preco and (reposicionar_campos or centralizar_preco):
                centralizar_shape_x(shape_preco_full, desc_x)
            if reposicionar_campos:
                shape_ref_unid = shape_preco_full
                shapes_colisao = [shape_preco_full]
        else:
            shape_preco_int = mapa_campos[f"preco_int_{i}"]
            shape_preco_dec = mapa_campos[f"preco_dec_{i}"]
            largura_limite_preco_int = resolver_largura_limite_preco(
                largura_configurada_cm=largura_max_preco_int_cm,
                largura_shape_atual_cm=float(shape_preco_int.SizeWidth),
                largura_referencia_card_cm=largura_referencia_card,
                fator_auto=fator_largura_preco_int_auto,
            )

            tamanho_centavos = aplicar_preco_separado(
                shape_int=shape_preco_int,
                shape_dec=shape_preco_dec,
                preco_inteiro=produto.preco_inteiro,
                preco_decimal=produto.preco_decimal,
                fonte=fonte,
                tamanho_preco_min=tamanho_preco_min,
                tamanho_preco_max=tamanho_preco_max,
                fator_centavos=fator_centavos,
                vshift_centavos=vshift_centavos,
                largura_max_preco_int_cm=largura_limite_preco_int,
                altura_max_preco_int_cm=altura_max_preco_int_cm,
            )
            definir_visibilidade_shape(shape_preco_int, slot_tem_preco)
            definir_visibilidade_shape(shape_preco_dec, slot_tem_preco)

            if slot_tem_preco and produto.preco_decimal and alinhar_centavos_preco:
                posicionar_centavos_ao_lado_inteiro(
                    shape_inteiro=shape_preco_int,
                    shape_centavos=shape_preco_dec,
                    gap_cm=float(gap_preco_centavos_cm),
                )

            if slot_tem_preco and (reposicionar_campos or centralizar_preco):
                centralizar_par_preco_x(shape_preco_int, shape_preco_dec, desc_x)

        definir_visibilidade_shape(shape_preco_icon, slot_tem_preco)
        set_story_text(shape_unid, (produto.unidade or "") if slot_tem_produto else "")
        set_story_font_size(shape_unid, font_name=fonte)
        definir_visibilidade_shape(shape_unid, bool(slot_tem_produto and produto.unidade))
        if slot_tem_produto and produto.unidade:
            tamanho_unidade = calcular_tamanho_unidade(
                tamanho_centavos=float(tamanho_centavos),
                fator_tamanho_unidade=fator_tamanho_unidade,
                tamanho_unidade_max=tamanho_unidade_max,
            )
            set_story_font_size(
                shape_unid,
                size_pt=float(tamanho_unidade),
                reset_vert_shift=True,
            )

        if slot_tem_produto and produto.unidade and reposicionar_campos:
            if modo_preco_full:
                delta_right_scaled, delta_y_scaled = escalar_offsets_alinhamento(
                    delta_right=delta_right,
                    delta_y=delta_y,
                    ref_w_base=ref_w_base,
                    ref_h_base=ref_h_base,
                    shape_ref_atual=shape_ref_unid,
                )
                alinhar_shape_por_offsets(
                    shape_ref=shape_ref_unid,
                    shape_alvo=shape_unid,
                    delta_right=delta_right_scaled,
                    delta_y=delta_y_scaled,
                )
                for shp_col in shapes_colisao:
                    evitar_sobreposicao(shape_ref=shp_col, shape_alvo=shape_unid)
        if slot_tem_produto and produto.unidade and slot_tem_preco and (not modo_preco_full) and (
            alinhar_unidade_centavos or reposicionar_campos
        ):
            posicionar_unidade_abaixo_centavos(
                shape_centavos=shape_preco_dec,
                shape_unidade=shape_unid,
                deslocamento_x_cm=float(deslocamento_x_unidade_cm),
                gap_min_cm=float(gap_unidade_min_cm),
                gap_fator_altura=float(gap_unidade_fator_altura),
            )
            y_unidade_ancora = float(shape_unid.PositionY)
            if ajustar_colisao_unidade:
                garantir_shape_abaixo(shape_ref=shape_preco_dec, shape_alvo=shape_unid, margem_cm=0.03)
                garantir_shape_abaixo(shape_ref=shape_preco_int, shape_alvo=shape_unid, margem_cm=0.02)
                evitar_sobreposicao(shape_ref=shape_preco_dec, shape_alvo=shape_unid)
                evitar_sobreposicao(shape_ref=shape_preco_int, shape_alvo=shape_unid)
                garantir_shape_abaixo(shape_ref=shape_preco_dec, shape_alvo=shape_unid, margem_cm=0.03)
                garantir_shape_abaixo(shape_ref=shape_preco_int, shape_alvo=shape_unid, margem_cm=0.02)

        shape_barcode = mapa_campos.get(f"barcode_{i}")
        if shape_barcode is not None:
            barcode_render = shape_barcode
            if produto.usar_codigo_barras and produto.codigo_barras:
                novo_barcode = gerar_grupo_codigo_barras(
                    page=page,
                    shape_slot=shape_barcode,
                    codigo_barras=produto.codigo_barras,
                    indice=i,
                )
                if novo_barcode is not None:
                    barcode_render = novo_barcode
                    mapa_campos[f"barcode_{i}"] = shape_barcode
            else:
                limpar_codigo_barras_renderizado(page, shape_barcode, i)
                barcode_render = None

            if slot_tem_produto and produto.unidade and barcode_render is not None:
                acomodar_unidade_com_codigo_barras(
                    shape_unidade=shape_unid,
                    shape_barcode=barcode_render,
                    margem_lateral_cm=0.03,
                    margem_vertical_cm=0.02,
                )
                if y_unidade_ancora is not None:
                    shape_unid.SetPosition(float(shape_unid.PositionX), float(y_unidade_ancora))

        if slot_tem_produto and produto.unidade and shape_validade is not None and y_unidade_ancora is not None:
            shape_unid.SetPosition(float(shape_unid.PositionX), float(y_unidade_ancora))

        if shape_validade is not None:
            if slot_tem_produto and produto.validade_oferta:
                set_story_text(
                    shape_validade,
                    montar_texto_validade_oferta(
                        texto_atual=get_story_text(shape_validade),
                        data_oferta=produto.validade_oferta,
                    ),
                )
                definir_visibilidade_shape(shape_validade, True)
            else:
                set_story_text(shape_validade, "")
                definir_visibilidade_shape(shape_validade, False)

    return mapa_campos


def imprimir_documento(doc, copias: int = 1, impressora: Optional[str] = None) -> None:
    try:
        doc.Activate()
    except Exception:
        pass
    try:
        doc.ActivePage.Activate()
    except Exception:
        pass

    ps = doc.PrintSettings
    if impressora:
        ps.SelectPrinter(str(impressora))
    try:
        impressora_ativa = str(ps.Printer).strip()
    except Exception:
        impressora_ativa = str(impressora or "").strip()
    ps.PrintRange = 0  # prnWholeDocument
    ps.Copies = max(1, int(copias))
    ps.Collate = False
    ps.PrintToFile = False
    print(
        "Corel: enviando para impressao"
        + (f" na impressora '{impressora_ativa}'" if impressora_ativa else " na impressora padrao")
        + f" | copias={ps.Copies}"
    )
    try:
        ps.PrintOut()
    except Exception:
        try:
            doc.PrintOut()
        except Exception as exc:
            raise RuntimeError(f"Falha ao enviar impressao ao CorelDRAW ({exc}).")


def atualizar_documento(
    config: dict,
    cdr_path: Path,
    salvar_em: Optional[Path],
    imprimir: bool = False,
    copias: int = 1,
    impressora: Optional[str] = None,
    salvar_documento: bool = True,
    fechar_documento: bool = False,
) -> bool:
    cdr_path = cdr_path.resolve()
    if salvar_em is not None:
        salvar_em = salvar_em.resolve()

    produtos = parse_produtos(config)

    fonte = str(config.get("fonte_descricao_unidade", "TangoSans"))
    tamanho_min = float(config.get("tamanho_descricao_min", 24))
    tamanho_max = float(config.get("tamanho_descricao_max", 38))
    largura_max_cm = float(config.get("largura_max_descricao_cm", 5.0))
    altura_max_descricao_cm_cfg = config.get("altura_max_descricao_cm")
    altura_max_descricao_cm = (
        float(altura_max_descricao_cm_cfg)
        if altura_max_descricao_cm_cfg is not None
        else 1.9
    )
    max_linhas_descricao = int(config.get("max_linhas_descricao", 2))
    preferir_desc_duas_linhas = bool(config.get("preferir_descricao_duas_linhas", True))
    desc_palavras_min_duas_linhas = int(config.get("descricao_palavras_min_duas_linhas", 3))
    desc_tamanho_min_emergencia = float(config.get("tamanho_descricao_min_emergencia", 16))
    tamanho_preco_min = float(config.get("tamanho_preco_min", 90))
    tamanho_preco_max = float(config.get("tamanho_preco_max", 170))
    fator_centavos = float(config.get("fator_tamanho_centavos", 0.37))
    vshift_centavos = int(config.get("deslocamento_vertical_centavos", 118))
    alinhar_centavos_preco = bool(config.get("alinhar_centavos_preco", True))
    gap_preco_centavos_cm = float(config.get("gap_preco_centavos_cm", 0.10))
    centralizar_preco = bool(config.get("centralizar_preco", True))
    fator_tamanho_unidade = float(config.get("fator_tamanho_unidade", 0.42))
    tamanho_unidade_max_cfg = config.get("tamanho_unidade_max")
    tamanho_unidade_max = (
        float(tamanho_unidade_max_cfg) if tamanho_unidade_max_cfg is not None else 28.0
    )
    alinhar_unidade_centavos = bool(config.get("alinhar_unidade_centavos", True))
    ajustar_colisao_unidade = bool(config.get("ajustar_colisao_unidade", False))
    deslocamento_x_unidade_cm = float(config.get("deslocamento_x_unidade_cm", 0.02))
    gap_unidade_min_cm = float(config.get("gap_unidade_min_cm", 0.252))
    gap_unidade_fator_altura = float(config.get("gap_unidade_fator_altura", 0.0))
    fator_largura_preco_auto = float(config.get("fator_largura_preco_auto", 0.78))
    fator_largura_preco_int_auto = float(config.get("fator_largura_preco_int_auto", 0.60))
    largura_max_preco_cm_cfg = config.get("largura_max_preco_cm")
    altura_max_preco_cm_cfg = config.get("altura_max_preco_cm")
    largura_max_preco_int_cm_cfg = config.get("largura_max_preco_int_cm")
    altura_max_preco_int_cm_cfg = config.get("altura_max_preco_int_cm")
    largura_max_preco_cm = (
        float(largura_max_preco_cm_cfg) if largura_max_preco_cm_cfg is not None else None
    )
    altura_max_preco_cm = (
        float(altura_max_preco_cm_cfg) if altura_max_preco_cm_cfg is not None else 3.7
    )
    largura_max_preco_int_cm = (
        float(largura_max_preco_int_cm_cfg) if largura_max_preco_int_cm_cfg is not None else None
    )
    altura_max_preco_int_cm = (
        float(altura_max_preco_int_cm_cfg) if altura_max_preco_int_cm_cfg is not None else 3.7
    )
    reposicionar_campos = bool(config.get("reposicionar_campos", False))

    app = obter_app_corel()

    def localizar_documento_aberto(caminho: Path):
        for d in app.Documents:
            try:
                if Path(str(d.FullFileName)).resolve() == caminho.resolve():
                    return d
            except Exception:
                continue
        return None

    doc_aberto_original = localizar_documento_aberto(cdr_path)
    alvo_path = cdr_path
    reutilizando_documento_aberto = False
    if salvar_em is not None:
        salvar_em.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cdr_path, salvar_em)
        alvo_path = salvar_em
    elif not salvar_documento and doc_aberto_original is not None:
        reutilizando_documento_aberto = True
        print(f"Corel: reutilizando documento aberto {cdr_path.name}.")
    elif not salvar_documento:
        print(f"Corel: abrindo documento original {alvo_path.name} para impressao sem salvar.")
    elif salvar_documento:
        print(f"Corel: abrindo documento de trabalho {alvo_path.name}.")

    doc = None
    abriu_documento = False
    if reutilizando_documento_aberto:
        doc = doc_aberto_original
    else:
        doc = localizar_documento_aberto(alvo_path)

    if doc is None:
        print(f"Corel: carregando {alvo_path.name}...")
        doc = app.OpenDocument(str(alvo_path))
        abriu_documento = True

    unidade_original = doc.Unit
    ref_original = doc.ReferencePoint
    try:
        print("Corel: preparando campos da placa...")
        doc.Unit = CDR_UNIT_CENTIMETER
        doc.ReferencePoint = CDR_REF_CENTER

        page, mapa_campos = selecionar_pagina_e_mapa(doc)
        aplicar_produtos_na_pagina(
            page=page,
            produtos=produtos,
            fonte=fonte,
            tamanho_min=tamanho_min,
            tamanho_max=tamanho_max,
            largura_max_cm=largura_max_cm,
            altura_max_descricao_cm=altura_max_descricao_cm,
            max_linhas_descricao=max_linhas_descricao,
            preferir_desc_duas_linhas=preferir_desc_duas_linhas,
            desc_palavras_min_duas_linhas=desc_palavras_min_duas_linhas,
            desc_tamanho_min_emergencia=desc_tamanho_min_emergencia,
            tamanho_preco_min=tamanho_preco_min,
            tamanho_preco_max=tamanho_preco_max,
            fator_centavos=fator_centavos,
            vshift_centavos=vshift_centavos,
            largura_max_preco_cm=largura_max_preco_cm,
            altura_max_preco_cm=altura_max_preco_cm,
            largura_max_preco_int_cm=largura_max_preco_int_cm,
            altura_max_preco_int_cm=altura_max_preco_int_cm,
            reposicionar_campos=reposicionar_campos,
            alinhar_centavos_preco=alinhar_centavos_preco,
            gap_preco_centavos_cm=gap_preco_centavos_cm,
            centralizar_preco=centralizar_preco,
            fator_tamanho_unidade=fator_tamanho_unidade,
            tamanho_unidade_max=tamanho_unidade_max,
            alinhar_unidade_centavos=alinhar_unidade_centavos,
            ajustar_colisao_unidade=ajustar_colisao_unidade,
            deslocamento_x_unidade_cm=deslocamento_x_unidade_cm,
            gap_unidade_min_cm=gap_unidade_min_cm,
            gap_unidade_fator_altura=gap_unidade_fator_altura,
            fator_largura_preco_auto=fator_largura_preco_auto,
            fator_largura_preco_int_auto=fator_largura_preco_int_auto,
            mapa=mapa_campos,
        )

        if salvar_documento:
            doc.Save()
        if imprimir:
            imprimir_documento(doc=doc, copias=copias, impressora=impressora)

        if salvar_documento and salvar_em:
            print(f"Arquivo salvo em: {salvar_em}")
        elif salvar_documento:
            print(f"Arquivo atualizado: {cdr_path}")
        else:
            print("Documento atualizado apenas para impressao (sem salvar).")
    finally:
        doc.Unit = unidade_original
        doc.ReferencePoint = ref_original
        deve_fechar_documento = bool(
            (fechar_documento and abriu_documento)
            and (not reutilizando_documento_aberto)
        )
        if deve_fechar_documento:
            try:
                if salvar_documento:
                    doc.Close()
                else:
                    # Fecha descartando alteracoes para nao persistir no CDR.
                    doc.Close(False)
            except Exception:
                try:
                    doc.Close()
                except Exception:
                    pass
    return bool(abriu_documento)


def fechar_documento_por_caminho(cdr_path: Path, salvar_documento: bool = False) -> bool:
    app = obter_app_corel()
    alvo = cdr_path.resolve()
    _MAPA_DOCUMENTO_CACHE.pop(str(alvo).lower(), None)

    doc = None
    for d in app.Documents:
        try:
            if Path(str(d.FullFileName)).resolve() == alvo:
                doc = d
                break
        except Exception:
            continue

    if doc is None:
        return False

    try:
        if salvar_documento:
            doc.Save()
            doc.Close()
        else:
            doc.Close(False)
    except Exception:
        try:
            doc.Close()
        except Exception:
            return False
    return True


def diagnosticar_campos(cdr_path: Path) -> int:
    app = obter_app_corel()

    doc = None
    for d in app.Documents:
        try:
            if Path(str(d.FullFileName)).resolve() == cdr_path.resolve():
                doc = d
                break
        except Exception:
            continue

    if doc is None:
        doc = app.OpenDocument(str(cdr_path))

    unidade_original = doc.Unit
    ref_original = doc.ReferencePoint

    try:
        doc.Unit = CDR_UNIT_CENTIMETER
        doc.ReferencePoint = CDR_REF_CENTER
        infos = listar_textos(doc.ActivePage)
        infos = sorted(infos, key=lambda i: (-i.y, i.x))

        print("=== Textos encontrados (ordem: topo->baixo, esquerda->direita) ===")
        for i, info in enumerate(infos, start=1):
            texto = re.sub(r"\s+", " ", info.text).strip()
            if len(texto) > 70:
                texto = texto[:67] + "..."
            print(
                f"{i:02d}. nome='{info.name or '-'}' x={info.x:.2f} y={info.y:.2f} "
                f"w={info.w:.2f} texto='{texto}'"
            )
        print("=== fim ===")
    finally:
        doc.Unit = unidade_original
        doc.ReferencePoint = ref_original

    return 0


def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Atualiza 4 produtos no template de ofertas do CorelDRAW.",
    )
    parser.add_argument(
        "--config",
        default="dados_ofertas.json",
        help="Caminho do arquivo JSON com os produtos.",
    )
    parser.add_argument(
        "--arquivo-cdr",
        default=None,
        help="Sobrescreve o caminho do arquivo CDR informado no JSON.",
    )
    parser.add_argument(
        "--salvar-em",
        default=None,
        help="Se informado, salva uma copia nesse caminho. Se nao, salva no proprio arquivo.",
    )
    parser.add_argument(
        "--diagnostico",
        action="store_true",
        help="Lista os objetos de texto detectados no CDR e encerra.",
    )
    parser.add_argument(
        "--imprimir",
        action="store_true",
        help="Imprime o documento atualizado.",
    )
    parser.add_argument(
        "--nao-salvar",
        action="store_true",
        help="Atualiza/imprime sem salvar alteracoes no CDR.",
    )
    parser.add_argument(
        "--copias",
        type=int,
        default=1,
        help="Quantidade de copias por impressao.",
    )
    parser.add_argument(
        "--impressora",
        default=None,
        help="Nome da impressora no Windows (opcional).",
    )
    parser.add_argument(
        "--fechar-documento",
        action="store_true",
        help="Fecha o documento no Corel ao final.",
    )
    return parser


def main() -> int:
    parser = construir_parser()
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config nao encontrado: {config_path}", file=sys.stderr)
        return 2

    config = carregar_config(config_path)

    if args.arquivo_cdr:
        cdr_path = Path(args.arquivo_cdr)
    else:
        cdr_cfg = config.get("arquivo_cdr")
        if not cdr_cfg:
            print("Defina 'arquivo_cdr' no JSON ou use --arquivo-cdr", file=sys.stderr)
            return 2
        cdr_path = Path(str(cdr_cfg))

    if not cdr_path.exists():
        print(f"Arquivo CDR nao encontrado: {cdr_path}", file=sys.stderr)
        return 2

    salvar_em = Path(args.salvar_em) if args.salvar_em else None

    if args.diagnostico:
        try:
            return diagnosticar_campos(cdr_path)
        except Exception as exc:
            print(f"Falha no diagnostico: {exc}", file=sys.stderr)
            return 1

    try:
        atualizar_documento(
            config=config,
            cdr_path=cdr_path,
            salvar_em=salvar_em,
            imprimir=bool(args.imprimir),
            copias=int(args.copias),
            impressora=args.impressora,
            salvar_documento=not bool(args.nao_salvar),
            fechar_documento=bool(args.fechar_documento),
        )
    except Exception as exc:
        print(f"Falha na automacao: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
