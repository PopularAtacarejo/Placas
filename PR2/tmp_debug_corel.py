from pathlib import Path
import json
import traceback

import win32com.client

import atualizar_ofertas_corel as ac


LOG_PATH = Path("tmp_debug_corel.log")


def log(msg: str) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")


lote = [
    {"descricao": "Gelatines Docile Beijo 1kg", "unidade": "Unid.", "preco": "4,98"},
    {"descricao": "Wafer Kinder Tronky 18g", "unidade": "Unid.", "preco": "4,98"},
    {"descricao": "Bisc Nesfit Banana/Aveia/Canela 160g", "unidade": "Unid.", "preco": "4,98"},
    {"descricao": "Bisc Cr Cracker Vitarella Pão Assado 350g", "unidade": "Unid.", "preco": "1,68"},
]

cfg = json.loads(Path("dados_ofertas.json").read_text(encoding="utf-8-sig"))
prods = ac.parse_produtos({"produtos": lote, "permitir_produto_vazio": True})
LOG_PATH.write_text("", encoding="utf-8")
app = win32com.client.Dispatch("CorelDRAW.Application")
app.Visible = True

log("abrindo documento")
doc = app.OpenDocument(str(Path("Validade.cdr").resolve()))

try:
    doc.Unit = ac.CDR_UNIT_CENTIMETER
    doc.ReferencePoint = ac.CDR_REF_CENTER
    log("selecionando pagina/mapa")
    page, mapa = ac.selecionar_pagina_e_mapa(doc)
    log(str(sorted(mapa.keys())))

    for i, produto in enumerate(prods, start=1):
        log(f"produto {i}: descricao")
        ac.aplicar_descricao(
            shape=mapa[f"desc_{i}"],
            texto=produto.descricao,
            fonte=str(cfg.get("fonte_descricao_unidade", "TangoSans")),
            tamanho_min=float(cfg.get("tamanho_descricao_min", 24)),
            tamanho_max=float(cfg.get("tamanho_descricao_max", 38)),
            largura_max_cm=float(cfg.get("largura_max_descricao_cm", 5.0)),
            altura_max_cm=float(cfg.get("altura_max_descricao_cm", 1.9)),
            max_linhas_quebra=int(cfg.get("max_linhas_descricao", 2)),
            preferir_duas_linhas=bool(cfg.get("preferir_descricao_duas_linhas", True)),
            palavras_min_duas_linhas=int(cfg.get("descricao_palavras_min_duas_linhas", 3)),
            tamanho_min_emergencia=float(cfg.get("tamanho_descricao_min_emergencia", 16)),
        )
        log(f"produto {i}: preco")
        ac.aplicar_preco_separado(
            shape_int=mapa[f"preco_int_{i}"],
            shape_dec=mapa[f"preco_dec_{i}"],
            preco_inteiro=produto.preco_inteiro,
            preco_decimal=produto.preco_decimal,
            fonte=str(cfg.get("fonte_descricao_unidade", "TangoSans")),
            tamanho_preco_min=float(cfg.get("tamanho_preco_min", 90)),
            tamanho_preco_max=float(cfg.get("tamanho_preco_max", 170)),
            fator_centavos=float(cfg.get("fator_tamanho_centavos", 0.37)),
            vshift_centavos=int(cfg.get("deslocamento_vertical_centavos", 118)),
            largura_max_preco_int_cm=None,
            altura_max_preco_int_cm=float(cfg.get("altura_max_preco_int_cm", 3.7)),
        )
        log(f"produto {i}: centavos")
        ac.posicionar_centavos_ao_lado_inteiro(
            mapa[f"preco_int_{i}"],
            mapa[f"preco_dec_{i}"],
            gap_cm=float(cfg.get("gap_preco_centavos_cm", 0.10)),
        )
        ac.centralizar_par_preco_x(
            mapa[f"preco_int_{i}"],
            mapa[f"preco_dec_{i}"],
            float(mapa[f"desc_{i}"].PositionX),
        )
        log(f"produto {i}: unidade texto")
        ac.set_story_text(mapa[f"unid_{i}"], produto.unidade or "")
        ac.set_story_font_size(
            mapa[f"unid_{i}"],
            font_name=str(cfg.get("fonte_descricao_unidade", "TangoSans")),
        )
        tamanho_unidade = ac.calcular_tamanho_unidade(
            tamanho_centavos=float(mapa[f"preco_dec_{i}"].Text.Story.Size),
            fator_tamanho_unidade=float(cfg.get("fator_tamanho_unidade", 0.42)),
            tamanho_unidade_max=float(cfg.get("tamanho_unidade_max", 28)),
        )
        log(f"produto {i}: unidade tamanho")
        ac.set_story_font_size(
            mapa[f"unid_{i}"],
            size_pt=float(tamanho_unidade),
            reset_vert_shift=True,
        )
        log(f"produto {i}: unidade pos")
        ac.posicionar_unidade_abaixo_centavos(
            shape_centavos=mapa[f"preco_dec_{i}"],
            shape_unidade=mapa[f"unid_{i}"],
            deslocamento_x_cm=float(cfg.get("deslocamento_x_unidade_cm", 0.0)),
            gap_min_cm=float(cfg.get("gap_unidade_min_cm", 0.28)),
            gap_fator_altura=float(cfg.get("gap_unidade_fator_altura", 0.08)),
        )

    log("fim ok")
except Exception:
    with LOG_PATH.open("a", encoding="utf-8") as f:
        traceback.print_exc(file=f)
finally:
    try:
        doc.Close()
    except Exception:
        pass
