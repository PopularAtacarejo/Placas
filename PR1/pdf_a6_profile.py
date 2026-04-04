from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence


def merge_four_plate_pdfs_grid(
    input_pdfs: Sequence[Optional[Path]],
    output_pdf: Path,
    gap_x_mm: float = 0.0,
    gap_y_mm: float = 0.0,
) -> None:
    """
    Junta ate 4 PDFs de placa em uma pagina final no formato 2x2 (A6).
    Ordem dos slots:
      1) superior esquerdo
      2) superior direito
      3) inferior esquerdo
      4) inferior direito
    """
    try:
        from pypdf import PageObject, PdfReader, PdfWriter, Transformation  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Dependencia ausente: pypdf. Instale com `pip install -r requirements.txt`."
        ) from exc

    slots: list[Optional[Path]] = list(input_pdfs[:4])
    while len(slots) < 4:
        slots.append(None)

    pages: list[Optional[object]] = []
    max_width = 0.0
    max_height = 0.0

    for pdf_path in slots:
        if not pdf_path:
            pages.append(None)
            continue
        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            pages.append(None)
            continue
        page = reader.pages[0]
        pages.append(page)
        max_width = max(max_width, float(page.mediabox.width))
        max_height = max(max_height, float(page.mediabox.height))

    if max_width <= 0 or max_height <= 0:
        raise RuntimeError("Nao foi possivel ler paginas validas para compor o perfil A6.")

    # Permite gap negativo para "encostar" mais as placas quando necessario.
    gap_x_points = float(gap_x_mm) * 72.0 / 25.4
    gap_y_points = float(gap_y_mm) * 72.0 / 25.4

    target_width = (max_width * 2.0) + gap_x_points
    target_height = (max_height * 2.0) + gap_y_points
    out_page = PageObject.create_blank_page(width=target_width, height=target_height)

    slot_positions = [
        (0.0, max_height + gap_y_points),
        (max_width + gap_x_points, max_height + gap_y_points),
        (0.0, 0.0),
        (max_width + gap_x_points, 0.0),
    ]

    for page, (slot_x, slot_y) in zip(pages, slot_positions):
        if page is None:
            continue
        page_width = float(page.mediabox.width)
        page_height = float(page.mediabox.height)
        tx = slot_x + max((max_width - page_width) / 2.0, 0.0)
        ty = slot_y + max((max_height - page_height) / 2.0, 0.0)
        out_page.merge_transformed_page(page, Transformation().translate(tx=tx, ty=ty))

    writer = PdfWriter()
    writer.add_page(out_page)
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    with output_pdf.open("wb") as fh:
        writer.write(fh)
