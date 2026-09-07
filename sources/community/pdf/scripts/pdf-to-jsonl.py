import json
import sys
import os
import argparse
import subprocess
import tempfile
from pathlib import Path

try:
    import fitz
except ImportError:
    print("PyMuPDF is required. Install: pip install pymupdf")
    sys.exit(1)


ANNOT_TYPE_NAMES = {
    0: "Text",
    1: "Link",
    2: "FreeText",
    3: "Line",
    4: "Square",
    5: "Circle",
    6: "Polygon",
    7: "PolyLine",
    8: "Highlight",
    9: "Underline",
    10: "Squiggly",
    11: "StrikeOut",
    12: "Stamp",
    13: "Caret",
    14: "Ink",
    15: "Popup",
    16: "FileAttachment",
    17: "Sound",
    18: "Movie",
    19: "Widget",
    20: "Screen",
    21: "PrinterMark",
    22: "TrapNet",
    23: "Watermark",
    24: "Redact",
    25: "Projection",
    26: "RichMedia",
    27: "WebMedia",
}

LINK_KIND_NAMES = {
    0: "none",
    1: "goto",
    2: "uri",
    3: "launch",
    4: "named",
    5: "gotor",
}


def _round_bbox(bbox):
    return {
        "x0": round(bbox[0], 1),
        "y0": round(bbox[1], 1),
        "x1": round(bbox[2], 1),
        "y1": round(bbox[3], 1),
    }


def extract_blocks(page):
    blocks = []
    raw = page.get_text("dict")["blocks"]
    for b in raw:
        if b["type"] == 0:
            for line in b["lines"]:
                for span in line["spans"]:
                    blocks.append(
                        {
                            "type": "text",
                            "text": span["text"],
                            "font": span["font"],
                            "size": round(span["size"], 1),
                            "bold": bool(span["flags"] & 2),
                            "italic": bool(span["flags"] & 1),
                            "color": span["color"],
                            **_round_bbox(span["bbox"]),
                        }
                    )
        elif b["type"] == 1:
            blocks.append(
                {
                    "type": "image",
                    "width": b["width"],
                    "height": b["height"],
                    **_round_bbox(b["bbox"]),
                }
            )
    return blocks


def extract_tables(page):
    tables = []
    for tab in page.find_tables().tables:
        rows = []
        for row in tab.extract():
            rows.append([cell.strip() if cell else "" for cell in row])
        tables.append(
            {
                "header": rows[0] if rows else [],
                "rows": rows[1:] if len(rows) > 1 else [],
                "bbox": _round_bbox(tab.bbox),
            }
        )
    return tables


def extract_images(page):
    return [
        {
            "xref": img[0],
            "width": img[2],
            "height": img[3],
            "bits": img[4] if len(img) > 4 else None,
        }
        for img in page.get_images(full=True)
    ]


def extract_links(page):
    links = []
    for link in page.get_links():
        kind = link.get("kind", 0)
        entry = {
            "kind": kind,
            "kind_name": LINK_KIND_NAMES.get(kind, "unknown"),
            "bbox": _round_bbox(link["from"]),
        }
        if kind == fitz.LINK_URI:
            entry["uri"] = link.get("uri", "")
        elif kind == fitz.LINK_GOTO:
            entry["page"] = link.get("page", 0)
        elif kind == fitz.LINK_GOTOR:
            entry["page"] = link.get("page", 0)
            entry["file"] = link.get("file", "")
        elif kind == fitz.LINK_LAUNCH:
            entry["file"] = link.get("file", "")
        links.append(entry)
    return links


def extract_annotations(page):
    annots = []
    for annot in page.annots():
        atype = annot.type[0] if annot.type else 0
        info = annot.info or {}
        colors = annot.colors
        entry = {
            "type": atype,
            "type_name": ANNOT_TYPE_NAMES.get(atype, "Unknown"),
            "content": info.get("content", ""),
            "title": info.get("title", ""),
            "name": info.get("name", ""),
            "bbox": _round_bbox(annot.rect),
        }
        if colors:
            if colors.get("stroke"):
                entry["color"] = colors["stroke"]
            if colors.get("fill"):
                entry["fill_color"] = colors["fill"]
        entry["opacity"] = annot.opacity
        annots.append(entry)
    return annots


def extract_widgets(page):
    widgets = []
    for widget in page.widgets():
        field_type = widget.field_type_string
        entry = {
            "field_name": widget.field_name or "",
            "field_type": field_type or "",
            "field_value": widget.field_value if widget.field_value is not None else "",
            "field_label": widget.field_label or "",
            "bbox": _round_bbox(widget.rect),
        }
        if field_type == "Button":
            entry["button_caption"] = widget.button_caption or ""
        if field_type in ("Choice", "ListBox"):
            entry["choice_values"] = widget.choice_values or []
        widgets.append(entry)
    return widgets


def build_markdown(blocks):
    lines = []
    for b in blocks:
        if b["type"] != "text":
            continue
        text = b["text"].strip()
        if not text:
            continue
        if b["bold"] and b["size"] >= 14:
            lines.append(f"# {text}")
        elif b["bold"] and b["size"] >= 10:
            lines.append(f"## {text}")
        elif b["bold"] and b["size"] >= 9:
            lines.append(f"### {text}")
        elif b["bold"]:
            lines.append(f"**{text}**")
        elif b["italic"]:
            lines.append(f"*{text}*")
        else:
            lines.append(text)
    return "\n".join(lines)


def extract_document_info(doc, pdf_path):
    meta = doc.metadata or {}
    toc = doc.get_toc()
    embedded = []
    for i in range(doc.embfile_count()):
        info = doc.embfile_info(i)
        embedded.append(
            {
                "name": info.get("name", ""),
                "description": info.get("desc", ""),
                "size": info.get("size", 0),
                "mimetype": info.get("mimetype", ""),
            }
        )
    page_labels = doc.get_page_labels() or []
    return {
        "file_name": pdf_path.name,
        "path": str(pdf_path.resolve()),
        "file_size": pdf_path.stat().st_size,
        "page_count": doc.page_count,
        "metadata": {k: v for k, v in meta.items() if v},
        "toc": [
            {"level": entry[0], "title": entry[1], "page": entry[2]} for entry in toc
        ],
        "embedded_files": embedded,
        "page_labels": [
            {
                "start_page": label["startpage"],
                "style": label["style"],
                "prefix": label["prefix"],
                "start": label["firstpagenum"],
            }
            for label in page_labels
        ],
    }


def extract_pdf(pdf_path, ocr=False):
    rows = []
    doc = None
    temp_dir = None
    try:
        path = pdf_path
        if ocr:
            temp_dir = tempfile.mkdtemp()
            ocr_path = Path(temp_dir) / pdf_path.name
            subprocess.run(
                ["ocrmypdf", "--force-ocr", str(pdf_path), str(ocr_path)],
                check=True,
                capture_output=True,
            )
            path = ocr_path

        doc = fitz.open(path)
        page_count = doc.page_count

        for i in range(page_count):
            page = doc[i]
            blocks = extract_blocks(page)
            tables = extract_tables(page)
            images = extract_images(page)
            links = extract_links(page)
            annots = extract_annotations(page)
            widgets = extract_widgets(page)
            text = page.get_text("text") or ""
            markdown = build_markdown(blocks)

            rows.append(
                {
                    "file_name": pdf_path.name,
                    "path": str(pdf_path.resolve()),
                    "page": i + 1,
                    "page_count": page_count,
                    "text": text.strip(),
                    "markdown": markdown.strip(),
                    "blocks": blocks,
                    "tables": tables,
                    "images": images,
                    "links": links,
                    "annotations": annots,
                    "widgets": widgets,
                }
            )

        doc_rows = [extract_document_info(doc, pdf_path)]

    finally:
        if doc:
            doc.close()
        if temp_dir:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    return rows, doc_rows


def main():
    parser = argparse.ArgumentParser(description="Convert PDFs to JSONL for Coral")
    parser.add_argument("--dir", help="Directory of PDFs to process")
    parser.add_argument("--files", nargs="*", help="Individual PDF files to process")
    parser.add_argument(
        "--out",
        default=str(Path.home() / ".coral" / "pdf" / "pages.jsonl"),
        help="Output JSONL path for pages table",
    )
    parser.add_argument(
        "--out-documents",
        default=str(Path.home() / ".coral" / "pdf" / "documents.jsonl"),
        help="Output JSONL path for documents table",
    )
    parser.add_argument(
        "--recursive", action="store_true", help="Scan --dir recursively"
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Run OCRmyPDF on scanned PDFs before extraction",
    )
    args = parser.parse_args()

    pdf_paths = []
    if args.dir:
        d = Path(args.dir)
        pattern = "**/*.pdf" if args.recursive else "*.pdf"
        pdf_paths.extend(sorted(d.glob(pattern)))
    if args.files:
        pdf_paths.extend(Path(f) for f in args.files)

    if not pdf_paths:
        print("No PDF files found.", file=sys.stderr)
        sys.exit(1)

    pages_path = Path(args.out)
    docs_path = Path(args.out_documents)
    pages_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.parent.mkdir(parents=True, exist_ok=True)

    total_pages = 0
    failures = []
    with open(pages_path, "w") as pf, open(docs_path, "w") as df:
        for pdf_path in pdf_paths:
            try:
                page_rows, doc_rows = extract_pdf(pdf_path, ocr=args.ocr)
            except Exception as e:
                print(f"Error processing {pdf_path}: {e}", file=sys.stderr)
                failures.append(pdf_path.name)
                doc_rows = [
                    {
                        "file_name": pdf_path.name,
                        "path": str(pdf_path.resolve()),
                        "file_size": pdf_path.stat().st_size
                        if pdf_path.exists()
                        else 0,
                        "page_count": 0,
                        "metadata": {},
                        "toc": [],
                        "embedded_files": [],
                        "page_labels": [],
                    }
                ]
                page_rows = []
            for row in page_rows:
                pf.write(json.dumps(row, ensure_ascii=False) + "\n")
            for row in doc_rows:
                df.write(json.dumps(row, ensure_ascii=False) + "\n")
            total_pages += len(page_rows)
            print(
                f"  {pdf_path.name}: {len(page_rows)} page(s), {len(doc_rows)} document(s)"
            )

    print(f"\nWrote {total_pages} page row(s) to {pages_path}")
    print(f"Wrote {len(pdf_paths)} document row(s) to {docs_path}")

    if failures:
        print(
            f"\nERROR: {len(failures)} PDF(s) could not be converted: {', '.join(failures)}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
