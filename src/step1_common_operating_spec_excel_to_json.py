#!/usr/bin/env python3
"""
공통 운영명세서 엑셀 → JSON 변환기

통합 방향
- 시트 타입 판별과 후처리 구조는 NISC03 스타일
- 운영 시트 파싱 엔진은 KAIT05의 강한 layout inference 우선 사용
- layout inference 결과가 약하거나 실패하면 시트 타입별 fallback parser 적용
- retrieval_rows 생성/중복 제거는 마지막 공통 후처리에서 수행
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

if __package__ in {None, ""}:
    # Allow running as a script: `python src/step1_common_operating_spec_excel_to_json.py ...`
    # Ensure repo-root `src/` is importable (e.g., for `summary_doc`).
    import sys

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

DEFAULT_INPUT_PATH = (
    "excel_test_file/template/step1_2024-2025_all/NISC/a25368d35b__정보보호 관리체계 인증 운영명세서.xlsx"
)
DEFAULT_OUTPUT_DIR = "excel_test_file/result_normalized_v3/step1_2024-2025_all/common/error"

SHEET_NAME = "ISMS"
SUMMARY_TEMPLATE = "COMMON_OPERATING_SPEC"
MIN_OP_STATUS_FILLED_FOR_SHEET_EXTRACTION = 5  # 앵커(항목코드) 기반: "운영현황" 열에 값이 채워진 셀 개수 기준

FIELD_CODE_RE = re.compile(r"^\d+\.\d+\.?$")
ITEM_CODE_RE = re.compile(r"^\d+\.\d+\.\d+\.?$")


def _cell_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def _clean_text(text: str) -> str:
    text = _cell_str(text)
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _join_nonempty(parts: list[str], sep: str = " | ") -> str:
    return sep.join([p for p in parts if _cell_str(p)])


def _normalize_code(code: str) -> str:
    return _cell_str(code).rstrip(".")


def _normalize_document_title(text: str) -> str:
    value = _clean_text(text)
    value = re.sub(r"^[▣□■◆◇▶▷*]+", "", value).strip()
    return value


def _is_section_title(value: str) -> bool:
    v = _cell_str(value).replace("\n", " ")
    if not v:
        return False
    if re.match(r"^[가나다라마바사아자차카타파하]\.", v):
        return True
    if re.match(r"^\d+\.(?!\d)", v):
        return True
    return False


def _normalize_header_key(text: str) -> str:
    value = _clean_text(text)
    if not value:
        return ""
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"[①②③④⑤⑥⑦⑧⑨⑩]", "", value)

    if value == "분야":
        return "분야"
    if value == "항목":
        return "항목"
    if "통제분야" in value:
        return "통제분야"
    if value == "구분":
        return "구분"
    if value == "No":
        return "No"
    if "상세내용" in value:
        return "상세내용"
    if "점검항목" in value:
        return "점검항목"
    if "운영여부" in value or "적용여부" in value:
        return "운영여부"
    if "인증구분" in value:
        return "인증구분"
    if "운영현황" in value or "운영 현황" in value:
        return "운영현황"
    if "관련문서" in value or "관련근거" in value:
        return "관련문서"
    if "기록" in value or "이행증적목록" in value:
        return "기록"
    return ""


def _normalize_header_text(text: str) -> str:
    return re.sub(r"\s+", "", _cell_str(text))


def read_workbook(path: str | Path) -> pd.ExcelFile:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.ExcelFile(path, engine="openpyxl")


def _extract_title_and_org(df: pd.DataFrame) -> Dict[str, str]:
    title = ""
    org = ""
    max_rows = min(10, len(df))
    max_cols = min(10, df.shape[1])

    def safe(i: int, j: int) -> str:
        if i < 0 or j < 0:
            return ""
        if i >= len(df) or j >= df.shape[1]:
            return ""
        return _cell_str(df.iloc[i, j])

    for i in range(max_rows):
        for j in range(max_cols):
            v = _cell_str(df.iloc[i, j])
            if not v:
                continue
            if not title:
                title = _normalize_document_title(v)
            if "기업명" in v:
                m = re.search(r"기업명\s*[:：]?\s*([^ )]+.*?)(?:\)|$)", v)
                if m and m.group(1).strip():
                    org = m.group(1).strip()
                else:
                    cand_right = safe(i, j + 1)
                    cand_down = safe(i + 1, j)
                    for cand in (cand_right, cand_down):
                        if cand and "기업명" not in cand:
                            org = cand
                            break
    return {
        "document_title": _normalize_document_title(title),
        "기업명": _clean_text(org),
    }


def _find_document_title(df: pd.DataFrame) -> str:
    scan_rows = min(len(df), 10)
    candidates = []
    for i in range(scan_rows):
        for j in range(df.shape[1]):
            value = _cell_str(df.iloc[i, j])
            if value and ("운영명세서" in value or "기업명" in value):
                candidates.append((i, j, value))
    if candidates:
        candidates.sort(key=lambda x: (x[0], x[1], -len(x[2])))
        return _normalize_document_title(candidates[0][2])
    fallback = _cell_str(df.iloc[1, 1]) if len(df) > 1 and df.shape[1] > 1 else ""
    return _normalize_document_title(fallback)


def _extract_company_from_title(title: str) -> str:
    value = _clean_text(title)
    if not value:
        return ""
    if "기업명" in value:
        tail = value.split("기업명", 1)[1]
        tail = re.sub(r"^[\s:：]*", "", tail)
        tail = tail.rsplit(")", 1)[0] if ")" in tail else tail
        candidate = _clean_text(tail).strip(" _-")
        if candidate:
            return candidate
    cleaned = value.lstrip("▣").strip()
    match = re.search(r"(.+?)\s+(?:정보보호(?:및개인정보보호)?\s*관리체계|ISMS(?:-P)?)", cleaned)
    if match:
        return match.group(1).strip(" _-()[]")
    return ""


def _extract_company_from_filename(path: str | Path) -> str:
    stem = Path(path).stem

    bracket_candidates = re.findall(r"\(([^)]+)\)|\[([^\]]+)\]", stem)
    for left, right in bracket_candidates:
        candidate = _clean_text(left or right).strip(" _-()[]")
        if candidate and not re.search(r"^(별첨|붙임\d*|단일인증|간편인증|ISMS(?:-P)?)$", candidate, re.IGNORECASE):
            return candidate

    normalized = re.sub(r"^\(별첨\)_?", "", stem)
    normalized = re.sub(r"^\[별첨\]_?", "", normalized)
    normalized = re.sub(r"^\d+[\._\-\s]*", "", normalized)
    normalized = re.sub(r"^\d{4}_?", "", normalized)
    match = re.search(r"(.+?)(?:_|\s)*(?:정보보호|운영명세서)", normalized)
    if match:
        return _clean_text(match.group(1)).strip(" _-()[]")
    return ""


def _apply_metadata_fallbacks(
    parsed: Dict[str, Any],
    sheet_meta: Dict[str, str],
    fallback_company: str,
    fallback_title: str,
) -> Dict[str, Any]:
    metadata = parsed.setdefault("metadata", {})
    if not metadata.get("기업명"):
        metadata["기업명"] = (
            sheet_meta.get("기업명")
            or _extract_company_from_title(sheet_meta.get("document_title", ""))
            or fallback_company
        )
    if not metadata.get("document_title"):
        metadata["document_title"] = sheet_meta.get("document_title") or fallback_title
    return parsed


def _find_layout_from_header(df: pd.DataFrame) -> dict:
    best = {}
    best_score = -1
    scan_rows = min(len(df), 20)

    for i in range(scan_rows):
        mapping = {}
        for j in range(df.shape[1]):
            key = _normalize_header_key(df.iloc[i, j])
            if key and key not in mapping:
                mapping[key] = j
        score = len(mapping)
        if score > best_score and {"분야", "항목", "상세내용"} <= set(mapping):
            best = {"header_row": i, **mapping}
            best_score = score

    if not best:
        return {}

    ncols = df.shape[1]
    header_row = best["header_row"]
    headers = [_normalize_header_text(df.iloc[header_row, j]) for j in range(ncols)]

    def find_name_col(code_col: int, prefixes: tuple[str, ...]) -> int | None:
        for j, header in enumerate(headers):
            if j == code_col:
                continue
            if any(prefix in header for prefix in prefixes) and any(
                suffix in header for suffix in ("명", "이름", "설명")
            ):
                return j

        for offset in (1, 2):
            cand = code_col + offset
            if cand < ncols:
                return cand
        return None

    return {
        "header_row": header_row,
        "field_code_col": best["분야"],
        "field_name_col": find_name_col(best["분야"], ("분야", "통제분야")),
        "item_code_col": best["항목"],
        "item_name_col": find_name_col(best["항목"], ("항목",)),
        "detail_col": best.get("상세내용"),
        "op_flag_col": best.get("운영여부"),
        "cert_type_col": best.get("인증구분"),
        "status_col": best.get("운영현황"),
        "related_docs_col": best.get("관련문서"),
        "records_col": best.get("기록"),
    }


def _find_layout_from_item_anchor(df: pd.DataFrame) -> dict:
    ncols = df.shape[1]
    for i in range(len(df)):
        for j in range(max(0, ncols - 1)):
            code = _cell_str(df.iloc[i, j]).rstrip(". ")
            name = _cell_str(df.iloc[i, j + 1]) if j + 1 < ncols else ""
            if not ITEM_CODE_RE.match(code):
                continue
            if not name or FIELD_CODE_RE.match(name) or ITEM_CODE_RE.match(name) or _is_section_title(name):
                continue
            return {
                "header_row": i - 1,
                "field_code_col": j - 2 if j - 2 >= 0 else None,
                "field_name_col": j - 1 if j - 1 >= 0 else None,
                "item_code_col": j,
                "item_name_col": j + 1 if j + 1 < ncols else None,
                "detail_col": j + 2 if j + 2 < ncols else None,
                "op_flag_col": j + 3 if j + 3 < ncols else None,
                "cert_type_col": j + 4 if j + 4 < ncols else None,
                "status_col": j + 5 if j + 5 < ncols else None,
                "related_docs_col": j + 6 if j + 6 < ncols else None,
                "records_col": j + 7 if j + 7 < ncols else None,
            }
    return {}


def _find_layout_from_kait_numbered_header(df: pd.DataFrame) -> dict:
    scan_rows = min(len(df), 20)

    for i in range(scan_rows):
        keys = [_normalize_header_key(df.iloc[i, j]) for j in range(df.shape[1])]
        no_positions = [idx for idx, key in enumerate(keys) if key == "No"]
        if len(no_positions) < 2:
            continue
        if "통제분야" not in keys or "항목" not in keys or "점검항목" not in keys:
            continue

        return {
            "header_row": i,
            "title_col": no_positions[1],
            "field_code_col": no_positions[0],
            "field_name_col": keys.index("통제분야"),
            "item_code_col": no_positions[1],
            "item_name_col": keys.index("항목"),
            "detail_col": keys.index("점검항목"),
            "op_flag_col": next((idx for idx, key in enumerate(keys) if key == "운영여부"), None),
            "cert_type_col": next((idx for idx, key in enumerate(keys) if key == "인증구분"), None),
            "status_col": next((idx for idx, key in enumerate(keys) if key == "운영현황"), None),
            "related_docs_col": next((idx for idx, key in enumerate(keys) if key == "관련문서"), None),
            "records_col": next((idx for idx, key in enumerate(keys) if key == "기록"), None),
        }

    return {}


def _infer_layout(df: pd.DataFrame) -> dict:
    layout = _find_layout_from_header(df)
    anchor_layout = _find_layout_from_item_anchor(df)
    numbered_layout = _find_layout_from_kait_numbered_header(df)

    if not layout:
        return numbered_layout or anchor_layout
    if not anchor_layout:
        return numbered_layout or layout

    merged = dict(layout)
    for candidate in (anchor_layout, numbered_layout):
        for key, value in candidate.items():
            if merged.get(key) is None and value is not None:
                merged[key] = value
    return merged


def _count_non_empty_op_status_cells(df: pd.DataFrame, layout: dict) -> int:
    """병합 레이아웃의 운영현황 열에서 비어 있지 않은 셀 개수."""
    col = layout.get("status_col")
    if col is None or col < 0 or col >= df.shape[1]:
        return 0
    header_row = layout.get("header_row")
    start = header_row + 1 if header_row is not None else 0
    n = 0
    for i in range(max(0, start), len(df)):
        if _cell_str(df.iloc[i, col]).strip():
            n += 1
    return n


def _sheet_qualifies_anchor_min_op_status(df: pd.DataFrame) -> bool:
    """항목코드 앵커가 있고, 운영현황 열에 값이 기준 개수 이상 채워진 시트만 운영 데이터로 추출."""
    if not _find_layout_from_item_anchor(df):
        return False
    layout = _infer_layout(df)
    return (
        layout.get("status_col") is not None
        and _count_non_empty_op_status_cells(df, layout) >= MIN_OP_STATUS_FILLED_FOR_SHEET_EXTRACTION
    )


def _get_cell_by_layout(df: pd.DataFrame, row_idx: int, col_idx: int | None) -> str:
    if col_idx is None or col_idx < 0 or col_idx >= df.shape[1]:
        return ""
    return _cell_str(df.iloc[row_idx, col_idx])


def _is_title_row_layout(df: pd.DataFrame, row_idx: int, layout: dict) -> bool:
    code_col = layout.get("title_col", layout.get("field_code_col"))
    value = _get_cell_by_layout(df, row_idx, code_col)
    if not value or not _is_section_title(value):
        return False

    check_cols = {
        layout.get("field_name_col"),
        layout.get("item_code_col"),
        layout.get("item_name_col"),
        layout.get("detail_col"),
        layout.get("op_flag_col"),
        layout.get("cert_type_col"),
        layout.get("status_col"),
        layout.get("related_docs_col"),
        layout.get("records_col"),
    }
    for j in check_cols:
        if j is None:
            continue
        if j == code_col:
            continue
        if _get_cell_by_layout(df, row_idx, j):
            return False
    return True


def _count_items(parsed: Dict[str, Any]) -> int:
    count = 0
    for section in parsed.get("sections", []):
        for field in section.get("fields", []):
            count += len(field.get("items", []))
    return count


def _group_flat_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_title: Dict[str, Dict[tuple, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(lambda: {"분야_code": None, "분야_name": None, "items": []})
    )

    for row in rows:
        title = _clean_text(row.get("title", ""))
        field_code = _clean_text(row.get("분야_code", ""))
        field_name = _clean_text(row.get("분야_name", ""))
        item_code = _clean_text(row.get("항목_code", ""))
        item_name = _clean_text(row.get("항목_name", ""))

        if not field_code or not item_code:
            continue

        field_key = (field_code, field_name)
        field = by_title[title][field_key]
        field["분야_code"] = field_code
        field["분야_name"] = field_name
        field["items"].append(
            {
                "항목_code": item_code,
                "항목_name": item_name,
                "상세내용": _clean_text(row.get("상세내용", "")),
                "운영여부": _clean_text(row.get("운영여부", "")),
                "인증구분": _clean_text(row.get("인증구분", "")),
                "운영현황": _clean_text(row.get("운영현황", "")),
                "관련문서": _clean_text(row.get("관련문서", "")),
                "기록": _clean_text(row.get("기록", "")),
            }
        )

    sections: List[Dict[str, Any]] = []
    for title, fields_dict in by_title.items():
        sections.append({"title": title, "fields": list(fields_dict.values())})
    return sections


def parse_operating_sheet_with_layout_inference(
    df: pd.DataFrame,
    *,
    sheet_name: str = SHEET_NAME,
    fallback_company: str = "",
) -> Dict[str, Any]:
    meta = _extract_title_and_org(df)
    if not meta.get("document_title"):
        meta["document_title"] = _find_document_title(df)
    if not meta.get("기업명"):
        meta["기업명"] = _extract_company_from_title(meta.get("document_title", "")) or _clean_text(fallback_company)

    layout = _infer_layout(df)
    if not layout:
        return {"metadata": meta, "sections": [], "sheet": sheet_name}

    def split_code_and_name(value: str, *, kind: str) -> tuple[str, str]:
        text = _cell_str(value).replace("\n", " ").strip()
        if not text:
            return ("", "")
        if kind == "field":
            m = re.match(r"^\s*(\d+\.\d+)\s*[\.\)]?\s*(.*)\s*$", text)
        else:
            m = re.match(r"^\s*(\d+\.\d+\.\d+)\s*[\.\)]?\s*(.*)\s*$", text)
        if not m:
            return ("", text)
        return (
            _cell_str(m.group(1)).rstrip("."),
            _cell_str(m.group(2)).strip("() ").strip(),
        )

    data_start = max(layout.get("header_row", 3) + 1, 0)
    title_col = layout.get("title_col", layout.get("field_code_col"))

    prev_title = ""
    prev_field_code = ""
    prev_field_name = ""
    prev_item_code = ""
    prev_item_name = ""
    flat_rows: List[Dict[str, Any]] = []

    for i in range(data_start, len(df)):
        field_code_raw = _get_cell_by_layout(df, i, layout.get("field_code_col")).replace("\n", " ")
        field_name_raw = _get_cell_by_layout(df, i, layout.get("field_name_col")).replace("\n", " ")
        item_code_raw = _get_cell_by_layout(df, i, layout.get("item_code_col")).replace("\n", " ")
        item_name_raw = _get_cell_by_layout(df, i, layout.get("item_name_col")).replace("\n", " ")
        detail = _get_cell_by_layout(df, i, layout.get("detail_col")).replace("\n", " ")
        oper = _get_cell_by_layout(df, i, layout.get("op_flag_col"))
        cert = _get_cell_by_layout(df, i, layout.get("cert_type_col"))
        status = _get_cell_by_layout(df, i, layout.get("status_col")).replace("\n", " ")
        docs = _get_cell_by_layout(df, i, layout.get("related_docs_col")).replace("\n", " ")
        records = _get_cell_by_layout(df, i, layout.get("records_col")).replace("\n", " ")

        title_val = _get_cell_by_layout(df, i, title_col).replace("\n", " ")
        if title_val and _is_section_title(title_val):
            prev_title = title_val

        # field_code/field_name 추론: "1.1 (관리체계 기반 마련)" 같이 한 셀에 합쳐진 경우도 처리
        if field_code_raw:
            code, name = split_code_and_name(field_code_raw, kind="field")
            if code and FIELD_CODE_RE.match(code):
                prev_field_code = code
                if name:
                    prev_field_name = name
        if field_name_raw:
            prev_field_name = field_name_raw

        if item_code_raw and ITEM_CODE_RE.match(item_code_raw):
            prev_item_code = item_code_raw.rstrip(". \t")
        if item_name_raw:
            prev_item_name = item_name_raw

        if _is_title_row_layout(df, i, layout):
            continue

        if not any([detail, oper, cert, status, docs, records]):
            continue

        row = {
            "title": prev_title,
            "분야_code": prev_field_code,
            "분야_name": prev_field_name,
            "항목_code": prev_item_code,
            "항목_name": prev_item_name,
            "상세내용": _clean_text(detail),
            "운영여부": _clean_text(oper),
            "인증구분": _clean_text(cert),
            "운영현황": _clean_text(status),
            "관련문서": _clean_text(docs),
            "기록": _clean_text(records),
        }
        if any(row.values()):
            flat_rows.append(row)

    return {
        "metadata": meta,
        "sections": _group_flat_rows(flat_rows),
        "sheet": sheet_name,
    }


def parse_operating_sheet_standard(df: pd.DataFrame) -> Dict[str, Any]:
    meta = _extract_title_and_org(df)

    def split_code_name(text: str) -> tuple[str, str]:
        value = _cell_str(text)
        if not value:
            return ("", "")
        match = re.match(r"^\s*(\d+(?:\.\d+)*\.?)\s*(.*)\s*$", value)
        if not match:
            return (value, "")
        return (match.group(1).strip(), match.group(2).strip())

    header_row_idx = None
    for i in range(min(30, len(df))):
        row_vals = [_cell_str(df.iloc[i, j]) for j in range(df.shape[1])]
        if (
            any(v == "분야" for v in row_vals)
            and any(v == "항목" for v in row_vals)
            and any(v == "상세내용" for v in row_vals)
        ):
            header_row_idx = i
            break
    if header_row_idx is None:
        return {"metadata": meta, "sections": []}

    headers = [_cell_str(df.iloc[header_row_idx, j]) for j in range(df.shape[1])]

    def idx_of(predicate):
        for j, header in enumerate(headers):
            if predicate(header):
                return j
        return None

    idx_field = idx_of(lambda h: h == "분야")
    idx_field_name = idx_of(lambda h: "분야" in h and ("명" in h or "이름" in h or "설명" in h))
    idx_item = idx_of(lambda h: h == "항목")
    idx_item_name = idx_of(lambda h: "항목" in h and ("명" in h or "이름" in h))
    idx_detail = idx_of(lambda h: h == "상세내용")
    idx_oper = idx_of(lambda h: "운영" in h and "여부" in h)
    idx_cert = idx_of(lambda h: "인증구분" in h)
    idx_status = idx_of(lambda h: "운영현황" in h)
    idx_docs = idx_of(lambda h: "관련문서" in h)
    idx_records = idx_of(lambda h: "기록" in h)
    ncols = df.shape[1]

    if idx_field is not None and idx_field_name is None:
        cand = idx_field + 1
        if cand < len(headers) and headers[cand] == "":
            idx_field_name = cand
    if idx_item is not None and idx_item_name is None:
        cand = idx_item + 1
        if cand < len(headers) and headers[cand] == "":
            idx_item_name = cand
    if idx_field is not None and idx_field_name is None and idx_field + 2 < df.shape[1]:
        idx_field_name = idx_field + 2
    if idx_item is not None and idx_item_name is None and idx_item + 2 < df.shape[1]:
        idx_item_name = idx_item + 2

    flat_rows: List[Dict[str, Any]] = []
    prev_title = ""
    prev_field_code = ""
    prev_field_name = ""
    prev_item_code = ""
    prev_item_name = ""

    for i in range(header_row_idx + 1, len(df)):

        def g(idx):
            return _cell_str(df.iloc[i, idx]) if idx is not None and idx < df.shape[1] else ""

        field_raw = g(idx_field)
        if field_raw and _is_section_title(field_raw):
            prev_title = field_raw.replace("\n", " ").strip()

        if idx_field is not None and _is_title_row_standard(df, i, idx_field, ncols):
            continue
        if field_raw and _is_section_title(field_raw):
            if not g(idx_item) and not g(idx_detail) and not g(idx_status) and not g(idx_docs) and not g(idx_records):
                continue

        if field_raw:
            code, name = split_code_name(field_raw)
            prev_field_code = code
            prev_field_name = name or prev_field_name

        field_name_raw = g(idx_field_name)
        if field_name_raw:
            prev_field_name = field_name_raw

        item_raw = g(idx_item)
        if item_raw:
            code, name = split_code_name(item_raw)
            prev_item_code = code
            prev_item_name = name or prev_item_name

        item_name_raw = g(idx_item_name)
        if item_name_raw:
            prev_item_name = item_name_raw

        row = {
            "title": prev_title,
            "분야_code": prev_field_code,
            "분야_name": prev_field_name,
            "항목_code": prev_item_code,
            "항목_name": prev_item_name,
            "상세내용": _clean_text(g(idx_detail)),
            "운영여부": _clean_text(g(idx_oper)),
            "인증구분": _clean_text(g(idx_cert)),
            "운영현황": _clean_text(g(idx_status)),
            "관련문서": _clean_text(g(idx_docs)),
            "기록": _clean_text(g(idx_records)),
        }
        if not any(row.values()):
            continue
        if row["분야_code"] and not row["항목_code"] and not row["상세내용"] and not row["운영여부"]:
            continue
        flat_rows.append(row)

    return {"metadata": meta, "sections": _group_flat_rows(flat_rows)}


def _is_title_row_standard(df: pd.DataFrame, row_idx: int, field_col_idx: int, ncols: int) -> bool:
    value = _cell_str(df.iloc[row_idx, field_col_idx]) if field_col_idx < df.shape[1] else ""
    if not _is_section_title(value):
        return False
    for j in range(min(ncols, df.shape[1])):
        if j == field_col_idx:
            continue
        if _cell_str(df.iloc[row_idx, j]):
            return False
    return True


def parse_operating_sheet_item_only(df: pd.DataFrame) -> Dict[str, Any]:
    meta = _extract_title_and_org(df)

    header_row_idx = None
    for i in range(min(30, len(df))):
        row_vals = [_normalize_header_text(df.iloc[i, j]) for j in range(df.shape[1])]
        has_item = any(v == "항목" for v in row_vals)
        has_detail = any(v == "상세내용" for v in row_vals)
        has_field = any(v == "분야" for v in row_vals)
        if has_item and has_detail and not has_field:
            header_row_idx = i
            break
    if header_row_idx is None:
        return {"metadata": meta, "sections": []}

    headers = [_normalize_header_text(df.iloc[header_row_idx, j]) for j in range(df.shape[1])]

    def idx_of(predicate):
        for j, header in enumerate(headers):
            if predicate(header):
                return j
        return None

    idx_item = idx_of(lambda h: h == "항목")
    idx_detail = idx_of(lambda h: h == "상세내용")
    idx_oper = idx_of(lambda h: "운영" in h and "여부" in h)
    idx_cert = idx_of(lambda h: h == "인증구분")
    idx_status = idx_of(lambda h: h == "운영현황")
    idx_docs = idx_of(lambda h: h == "관련문서")
    idx_records = idx_of(lambda h: h == "기록")

    idx_item_name = None
    if idx_item is not None and idx_item + 1 < df.shape[1]:
        next_header = headers[idx_item + 1]
        known_headers = {
            "항목",
            "상세내용",
            "운영여부",
            "인증구분",
            "운영현황",
            "관련문서",
            "기록",
        }
        if next_header not in known_headers:
            idx_item_name = idx_item + 1

    def g(i: int, idx: int | None) -> str:
        return _cell_str(df.iloc[i, idx]) if idx is not None and idx < df.shape[1] else ""

    def split_compact(text: str) -> tuple[str, str]:
        value = _cell_str(text).replace("\n", " ").strip()
        if not value:
            return ("", "")
        match = re.match(r"^\s*(\d+(?:\.\d+)*\.?)\s*(.*)\s*$", value)
        if not match:
            return ("", value)
        return (match.group(1).strip(), match.group(2).strip().strip("()").strip())

    flat_rows: List[Dict[str, Any]] = []
    prev_title = ""
    prev_field_code = ""
    prev_field_name = ""
    prev_item_code = ""
    prev_item_name = ""

    for i in range(header_row_idx + 1, len(df)):
        compact = g(i, idx_item)
        item_name_raw = g(i, idx_item_name)
        detail = _clean_text(g(i, idx_detail))
        oper = _clean_text(g(i, idx_oper))
        cert = _clean_text(g(i, idx_cert))
        status = _clean_text(g(i, idx_status))
        docs = _clean_text(g(i, idx_docs))
        records = _clean_text(g(i, idx_records))

        if not any([compact, item_name_raw, detail, oper, cert, status, docs, records]):
            continue

        code, inline_name = split_compact(compact)
        norm_code = code.rstrip(".")
        segments = [part for part in norm_code.split(".") if part]

        if norm_code and len(segments) == 1 and inline_name:
            prev_title = inline_name
            continue
        if norm_code and len(segments) == 2 and inline_name:
            prev_field_code = code if code.endswith(".") else f"{code}."
            prev_field_name = inline_name
            continue
        if norm_code and len(segments) >= 3:
            prev_item_code = norm_code
            if item_name_raw:
                prev_item_name = item_name_raw
            elif inline_name:
                prev_item_name = inline_name
        elif item_name_raw:
            prev_item_name = item_name_raw

        row = {
            "title": prev_title,
            "분야_code": prev_field_code,
            "분야_name": prev_field_name,
            "항목_code": prev_item_code,
            "항목_name": prev_item_name,
            "상세내용": detail,
            "운영여부": oper,
            "인증구분": cert,
            "운영현황": status,
            "관련문서": docs,
            "기록": records,
        }
        if not any(row.values()):
            continue
        if row["분야_code"] and not row["항목_code"] and not row["상세내용"] and not row["운영여부"]:
            continue
        flat_rows.append(row)

    return {"metadata": meta, "sections": _group_flat_rows(flat_rows)}


def parse_operating_sheet_compact(df: pd.DataFrame) -> Dict[str, Any]:
    meta = _extract_title_and_org(df)

    header_row_idx = None
    for i in range(min(40, len(df))):
        row_vals = [_cell_str(df.iloc[i, j]) for j in range(df.shape[1])]
        if any(("분야" in v and "항목" in v) for v in row_vals) and any(v == "상세내용" for v in row_vals):
            header_row_idx = i
            break
    if header_row_idx is None:
        return {"metadata": meta, "sections": []}

    headers = [_cell_str(df.iloc[header_row_idx, j]) for j in range(df.shape[1])]

    def idx_of(predicate):
        for j, header in enumerate(headers):
            if predicate(header):
                return j
        return None

    idx_field_item = idx_of(lambda h: "분야" in h and "항목" in h)
    idx_detail = idx_of(lambda h: h == "상세내용")
    idx_oper = idx_of(lambda h: "운영" in h and "여부" in h)
    idx_cert = idx_of(lambda h: "인증" in h and "구분" in h)
    idx_status = idx_of(lambda h: "운영현황" in h)
    idx_docs = idx_of(lambda h: "관련문서" in h)
    idx_records = idx_of(lambda h: "기록" in h)

    def g(i: int, idx: int | None) -> str:
        return _cell_str(df.iloc[i, idx]) if idx is not None and idx < df.shape[1] else ""

    def split_compact(value: str) -> tuple[str, str]:
        text = _cell_str(value).replace("\n", " ").strip()
        if not text:
            return ("", "")
        match = re.match(r"^\s*(\d+(?:\.\d+)*\.?)\s*[\.)]?\s*(.*)\s*$", text)
        if not match:
            return ("", text)
        return (match.group(1).strip().rstrip("."), match.group(2).strip())

    flat_rows: List[Dict[str, Any]] = []
    prev_title = ""
    prev_field_code = ""
    prev_field_name = ""
    prev_item_code = ""
    prev_item_name = ""

    for i in range(header_row_idx + 1, len(df)):
        compact = g(i, idx_field_item)
        detail = _clean_text(g(i, idx_detail))
        oper = _clean_text(g(i, idx_oper))
        cert = _clean_text(g(i, idx_cert))
        status = _clean_text(g(i, idx_status))
        docs = _clean_text(g(i, idx_docs))
        records = _clean_text(g(i, idx_records))

        if not any([compact, detail, oper, cert, status, docs, records]):
            continue
        if compact and _is_section_title(compact) and not any([detail, status, docs, records]):
            prev_title = compact.replace("\n", " ").strip()
            continue

        code, name = split_compact(compact)
        dots = code.count(".") if code else 0

        if code and dots == 1 and name:
            prev_field_code = code + "."
            prev_field_name = name
            continue
        if code and dots >= 2 and name:
            prev_item_code = code
            prev_item_name = name

        row = {
            "title": prev_title,
            "분야_code": prev_field_code,
            "분야_name": prev_field_name,
            "항목_code": prev_item_code,
            "항목_name": prev_item_name,
            "상세내용": detail,
            "운영여부": oper,
            "인증구분": cert,
            "운영현황": status,
            "관련문서": docs,
            "기록": records,
        }
        if any(row.values()):
            flat_rows.append(row)

    return {"metadata": meta, "sections": _group_flat_rows(flat_rows)}


def parse_instruction_sheet(df: pd.DataFrame) -> Dict[str, Any]:
    meta_title = ""
    if len(df) > 1 and df.shape[1] > 1:
        meta_title = _cell_str(df.iloc[1, 1])

    header_row_idx = None
    for i in range(min(20, len(df))):
        c1 = _cell_str(df.iloc[i, 1]) if df.shape[1] > 1 else ""
        c3 = _cell_str(df.iloc[i, 3]) if df.shape[1] > 3 else ""
        if c1 == "구분" and c3 == "상세내용":
            header_row_idx = i
            break
    if header_row_idx is None:
        return {"metadata": {"document_title": meta_title}, "rows": []}

    rows: List[Dict[str, str]] = []
    for i in range(header_row_idx + 1, len(df)):
        c1 = _cell_str(df.iloc[i, 1]) if df.shape[1] > 1 else ""
        c3 = _cell_str(df.iloc[i, 3]) if df.shape[1] > 3 else ""
        if not c1 and not c3:
            continue
        rows.append({"구분": _clean_text(c1), "상세내용": _clean_text(c3)})
    return {"metadata": {"document_title": meta_title}, "rows": rows}


def _is_usable_layout(layout: Dict[str, Any]) -> bool:
    if not layout:
        return False
    required = ["item_code_col", "detail_col"]
    return all(layout.get(k) is not None for k in required)


def sheet_type(df: pd.DataFrame, name: str) -> str:
    del name

    for i in range(min(20, len(df))):
        c1 = _cell_str(df.iloc[i, 1]) if df.shape[1] > 1 else ""
        c3 = _cell_str(df.iloc[i, 3]) if df.shape[1] > 3 else ""
        if c1 == "구분" and c3 == "상세내용":
            return "instruction"

    layout = _infer_layout(df)
    if _is_usable_layout(layout):
        return "operating"

    for i in range(min(40, len(df))):
        row_vals = [_cell_str(df.iloc[i, j]) for j in range(df.shape[1])]
        if any(("분야" in v and "항목" in v) for v in row_vals) and any(v == "상세내용" for v in row_vals):
            return "operating_compact"

    for i in range(min(30, len(df))):
        row_vals = [_normalize_header_text(df.iloc[i, j]) for j in range(df.shape[1])]
        has_item = any(v == "항목" for v in row_vals)
        has_detail = any(v == "상세내용" for v in row_vals)
        has_field = any(v == "분야" for v in row_vals)
        if not (has_item and has_detail) or has_field:
            continue

        for r in range(i + 1, min(i + 10, len(df))):
            for j in range(max(0, df.shape[1] - 1)):
                code = _cell_str(df.iloc[r, j]).rstrip(". ")
                name_next = _cell_str(df.iloc[r, j + 1]) if j + 1 < df.shape[1] else ""
                if re.match(r"^\d+\.\d+\.\d+$", code) and name_next:
                    return "operating_item_only"

    return "raw"


def _parse_operating_sheet_by_type(
    df: pd.DataFrame,
    stype: str,
    *,
    sheet_name: str,
    fallback_company: str,
) -> Dict[str, Any]:
    parsed = parse_operating_sheet_with_layout_inference(
        df,
        sheet_name=sheet_name,
        fallback_company=fallback_company,
    )
    if _count_items(parsed) > 0:
        parsed["sheet_type"] = "operating"
        return parsed

    if stype == "operating":
        fallback = parse_operating_sheet_standard(df)
    elif stype == "operating_item_only":
        fallback = parse_operating_sheet_item_only(df)
    elif stype == "operating_compact":
        fallback = parse_operating_sheet_compact(df)
    else:
        fallback = {"metadata": {}, "sections": []}

    merged_meta = dict(parsed.get("metadata", {}))
    merged_meta.update({k: v for k, v in fallback.get("metadata", {}).items() if v})
    fallback["metadata"] = merged_meta
    fallback["sheet_type"] = "operating"
    return fallback


def build_retrieval_rows(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    sheets = result.get("sheets", {})

    for sheet_name, sheet in sheets.items():
        if not isinstance(sheet, dict):
            continue
        if sheet.get("sheet_type") != "operating":
            continue

        meta = sheet.get("metadata", {})
        company = _clean_text(meta.get("기업명", ""))
        doc_title = _clean_text(meta.get("document_title", ""))

        for section in sheet.get("sections", []):
            section_title = _clean_text(section.get("title", ""))
            for field in section.get("fields", []):
                field_code = _normalize_code(field.get("분야_code", ""))
                field_name = _clean_text(field.get("분야_name", ""))

                for item in field.get("items", []):
                    item_code = _clean_text(item.get("항목_code", ""))
                    item_name = _clean_text(item.get("항목_name", ""))
                    detail = _clean_text(item.get("상세내용", ""))
                    oper = _clean_text(item.get("운영여부", ""))
                    cert = _clean_text(item.get("인증구분", ""))
                    status = _clean_text(item.get("운영현황", ""))
                    docs = _clean_text(item.get("관련문서", ""))
                    records = _clean_text(item.get("기록", ""))

                    if not any(
                        [
                            item_code,
                            item_name,
                            detail,
                            oper,
                            cert,
                            status,
                            docs,
                            records,
                        ]
                    ):
                        continue

                    text = "\n".join(
                        [
                            _join_nonempty([company, doc_title, sheet_name, section_title]),
                            _join_nonempty([field_code, field_name], sep=" "),
                            _join_nonempty([item_code, item_name], sep=" "),
                            _join_nonempty(
                                [
                                    f"운영여부: {oper}" if oper else "",
                                    f"인증구분: {cert}" if cert else "",
                                ],
                                sep=" | ",
                            ),
                            f"상세내용: {detail}" if detail else "",
                            f"운영현황: {status}" if status else "",
                            f"관련문서: {docs}" if docs else "",
                            f"기록: {records}" if records else "",
                        ]
                    ).strip()

                    rows.append(
                        {
                            "kind": "item",
                            "sheet": sheet_name,
                            "company": company,
                            "doc_title": doc_title,
                            "section_title": section_title,
                            "분야_code": field_code,
                            "분야_name": field_name,
                            "항목_code": item_code,
                            "항목_name": item_name,
                            "운영여부": oper,
                            "인증구분": cert,
                            "상세내용": detail,
                            "운영현황": status,
                            "관련문서": docs,
                            "기록": records,
                            "text": text,
                        }
                    )

    return dedupe_retrieval_rows(rows)


def dedupe_retrieval_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []

    for row in rows:
        key = (
            row.get("kind", ""),
            row.get("sheet", ""),
            row.get("section_title", ""),
            row.get("분야_code", ""),
            row.get("항목_code", ""),
            row.get("text", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)

    return out


def excel_to_structured(path: str | Path) -> Dict[str, Any]:
    path = Path(path)
    xl = read_workbook(path)
    result: Dict[str, Any] = {
        "source_file": str(path.name),
        "sheets": {},
    }
    fallback_company = _extract_company_from_filename(path)
    fallback_title = path.stem
    candidates = []
    excluded_sheet_names = ("작성요령", "표지", "개정이력", "Sheet4")

    for sh_name in xl.sheet_names:
        if any(excluded in _cell_str(sh_name) for excluded in excluded_sheet_names):
            continue

        df = pd.read_excel(xl, sheet_name=sh_name, header=None)
        meta = _extract_title_and_org(df)
        stype = sheet_type(df, sh_name)
        candidates.append((sh_name, df, meta, stype))

    for sh_name, df, meta, stype in candidates:
        if stype == "instruction":
            should_include = True
        else:
            should_include = _sheet_qualifies_anchor_min_op_status(df)

        if not should_include:
            continue

        if stype in {"operating", "operating_item_only", "operating_compact"}:
            parsed = _parse_operating_sheet_by_type(
                df,
                stype,
                sheet_name=sh_name,
                fallback_company=fallback_company,
            )
        elif stype == "instruction":
            parsed = parse_instruction_sheet(df)
            parsed["sheet_type"] = "instruction"
        else:
            parsed = _parse_operating_sheet_by_type(
                df,
                "operating",
                sheet_name=sh_name,
                fallback_company=fallback_company,
            )

        result["sheets"][sh_name] = _apply_metadata_fallbacks(
            parsed,
            sheet_meta=meta,
            fallback_company=fallback_company,
            fallback_title=fallback_title,
        )

    if not result["sheets"]:
        result["sheets"][SHEET_NAME] = {
            "metadata": {},
            "sheet_type": "missing",
            "error": "운영명세서 형식의 시트를 찾지 못했습니다.",
        }

    result["retrieval_rows"] = build_retrieval_rows(result)
    for sheet in result["sheets"].values():
        if isinstance(sheet, dict):
            sheet.pop("sections", None)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="공통 운영명세서 엑셀(모든 시트) → JSON/JSONL 변환",
    )
    parser.add_argument(
        "input",
        nargs="*",
        default=None,
        help="입력 엑셀 파일 경로 (.xlsx). 미지정 시 기본 경로 사용",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="출력 JSON 파일 경로 (미지정 시 기본 경로 또는 stdout)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "jsonl"],
        default="json",
        help="출력 형식: json(단일 객체), jsonl(한 줄에 한 레코드)",
    )
    parser.add_argument(
        "--no-summary-doc",
        action="store_true",
        help="summary_doc를 포함하지 않습니다. (기본값: 포함, json 출력에서만 적용)",
    )
    args = parser.parse_args()

    if not args.input:
        args.input = [DEFAULT_INPUT_PATH]
        if args.output is None:
            args.output = str(Path(DEFAULT_OUTPUT_DIR) / f"{Path(DEFAULT_INPUT_PATH).stem}.json")

    path = Path(args.input[0])
    if not path.is_file():
        print(f"오류: 파일을 찾을 수 없습니다. {path}", file=sys.stderr)
        sys.exit(1)

    try:
        data = excel_to_structured(path)
    except Exception as exc:
        print(f"오류: 변환 실패 - {exc}", file=sys.stderr)
        sys.exit(1)

    data_for_output = dict(data) if isinstance(data, dict) else data
    if isinstance(data_for_output, dict):
        data_for_output.pop("source_file", None)

    if args.format == "jsonl":
        text = json.dumps(data_for_output, ensure_ascii=False)
    else:
        if args.no_summary_doc:
            payload = data_for_output
        else:
            from summary_doc import build_summary_doc

            summary_doc = build_summary_doc(
                data,
                template=SUMMARY_TEMPLATE,
                source_file=str(path.name),
            )
            payload = {
                "template": SUMMARY_TEMPLATE,
                "summary_doc": summary_doc if isinstance(summary_doc, dict) else {},
                "data": data_for_output,
            }
        text = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        print(
            f"저장됨: {out_path} (시트 {len(data.get('sheets', {}))}개, retrieval_rows {len(data.get('retrieval_rows', []))}건)",
            file=sys.stderr,
        )
    else:
        print(text)


if __name__ == "__main__":
    main()
