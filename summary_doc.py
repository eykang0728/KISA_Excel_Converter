from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Optional, Tuple


def _safe_str(x: Any) -> str:
    return "" if x is None else str(x).strip()


def _safe_int_year(x: Any) -> Optional[int]:
    try:
        y = int(str(x).strip())
        return y if 1900 <= y <= 2100 else None
    except Exception:
        return None


def _extract_year_and_period(metadata: Dict[str, Any]) -> Tuple[Optional[int], str]:
    period = _safe_str(metadata.get("심사기간") or metadata.get("기간") or metadata.get("작성일") or "")
    m = re.search(r"(20[0-3]\d)", period)
    year = _safe_int_year(m.group(1)) if m else None
    return year, period


def _iter_dict_scalars_no_lists(obj: Any) -> Iterable[Tuple[str, Any]]:
    """
    Yield (key, value) for scalar leaves under dicts, but do not descend into lists.
    Used to extract hints from templates like KAIT01 where metadata may be empty,
    but values live under sheets.*.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list):
                continue
            if isinstance(v, dict):
                yield from _iter_dict_scalars_no_lists(v)
            else:
                yield str(k), v


def _find_first_value(obj: Any, keys: Tuple[str, ...]) -> str:
    want = set(keys)
    for k, v in _iter_dict_scalars_no_lists(obj):
        if k in want:
            s = _safe_str(v)
            if s:
                return s
    return ""


def _extract_company(metadata: Dict[str, Any]) -> str:
    # common variants across templates
    for k in ("기업명", "회사명", "신청기관명", "신청기관", "신청기관명/회사명", "기관명"):
        v = _safe_str(metadata.get(k))
        if v:
            return v
    return ""


def _extract_doc_type(metadata: Dict[str, Any], fallback: str = "") -> str:
    for k in ("document_title", "문서명", "유형", "문서 제목", "제목"):
        v = _safe_str(metadata.get(k))
        if v:
            return v
    return fallback


def _extract_company_from_source_file(source_file: str) -> str:
    s = _safe_str(source_file)
    if not s:
        return ""

    # [회사명] 패턴
    m = re.search(r"\[([^\[\]]+)\]", s)
    if m:
        return m.group(1).strip()

    return ""


def build_summary_doc(
    converted: Any,
    *,
    template: str = "",
    source_file: str = "",
) -> Dict[str, Any]:
    """
    Build minimal summary_doc only.
    Keeps ONLY: template, source_file, company, doc_type, year_hint.
    """
    resolved_source_file = source_file or _safe_str(converted.get("source_file"))
    if not isinstance(converted, dict):
        converted_dict = {"value": converted}
    else:
        converted_dict = converted

    md = converted_dict.get("metadata") if isinstance(converted_dict.get("metadata"), dict) else {}
    year_hint, period_hint = _extract_year_and_period(md)
    company = (
        _extract_company(md)
        or _find_first_value(
            converted_dict,
            ("기업명", "회사명", "신청기관명", "신청기관", "기관명"),
        )
        or _extract_company_from_source_file(resolved_source_file)
    )
    doc_type = _extract_doc_type(md, fallback=_safe_str(md.get("document_title") or "")) or _find_first_value(
        converted_dict,
        ("document_title", "문서명", "유형", "문서 제목", "제목"),
    )

    period_fallback = _find_first_value(converted_dict, ("심사기간", "기간", "작성일"))
    if period_fallback:
        period_hint = period_fallback
    if year_hint is None and period_hint:
        m = re.search(r"(20[0-3]\d)", period_hint)
        if m:
            year_hint = _safe_int_year(m.group(1))

    year_hint_out: Any = period_hint or year_hint
    return {
        "template": template,
        "source_file": resolved_source_file,
        "company": company,
        "doc_type": doc_type,
        "year_hint": year_hint_out,
    }