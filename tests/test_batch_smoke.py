from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _first_excel_under(root: Path) -> Path:
    exc = sorted([p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in {".xls", ".xlsx", ".xlsm"}])
    assert exc, f"no excel files under: {root}"
    return exc[0]


def test_step1_process_batch_smoke(tmp_path: Path) -> None:
    from batch_runner.step1_2024_2025_batch_runner import process_batch

    repo = _repo_root()
    input_root = repo / "template" / "step1_2024-2025_small"
    assert input_root.is_dir(), f"missing sample dir: {input_root}"

    # Quick sanity: at least one excel exists
    _first_excel_under(input_root)

    output_root = tmp_path / "step1-out"
    success_count, failures = process_batch(
        input_root=input_root,
        output_root=output_root,
        output_format="json",
        include_summary_doc=True,
        continue_on_error=False,
        use_common_converter=False,
    )
    assert failures == []
    assert success_count > 0


def test_step2_process_batch_smoke(tmp_path: Path) -> None:
    from batch_runner.step2_2024_2025_batch_runner import process_batch

    repo = _repo_root()
    input_root = repo / "template" / "step2_2024-2025_small"
    assert input_root.is_dir(), f"missing sample dir: {input_root}"

    _first_excel_under(input_root)

    output_root = tmp_path / "step2-out"
    success_count, failures = process_batch(
        input_root=input_root,
        output_root=output_root,
        output_format="json",
        include_summary_doc=True,
        continue_on_error=False,
    )
    assert failures == []
    assert success_count > 0
