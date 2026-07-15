from pipeline.demo_small_files import run_demo


def test_compaction_detects_reduces_and_preserves_rows(tmp_path):
    result = run_demo(tmp_path / "small-files", files=5, rows_per_file=10)
    assert result["detected"]
    assert result["files_before"] == 5
    assert result["files_after"] == 1
    assert result["rows_before"] == result["rows_after"] == 50

