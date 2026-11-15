"""Data-directory relocation (RAIN_DATA_DIR move) behavior.

A deployment move copies the whole data directory (SQLite + Parquet tree) and
restarts with a new ``RAIN_DATA_DIR``. The database still stores the old
absolute paths, so startup must verify the move and rewrite metadata *before*
the orphan-file sweep runs. These tests exercise the service layer directly
with isolated settings/databases so the global test settings are untouched.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app import db
from app.config import Settings
from app.errors import DataMigrationError
from app.storage import FileManager
from tests.conftest import make_csv


def _make_settings(root: Path) -> Settings:
    root = root.resolve()
    return Settings(
        data_dir=root,
        db_path=root / "metadata.sqlite3",
        parquet_root=root / "parquet",
        tmp_dir=root / "tmp",
        reaper_interval=3600.0,
        max_upload_bytes=50 * 1024 * 1024,
    )


def _seed(root: Path) -> tuple[Settings, FileManager]:
    """Create a populated data root: published batches, a reaped merge, orphans."""
    cfg = _make_settings(root)
    fm = FileManager(cfg)
    original = db.settings
    db.settings = cfg
    try:
        db.init_db()
        fm.startup_sweep()
        from app import services

        for i in range(3):
            services.upload_csv(
                "ST001",
                "2026-11-12",
                make_csv(
                    [(f"2026-11-12T0{i}:00Z", float(i))], "ST001"
                ),
                fm=fm,
            )
        # A merge followed by reaping produces historical reaped rows whose
        # files no longer exist; they must not block relocation.
        services.merge_station_day("ST001", "2026-11-12", fm=fm)
        fm.reap_once()
        services.upload_csv(
            "ST002",
            "2026-11-12",
            make_csv([("2026-11-12T00:00Z", 1.0)], "ST002"),
            fm=fm,
        )
    finally:
        db.settings = original
    return cfg, fm


def _copy_tree(src: Path, dst: Path) -> None:
    import shutil

    shutil.copytree(src, dst)


def _registered_paths(cfg: Settings) -> list[str]:
    original = db.settings
    db.settings = cfg
    try:
        conn = db.connect()
        try:
            return [r["file_path"] for r in conn.execute("SELECT file_path FROM batches")]
        finally:
            conn.close()
    finally:
        db.settings = original


def test_complete_relocation_rewrites_paths_and_keeps_everything_queryable(
    tmp_path,
):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    old_cfg, _ = _seed(old_root)
    old_paths = _registered_paths(old_cfg)
    assert all(str(old_root) in p for p in old_paths)

    _copy_tree(old_root, new_root)
    new_cfg = _make_settings(new_root)
    new_fm = FileManager(new_cfg)

    original = db.settings
    db.settings = new_cfg
    try:
        report = new_fm.startup_sweep()
    finally:
        db.settings = original

    # All five registered rows (3 reaped sources + 1 merged + 1 ST002) moved.
    assert len(report["relocated"]) == 5
    assert report["orphan_parquet"] == []
    new_paths = _registered_paths(new_cfg)
    assert all(str(new_root) in p for p in new_paths)

    # Every physical file survived and queries/merge still work.
    parquet_files = list(new_cfg.parquet_root.rglob("*.parquet"))
    assert len(parquet_files) == 2  # the merged ST001 file + ST002 batch

    from app import services

    original = db.settings
    db.settings = new_cfg
    try:
        q1 = services.query_station_day("ST001", "2026-11-12", fm=new_fm)
        assert q1["row_count"] == 3 and q1["file_count"] == 1
        q2 = services.query_station_day("ST002", "2026-11-12", fm=new_fm)
        assert q2["row_count"] == 1
        # Merge capability preserved after relocation.
        merged = services.merge_station_day("ST002", "2026-11-12", fm=new_fm)
        assert merged["row_count"] == 1
    finally:
        db.settings = original


def test_relocation_is_idempotent_across_repeated_restarts(tmp_path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _seed(old_root)
    _copy_tree(old_root, new_root)
    new_cfg = _make_settings(new_root)

    original = db.settings
    db.settings = new_cfg
    try:
        fm = FileManager(new_cfg)
        first = fm.startup_sweep()
        assert len(first["relocated"]) == 5
        # A restart after the successful move has nothing to do.
        second = FileManager(new_cfg).startup_sweep()
        assert second["relocated"] == []
        assert second["orphan_parquet"] == []
        # And yet another restart remains stable.
        third = FileManager(new_cfg).startup_sweep()
        assert third["relocated"] == []
    finally:
        db.settings = original


def test_incomplete_relocation_raises_and_preserves_all_state(tmp_path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    old_cfg, _ = _seed(old_root)
    before_paths = _registered_paths(old_cfg)
    _copy_tree(old_root, new_root)

    # Drop the one still-published ST002 file: the move is now incomplete.
    st002_files = list((new_root / "parquet" / "ST002").rglob("*.parquet"))
    assert len(st002_files) == 1
    st002_files[0].unlink()

    new_cfg = _make_settings(new_root)
    fm = FileManager(new_cfg)
    files_before = sorted(p.name for p in new_cfg.parquet_root.rglob("*.parquet"))
    original = db.settings
    db.settings = new_cfg
    try:
        with pytest.raises(DataMigrationError) as exc:
            fm.startup_sweep()
        assert "missing" in str(exc.value)
    finally:
        db.settings = original

    # No metadata rewrite and no file deletion happened.
    assert _registered_paths(new_cfg) == before_paths
    files_after = sorted(p.name for p in new_cfg.parquet_root.rglob("*.parquet"))
    assert files_after == files_before


def test_size_mismatch_is_treated_as_incomplete(tmp_path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _seed(old_root)
    _copy_tree(old_root, new_root)

    target = next((new_root / "parquet" / "ST002").rglob("*.parquet"))
    target.write_bytes(target.read_bytes()[:20])  # truncated / partial copy

    new_cfg = _make_settings(new_root)
    fm = FileManager(new_cfg)
    original = db.settings
    db.settings = new_cfg
    try:
        with pytest.raises(DataMigrationError) as exc:
            fm.relocate_registered_files()
        assert "size-mismatched" in str(exc.value)
        # Nothing was rewritten.
        conn = db.connect()
        try:
            rows = conn.execute("SELECT file_path FROM batches").fetchall()
        finally:
            conn.close()
    finally:
        db.settings = original
    assert all(str(old_root) in r["file_path"] for r in rows)


def test_reaped_historical_rows_do_not_block_relocation(tmp_path):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _seed(old_root)
    # Remove ALL reaped files at the new location (they are normally already
    # gone); the move must still succeed because reaped rows need no file.
    _copy_tree(old_root, new_root)
    new_cfg = _make_settings(new_root)

    original = db.settings
    db.settings = new_cfg
    try:
        conn = db.connect()
        try:
            reaped_paths = [
                Path(r["file_path"])
                for r in conn.execute(
                    "SELECT file_path FROM batches WHERE reaped = 1"
                ).fetchall()
            ]
        finally:
            conn.close()
    finally:
        db.settings = original
    assert reaped_paths, "test setup must include reaped rows"

    fm = FileManager(new_cfg)
    original = db.settings
    db.settings = new_cfg
    try:
        report = fm.startup_sweep()
        conn = db.connect()
        try:
            reaped = conn.execute(
                "SELECT COUNT(*) c FROM batches WHERE reaped = 1"
            ).fetchone()["c"]
            under_new = conn.execute(
                "SELECT COUNT(*) c FROM batches WHERE file_path LIKE ?",
                (f"{new_root}%",),
            ).fetchone()["c"]
            total = conn.execute("SELECT COUNT(*) c FROM batches").fetchone()["c"]
        finally:
            conn.close()
    finally:
        db.settings = original

    assert reaped == 3
    assert under_new == total  # reaped rows were rewritten too
    assert len(report["relocated"]) == total


def test_genuine_unregistered_orphan_is_still_cleaned_after_relocation(tmp_path):
    import pyarrow.parquet as pq

    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _seed(old_root)
    _copy_tree(old_root, new_root)
    new_cfg = _make_settings(new_root)
    fm = FileManager(new_cfg)

    original = db.settings
    db.settings = new_cfg
    try:
        fm.startup_sweep()  # relocate first
        # A parquet with no metadata row is a real crash leftover.
        orphan = fm.station_day_dir("ST002", "2026-11-12") / "CRASHED.parquet"
        orphan.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(fm.empty_table(), orphan)
        report = fm.startup_sweep()
    finally:
        db.settings = original

    assert not orphan.exists()
    assert any(p.endswith("CRASHED.parquet") for p in report["orphan_parquet"])


def test_relocation_then_restart_converges_after_interrupted_move(tmp_path):
    """Completing the file move later lets the next startup finish cleanly."""
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _seed(old_root)
    _copy_tree(old_root, new_root)

    missing = next((new_root / "parquet" / "ST002").rglob("*.parquet"))
    missing.unlink()

    new_cfg = _make_settings(new_root)
    original = db.settings
    db.settings = new_cfg
    try:
        # First boot: incomplete move, startup refuses.
        with pytest.raises(DataMigrationError):
            FileManager(new_cfg).startup_sweep()

        # Engineer finishes copying the missing file.
        import shutil

        src = next((old_root / "parquet" / "ST002").rglob("*.parquet"))
        shutil.copy2(src, missing)

        # Next boot completes the relocation; queries work immediately.
        fm = FileManager(new_cfg)
        report = fm.startup_sweep()
        assert len(report["relocated"]) == 5

        from app import services

        q = services.query_station_day("ST002", "2026-11-12", fm=fm)
        assert q["row_count"] == 1
    finally:
        db.settings = original


def test_lifespan_aborts_startup_on_incomplete_relocation(tmp_path):
    """The real restart path: FastAPI lifespan must refuse to serve."""
    from fastapi.testclient import TestClient

    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    _seed(old_root)
    _copy_tree(old_root, new_root)
    next((new_root / "parquet" / "ST002").rglob("*.parquet")).unlink()

    new_cfg = _make_settings(new_root)
    # Import lazily and rebind the globals the lifespan reads.
    from app import main as main_module
    from app.main import app
    from app.storage import file_manager as global_fm

    original_settings = main_module.settings
    original_fm = main_module.file_manager
    original_db_settings = db.settings
    isolated_fm = FileManager(new_cfg)
    main_module.settings = new_cfg
    main_module.file_manager = isolated_fm
    db.settings = new_cfg
    try:
        with pytest.raises(DataMigrationError):
            with TestClient(app):
                pass  # entering the context runs the lifespan startup
    finally:
        main_module.settings = original_settings
        main_module.file_manager = original_fm
        db.settings = original_db_settings
    # The global reaper was never started because startup aborted.
    assert isolated_fm._reaper_thread is None

