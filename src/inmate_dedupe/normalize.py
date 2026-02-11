from __future__ import annotations

import re

import pandas as pd

from inmate_dedupe.config import AppConfig


NON_ALNUM = re.compile(r"[^A-Za-z0-9 ]+")
MULTISPACE = re.compile(r"\s+")


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    text = NON_ALNUM.sub(" ", text)
    text = MULTISPACE.sub(" ", text).strip()
    return text or None


def _digits_only(value: object) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\D+", "", str(value))
    return text or None


def _coalesce_text(parts: list[pd.Series]) -> pd.Series:
    if not parts:
        return pd.Series(dtype=object)
    out = parts[0].fillna("")
    for part in parts[1:]:
        out = (out + " " + part.fillna("")).str.strip()
    out = out.str.replace(MULTISPACE, " ", regex=True).str.strip()
    out = out.mask(out == "", None)
    return out


def soundex(value: object) -> str | None:
    if value is None:
        return None
    text = re.sub(r"[^A-Z]", "", str(value).upper())
    if not text:
        return None
    first = text[0]
    groups = {
        ("B", "F", "P", "V"): "1",
        ("C", "G", "J", "K", "Q", "S", "X", "Z"): "2",
        ("D", "T"): "3",
        ("L",): "4",
        ("M", "N"): "5",
        ("R",): "6",
    }
    mapping = {ch: code for letters, code in groups.items() for ch in letters}
    encoded = []
    prev = mapping.get(first, "")
    for ch in text[1:]:
        code = mapping.get(ch, "")
        if code != prev:
            encoded.append(code)
        prev = code
    payload = "".join([c for c in encoded if c])[:3]
    return (first + payload + "000")[:4]


def normalize_source_frame(raw: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    c = cfg.columns
    id_col = cfg.source_mysql.source_id_column
    updated_col = cfg.source_mysql.source_updated_at_column

    frame = pd.DataFrame()
    frame["record_id"] = raw[id_col].map(lambda v: str(v).strip() if v is not None else None)
    frame["record_id"] = frame["record_id"].mask(frame["record_id"] == "", None)
    frame["source_updated_at"] = pd.to_datetime(raw[updated_col], errors="coerce") if updated_col else pd.NaT

    frame["id_upt"] = raw[c.id_upt].map(_clean_text)
    frame["nik"] = raw[c.nik].map(_digits_only)
    frame["nomor_induk_nasional"] = raw[c.nomor_induk_nasional].map(_digits_only)
    frame["nama_lengkap"] = raw[c.nama_lengkap].map(_clean_text)

    alias1 = raw[c.nama_alias1].map(_clean_text)
    alias2 = raw[c.nama_alias2].map(_clean_text)
    alias3 = raw[c.nama_alias3].map(_clean_text)
    frame["alias_names"] = _coalesce_text([alias1, alias2, alias3])

    kecil1 = raw[c.nama_kecil1].map(_clean_text)
    kecil2 = raw[c.nama_kecil2].map(_clean_text)
    kecil3 = raw[c.nama_kecil3].map(_clean_text)
    frame["nama_kecil"] = _coalesce_text([kecil1, kecil2, kecil3])

    frame["tanggal_lahir"] = pd.to_datetime(raw[c.tanggal_lahir], errors="coerce").dt.date
    frame["id_jenis_kelamin"] = raw[c.id_jenis_kelamin].map(_clean_text)
    frame["alamat"] = raw[c.alamat].map(_clean_text)
    frame["alamat_alternatif"] = raw[c.alamat_alternatif].map(_clean_text)
    frame["alamat_combined"] = _coalesce_text([frame["alamat"], frame["alamat_alternatif"]])
    frame["kodepos"] = raw[c.kodepos].map(_digits_only).str[:10]

    telepon = raw[c.telepon].map(_digits_only)
    telepon_keluarga = raw[c.telephone_keluarga].map(_digits_only)
    frame["telepon"] = telepon
    frame["telephone_keluarga"] = telepon_keluarga
    frame["telepon_any"] = telepon.fillna(telepon_keluarga)

    frame["nm_ayah"] = raw[c.nm_ayah].map(_clean_text)
    frame["nm_ibu"] = raw[c.nm_ibu].map(_clean_text)
    frame["nm_istri_suami"] = raw[c.nm_istri_suami].map(_clean_text)

    frame["nama_prefix"] = frame["nama_lengkap"].str[:6]
    frame["nama_lengkap_soundex"] = frame["nama_lengkap"].map(soundex)
    frame["dob_year"] = pd.to_datetime(frame["tanggal_lahir"], errors="coerce").dt.year.astype("Int64")

    frame = frame.dropna(subset=["record_id"]).copy()
    return frame


def normalize_entity_frame(entity_frame: pd.DataFrame) -> pd.DataFrame:
    frame = entity_frame.copy()
    frame["record_id"] = frame["entity_id"].map(lambda x: f"entity::{int(x)}")
    frame["id_upt"] = frame["canonical_id_upt"].map(_clean_text)
    frame["nik"] = frame["canonical_nik"].map(_digits_only)
    frame["nomor_induk_nasional"] = frame["canonical_nomor_induk_nasional"].map(_digits_only)
    frame["nama_lengkap"] = frame["canonical_nama_lengkap"].map(_clean_text)
    frame["alias_names"] = frame["canonical_alias_names"].map(_clean_text)
    frame["nama_kecil"] = frame["canonical_nama_kecil"].map(_clean_text)
    frame["tanggal_lahir"] = pd.to_datetime(frame["canonical_tanggal_lahir"], errors="coerce").dt.date
    frame["id_jenis_kelamin"] = frame["canonical_id_jenis_kelamin"].map(_clean_text)
    frame["alamat_combined"] = frame["canonical_alamat"].map(_clean_text)
    frame["kodepos"] = frame["canonical_kodepos"].map(_digits_only).str[:10]
    frame["telepon_any"] = frame["canonical_telepon"].map(_digits_only)
    frame["nm_ayah"] = frame["canonical_nm_ayah"].map(_clean_text)
    frame["nm_ibu"] = frame["canonical_nm_ibu"].map(_clean_text)
    frame["nm_istri_suami"] = frame["canonical_nm_istri_suami"].map(_clean_text)
    frame["nama_prefix"] = frame["nama_lengkap"].str[:6]
    frame["nama_lengkap_soundex"] = frame["nama_lengkap"].map(soundex)
    frame["dob_year"] = pd.to_datetime(frame["tanggal_lahir"], errors="coerce").dt.year.astype("Int64")
    frame["source_dataset"] = "entity"

    return frame[
        [
            "record_id",
            "entity_id",
            "id_upt",
            "nik",
            "nomor_induk_nasional",
            "nama_lengkap",
            "alias_names",
            "nama_kecil",
            "tanggal_lahir",
            "id_jenis_kelamin",
            "alamat_combined",
            "kodepos",
            "telepon_any",
            "nm_ayah",
            "nm_ibu",
            "nm_istri_suami",
            "nama_prefix",
            "nama_lengkap_soundex",
            "dob_year",
            "source_dataset",
        ]
    ]
