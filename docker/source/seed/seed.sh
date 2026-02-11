#!/bin/sh
set -eu

MYSQL_HOST="${MYSQL_HOST:-source-db}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DATABASE="${MYSQL_DATABASE:-corrections_prod}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-rootpass}"
SEED_ROWS="${SEED_ROWS:-500000}"
FORCE_RESEED="${FORCE_RESEED:-false}"

echo "Waiting for MariaDB at ${MYSQL_HOST}:${MYSQL_PORT}..."
until mariadb-admin --protocol=tcp -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
  -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ping --silent >/dev/null 2>&1
do
  sleep 2
done

mysql_exec() {
  mariadb --protocol=tcp \
    -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" \
    -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" \
    "${MYSQL_DATABASE}" "$@"
}

echo "Ensuring source table exists..."
mysql_exec <<'SQL'
CREATE TABLE IF NOT EXISTS inmate_records_stage (
  nomor_induk VARCHAR(32) NOT NULL,
  id_upt VARCHAR(20) DEFAULT '',
  nik VARCHAR(32) DEFAULT '',
  nomor_induk_nasional VARCHAR(32) DEFAULT '',
  nama_lengkap VARCHAR(255) DEFAULT '',
  nama_alias1 VARCHAR(255) DEFAULT '',
  nama_alias2 VARCHAR(255) DEFAULT '',
  nama_alias3 VARCHAR(255) DEFAULT '',
  nama_kecil1 VARCHAR(255) DEFAULT '',
  nama_kecil2 VARCHAR(255) DEFAULT '',
  nama_kecil3 VARCHAR(255) DEFAULT '',
  tanggal_lahir DATE NULL,
  id_jenis_kelamin VARCHAR(10) DEFAULT '',
  id_tempat_lahir VARCHAR(20) DEFAULT '',
  id_tempat_lahir_lain VARCHAR(255) DEFAULT '',
  alamat VARCHAR(255) DEFAULT '',
  alamat_alternatif VARCHAR(255) DEFAULT '',
  kodepos VARCHAR(10) DEFAULT '',
  telepon VARCHAR(32) DEFAULT '',
  telephone_keluarga VARCHAR(32) DEFAULT '',
  id_jenis_agama VARCHAR(20) DEFAULT '',
  id_jenis_agama_lain VARCHAR(255) DEFAULT '',
  id_jenis_suku VARCHAR(20) DEFAULT '',
  id_jenis_suku_lain VARCHAR(255) DEFAULT '',
  id_jenis_pendidikan VARCHAR(20) DEFAULT '',
  id_jenis_pendidikan_lain VARCHAR(255) DEFAULT '',
  id_jenis_pekerjaan VARCHAR(20) DEFAULT '',
  id_jenis_pekerjaan_lain VARCHAR(255) DEFAULT '',
  keterangan_pekerjaan VARCHAR(255) DEFAULT '',
  alamat_pekerjaan VARCHAR(255) DEFAULT '',
  minat VARCHAR(255) DEFAULT '',
  id_jenis_warganegara VARCHAR(20) DEFAULT '',
  id_negara_asing VARCHAR(20) DEFAULT '',
  id_jenis_status_perkawinan VARCHAR(20) DEFAULT '',
  nm_ayah VARCHAR(255) DEFAULT '',
  tmp_tgl_ayah VARCHAR(255) DEFAULT '',
  nm_ibu VARCHAR(255) DEFAULT '',
  tmp_tgl_ibu VARCHAR(255) DEFAULT '',
  nm_saudara VARCHAR(255) DEFAULT '',
  anakke INT DEFAULT 0,
  jml_saudara INT DEFAULT 0,
  jml_istri_suami INT DEFAULT 0,
  nm_istri_suami VARCHAR(255) DEFAULT '',
  tmp_tgl_istri_suami VARCHAR(255) DEFAULT '',
  jml_anak INT DEFAULT 0,
  nm_anak VARCHAR(255) DEFAULT '',
  tinggi INT DEFAULT 0,
  berat INT DEFAULT 0,
  cacat VARCHAR(255) DEFAULT '',
  ciri VARCHAR(255) DEFAULT '',
  ciri2 VARCHAR(255) DEFAULT '',
  ciri3 VARCHAR(255) DEFAULT '',
  id_jenis_rambut VARCHAR(20) DEFAULT '',
  id_bentukrambut VARCHAR(20) DEFAULT '',
  id_jenis_muka VARCHAR(20) DEFAULT '',
  id_bentuk_mata VARCHAR(20) DEFAULT '',
  id_warna_mata VARCHAR(20) DEFAULT '',
  id_kacamata VARCHAR(20) DEFAULT '',
  id_jenis_hidung VARCHAR(20) DEFAULT '',
  id_telinga VARCHAR(20) DEFAULT '',
  id_jenis_mulut VARCHAR(20) DEFAULT '',
  id_bentukbibir VARCHAR(20) DEFAULT '',
  id_warnakulit VARCHAR(20) DEFAULT '',
  id_jenis_tangan VARCHAR(20) DEFAULT '',
  id_lengan VARCHAR(20) DEFAULT '',
  id_jenis_kaki VARCHAR(20) DEFAULT '',
  foto_depan VARCHAR(255) DEFAULT '',
  foto_kanan VARCHAR(255) DEFAULT '',
  foto_kiri VARCHAR(255) DEFAULT '',
  foto_closeup VARCHAR(255) DEFAULT '',
  foto_ciri_1 VARCHAR(255) DEFAULT '',
  foto_ciri_2 VARCHAR(255) DEFAULT '',
  foto_ciri_3 VARCHAR(255) DEFAULT '',
  is_wbp_beresiko_tinggi TINYINT(1) DEFAULT 0,
  is_pengaruh_terhadap_masyarakat TINYINT(1) DEFAULT 0,
  is_baca_latin TINYINT(1) DEFAULT 0,
  is_baca_quran TINYINT(1) DEFAULT 0,
  is_disabilitas TINYINT(1) DEFAULT 0,
  is_verifikasi TINYINT(1) DEFAULT 0,
  residivis VARCHAR(20) DEFAULT '',
  residivis_counter INT DEFAULT 0,
  id_jenis_keahlian_1 VARCHAR(20) DEFAULT '',
  id_jenis_keahlian_1_lain VARCHAR(255) DEFAULT '',
  id_jenis_keahlian_2 VARCHAR(20) DEFAULT '',
  id_jenis_keahlian_2_lain VARCHAR(255) DEFAULT '',
  id_user VARCHAR(50) DEFAULT '',
  created_by VARCHAR(50) DEFAULT '',
  updated_by VARCHAR(50) DEFAULT '',
  updated_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (nomor_induk),
  KEY idx_updated_at (updated_at),
  KEY idx_nik (nik),
  KEY idx_nomor_induk_nasional (nomor_induk_nasional),
  KEY idx_tanggal_lahir (tanggal_lahir),
  KEY idx_nama_lengkap (nama_lengkap(100))
) ENGINE=InnoDB;
SQL

existing_rows="$(mysql_exec -Nse "SELECT COUNT(*) FROM inmate_records_stage;")"
if [ "${existing_rows}" -gt 0 ] && [ "${FORCE_RESEED}" != "true" ]; then
  echo "Table already has ${existing_rows} rows. Skip seeding (set FORCE_RESEED=true to rebuild)."
  exit 0
fi

if [ "${FORCE_RESEED}" = "true" ]; then
  echo "FORCE_RESEED=true, truncating existing rows..."
  mysql_exec -e "TRUNCATE TABLE inmate_records_stage;"
fi

echo "Seeding ${SEED_ROWS} synthetic rows..."
mysql_exec <<SQL
SET @seed_rows := ${SEED_ROWS};

INSERT INTO inmate_records_stage (
  nomor_induk,
  id_upt,
  nik,
  nomor_induk_nasional,
  nama_lengkap,
  nama_alias1,
  nama_alias2,
  nama_alias3,
  nama_kecil1,
  nama_kecil2,
  nama_kecil3,
  tanggal_lahir,
  id_jenis_kelamin,
  id_tempat_lahir,
  id_tempat_lahir_lain,
  alamat,
  alamat_alternatif,
  kodepos,
  telepon,
  telephone_keluarga,
  id_jenis_agama,
  id_jenis_agama_lain,
  id_jenis_suku,
  id_jenis_suku_lain,
  id_jenis_pendidikan,
  id_jenis_pendidikan_lain,
  id_jenis_pekerjaan,
  id_jenis_pekerjaan_lain,
  keterangan_pekerjaan,
  alamat_pekerjaan,
  minat,
  id_jenis_warganegara,
  id_negara_asing,
  id_jenis_status_perkawinan,
  nm_ayah,
  tmp_tgl_ayah,
  nm_ibu,
  tmp_tgl_ibu,
  nm_saudara,
  anakke,
  jml_saudara,
  jml_istri_suami,
  nm_istri_suami,
  tmp_tgl_istri_suami,
  jml_anak,
  nm_anak,
  tinggi,
  berat,
  cacat,
  ciri,
  ciri2,
  ciri3,
  id_jenis_rambut,
  id_bentukrambut,
  id_jenis_muka,
  id_bentuk_mata,
  id_warna_mata,
  id_kacamata,
  id_jenis_hidung,
  id_telinga,
  id_jenis_mulut,
  id_bentukbibir,
  id_warnakulit,
  id_jenis_tangan,
  id_lengan,
  id_jenis_kaki,
  foto_depan,
  foto_kanan,
  foto_kiri,
  foto_closeup,
  foto_ciri_1,
  foto_ciri_2,
  foto_ciri_3,
  is_wbp_beresiko_tinggi,
  is_pengaruh_terhadap_masyarakat,
  is_baca_latin,
  is_baca_quran,
  is_disabilitas,
  is_verifikasi,
  residivis,
  residivis_counter,
  id_jenis_keahlian_1,
  id_jenis_keahlian_1_lain,
  id_jenis_keahlian_2,
  id_jenis_keahlian_2_lain,
  id_user,
  created_by,
  updated_by,
  updated_at
)
SELECT
  CONCAT('NI', LPAD(src.n, 12, '0')) AS nomor_induk,
  CONCAT('UPT', LPAD(1 + MOD(src.person_key, 120), 3, '0')) AS id_upt,
  LPAD(src.person_key, 16, '0') AS nik,
  LPAD(src.person_key, 12, '0') AS nomor_induk_nasional,
  CONCAT(
    ELT(1 + MOD(src.person_key, 20), 'BUDI', 'AHMAD', 'JOKO', 'RIZKY', 'FAJAR', 'BAGUS', 'RAMA', 'YUSUF', 'DANI', 'AGUS', 'ARI', 'HENDRA', 'RUDI', 'YOGA', 'SANDI', 'IRFAN', 'RIZAL', 'ILHAM', 'NUGROHO', 'BAYU'),
    ' ',
    ELT(1 + MOD(FLOOR(src.person_key / 7), 20), 'SANTOSO', 'PRATAMA', 'SYAHPUTRA', 'NUGRAHA', 'GUNAWAN', 'SETIAWAN', 'FIRMANSYAH', 'SAPUTRA', 'KURNIAWAN', 'HIDAYAT', 'MAULANA', 'RAHMAN', 'WIJAYA', 'SIREGAR', 'TANJUNG', 'NASUTION', 'LUBIS', 'HUTAGALUNG', 'HADINATA', 'PANGESTU')
  ) AS nama_lengkap,
  IF(MOD(src.n, 4) = 0, CONCAT('ALIAS ', ELT(1 + MOD(src.n, 10), 'BOY', 'JAY', 'UDI', 'ANDI', 'RAMA', 'BAY', 'MAN', 'DAN', 'GUS', 'TON')), '') AS nama_alias1,
  IF(MOD(src.n, 9) = 0, CONCAT('ALIAS ', ELT(1 + MOD(src.n, 10), 'RA', 'AL', 'SU', 'JO', 'RY', 'FA', 'AR', 'YU', 'ZA', 'RE')), '') AS nama_alias2,
  IF(MOD(src.n, 17) = 0, CONCAT('ALIAS ', ELT(1 + MOD(src.n, 10), 'X1', 'X2', 'X3', 'X4', 'X5', 'X6', 'X7', 'X8', 'X9', 'X0')), '') AS nama_alias3,
  IF(MOD(src.n, 5) = 0, ELT(1 + MOD(src.person_key, 10), 'BUDI', 'AHMAD', 'JOKO', 'RIZKY', 'FAJAR', 'BAGUS', 'RAMA', 'YUSUF', 'DANI', 'AGUS'), '') AS nama_kecil1,
  IF(MOD(src.n, 13) = 0, ELT(1 + MOD(src.person_key, 10), 'ARI', 'RUDI', 'BAYU', 'YOGA', 'ILHAM', 'RIZAL', 'IRFAN', 'SANDI', 'TONI', 'DANI'), '') AS nama_kecil2,
  IF(MOD(src.n, 29) = 0, ELT(1 + MOD(src.person_key, 10), 'BOY', 'JAY', 'UDI', 'ANDI', 'RAMA', 'BAY', 'MAN', 'DAN', 'GUS', 'TON'), '') AS nama_kecil3,
  DATE_ADD('1970-01-01', INTERVAL MOD(src.person_key, 18000) DAY) AS tanggal_lahir,
  IF(MOD(src.person_key, 2) = 0, '1', '2') AS id_jenis_kelamin,
  CONCAT('TPL', LPAD(1 + MOD(src.person_key, 300), 4, '0')) AS id_tempat_lahir,
  '' AS id_tempat_lahir_lain,
  CONCAT('JL TEST NO ', MOD(src.person_key, 5000), ' KEL ', MOD(src.person_key, 200)) AS alamat,
  IF(MOD(src.n, 11) = 0, CONCAT('ALT ', MOD(src.n, 9999), ' BLOK ', MOD(src.person_key, 50)), '') AS alamat_alternatif,
  LPAD(10000 + MOD(src.person_key, 89999), 5, '0') AS kodepos,
  CONCAT('08', LPAD(MOD(src.person_key * 31, 10000000000), 10, '0')) AS telepon,
  CONCAT('08', LPAD(MOD(src.person_key * 47, 10000000000), 10, '0')) AS telephone_keluarga,
  CONCAT('AGM', MOD(src.person_key, 8)) AS id_jenis_agama,
  '' AS id_jenis_agama_lain,
  CONCAT('SUK', MOD(src.person_key, 40)) AS id_jenis_suku,
  '' AS id_jenis_suku_lain,
  CONCAT('PND', MOD(src.person_key, 10)) AS id_jenis_pendidikan,
  '' AS id_jenis_pendidikan_lain,
  CONCAT('PKJ', MOD(src.person_key, 25)) AS id_jenis_pekerjaan,
  '' AS id_jenis_pekerjaan_lain,
  CONCAT('PEKERJAAN ', MOD(src.person_key, 25)) AS keterangan_pekerjaan,
  CONCAT('KAWASAN ', MOD(src.person_key, 300)) AS alamat_pekerjaan,
  CONCAT('MINAT ', MOD(src.person_key, 15)) AS minat,
  IF(MOD(src.person_key, 50) = 0, '2', '1') AS id_jenis_warganegara,
  IF(MOD(src.person_key, 50) = 0, CONCAT('NEG', MOD(src.person_key, 15)), '') AS id_negara_asing,
  CONCAT('KWN', MOD(src.person_key, 5)) AS id_jenis_status_perkawinan,
  CONCAT('AYAH ', ELT(1 + MOD(src.person_key, 20), 'SANTOSO', 'PRATAMA', 'SYAHPUTRA', 'NUGRAHA', 'GUNAWAN', 'SETIAWAN', 'FIRMANSYAH', 'SAPUTRA', 'KURNIAWAN', 'HIDAYAT', 'MAULANA', 'RAHMAN', 'WIJAYA', 'SIREGAR', 'TANJUNG', 'NASUTION', 'LUBIS', 'HUTAGALUNG', 'HADINATA', 'PANGESTU')) AS nm_ayah,
  CONCAT('TMP ', MOD(src.person_key, 300), ' / ', DATE_FORMAT(DATE_ADD('1950-01-01', INTERVAL MOD(src.person_key, 25000) DAY), '%Y-%m-%d')) AS tmp_tgl_ayah,
  CONCAT('IBU ', ELT(1 + MOD(src.person_key, 20), 'SARI', 'WATI', 'DEWI', 'NINGSIH', 'KURNIA', 'MELATI', 'SUSANTI', 'ROHAYATI', 'FITRI', 'YULI', 'MAWAR', 'NURAINI', 'SULASTRI', 'HARTATI', 'NURHAYATI', 'MARIANI', 'KARTINI', 'JULIANA', 'FATIMAH', 'RUSMINI')) AS nm_ibu,
  CONCAT('TMP ', MOD(src.person_key, 300), ' / ', DATE_FORMAT(DATE_ADD('1955-01-01', INTERVAL MOD(src.person_key, 25000) DAY), '%Y-%m-%d')) AS tmp_tgl_ibu,
  CONCAT('SAUDARA ', MOD(src.person_key, 2000)) AS nm_saudara,
  MOD(src.person_key, 8) + 1 AS anakke,
  MOD(src.person_key, 6) AS jml_saudara,
  MOD(src.person_key, 3) AS jml_istri_suami,
  IF(MOD(src.person_key, 3) = 0, '', CONCAT('PASANGAN ', MOD(src.person_key, 5000))) AS nm_istri_suami,
  CONCAT('TMP ', MOD(src.person_key, 300), ' / ', DATE_FORMAT(DATE_ADD('1975-01-01', INTERVAL MOD(src.person_key, 20000) DAY), '%Y-%m-%d')) AS tmp_tgl_istri_suami,
  MOD(src.person_key, 5) AS jml_anak,
  CONCAT('ANAK ', MOD(src.person_key, 5000)) AS nm_anak,
  150 + MOD(src.person_key, 40) AS tinggi,
  45 + MOD(src.person_key, 60) AS berat,
  IF(MOD(src.person_key, 100) = 0, 'DISABILITAS RINGAN', '') AS cacat,
  CONCAT('CIRI ', MOD(src.person_key, 1000)) AS ciri,
  CONCAT('CIRI2 ', MOD(src.person_key, 1000)) AS ciri2,
  CONCAT('CIRI3 ', MOD(src.person_key, 1000)) AS ciri3,
  CONCAT('RMB', MOD(src.person_key, 5)) AS id_jenis_rambut,
  CONCAT('BRM', MOD(src.person_key, 4)) AS id_bentukrambut,
  CONCAT('MUK', MOD(src.person_key, 5)) AS id_jenis_muka,
  CONCAT('MAT', MOD(src.person_key, 5)) AS id_bentuk_mata,
  CONCAT('WAR', MOD(src.person_key, 5)) AS id_warna_mata,
  IF(MOD(src.person_key, 4) = 0, '1', '0') AS id_kacamata,
  CONCAT('HDG', MOD(src.person_key, 5)) AS id_jenis_hidung,
  CONCAT('TLG', MOD(src.person_key, 5)) AS id_telinga,
  CONCAT('MLT', MOD(src.person_key, 5)) AS id_jenis_mulut,
  CONCAT('BBR', MOD(src.person_key, 5)) AS id_bentukbibir,
  CONCAT('KLT', MOD(src.person_key, 6)) AS id_warnakulit,
  CONCAT('TGN', MOD(src.person_key, 4)) AS id_jenis_tangan,
  CONCAT('LGN', MOD(src.person_key, 4)) AS id_lengan,
  CONCAT('KAK', MOD(src.person_key, 4)) AS id_jenis_kaki,
  '' AS foto_depan,
  '' AS foto_kanan,
  '' AS foto_kiri,
  '' AS foto_closeup,
  '' AS foto_ciri_1,
  '' AS foto_ciri_2,
  '' AS foto_ciri_3,
  IF(MOD(src.person_key, 30) = 0, 1, 0) AS is_wbp_beresiko_tinggi,
  IF(MOD(src.person_key, 40) = 0, 1, 0) AS is_pengaruh_terhadap_masyarakat,
  IF(MOD(src.person_key, 5) = 0, 1, 0) AS is_baca_latin,
  IF(MOD(src.person_key, 7) = 0, 1, 0) AS is_baca_quran,
  IF(MOD(src.person_key, 100) = 0, 1, 0) AS is_disabilitas,
  IF(MOD(src.n, 3) = 0, 1, 0) AS is_verifikasi,
  IF(MOD(src.person_key, 9) = 0, 'YA', 'TIDAK') AS residivis,
  MOD(src.person_key, 4) AS residivis_counter,
  CONCAT('AHL', MOD(src.person_key, 20)) AS id_jenis_keahlian_1,
  '' AS id_jenis_keahlian_1_lain,
  CONCAT('AHL', MOD(src.person_key + 7, 20)) AS id_jenis_keahlian_2,
  '' AS id_jenis_keahlian_2_lain,
  CONCAT('USR', MOD(src.n, 500)) AS id_user,
  'seed-script' AS created_by,
  'seed-script' AS updated_by,
  TIMESTAMPADD(DAY, MOD(src.n, 760), '2024-01-01 00:00:00') AS updated_at
FROM (
  SELECT
    seq.n,
    GREATEST(
      1,
      CASE
        WHEN MOD(seq.n, 25) IN (0, 1, 2) THEN seq.n - MOD(seq.n, 25)
        WHEN MOD(seq.n, 17) = 0 THEN seq.n - 1
        ELSE seq.n
      END
    ) AS person_key
  FROM (
    SELECT
      a.n
      + b.n * 10
      + c.n * 100
      + d.n * 1000
      + e.n * 10000
      + f.n * 100000
      + 1 AS n
    FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) f
  ) seq
  WHERE seq.n <= @seed_rows
) src;
SQL

final_rows="$(mysql_exec -Nse "SELECT COUNT(*) FROM inmate_records_stage;")"
dup_rows="$(mysql_exec -Nse "SELECT SUM(c - 1) FROM (SELECT COUNT(*) AS c FROM inmate_records_stage GROUP BY nik, tanggal_lahir HAVING c > 1) x;")"

echo "Seed complete. total_rows=${final_rows} approx_duplicate_rows=${dup_rows:-0}"
