# [2] Extraction ----
# Unpacks each archive of data/raw/<dataset>/ and saves its data as parquet in
# data/intermediate/<dataset>/, with the same name (performance_2015.zip -> performance_2015.parquet).
# All columns are saved as text; types are set in 3_unification.
# Everything else inside the archive (PDFs, codebooks, frequency tables) is copied to data/docs/<dataset>/.
# Archives already extracted are skipped.

NOT_DATA = ("frecuencia", "diccionario", "libro de codigo", "readme")  # CSVs that are documentation


def unpack(archive, folder):
    """Extract a .zip/.rar into folder, and the archives inside it too (some come nested)."""
    if archive.suffix.lower() == ".zip":
        with zipfile.ZipFile(archive) as z:
            z.extractall(folder)
    else:
        patoolib.extract_archive(str(archive), outdir=str(folder), verbosity=-1, interactive=False)
    for inner in list(folder.rglob("*")):
        if inner.suffix.lower() in (".zip", ".rar"):
            unpack(inner, inner.with_name(inner.name + "_unpacked"))
            inner.unlink()


def to_utf8(csv):
    """Polars reads UTF-8 only. MINEDUC files that are not UTF-8 are Latin-1, so convert those."""
    try:
        with open(csv, encoding="utf-8") as f:
            while f.read(1 << 24):
                pass
        return csv
    except UnicodeDecodeError:
        converted = csv.with_name(csv.stem + "_utf8.csv")
        with open(csv, encoding="latin-1") as src, open(converted, "w", encoding="utf-8") as dst:
            shutil.copyfileobj(src, dst, 1 << 24)
        return converted


for dataset in DATASETS:
    out = INTERMEDIATE / dataset
    out.mkdir(parents=True, exist_ok=True)
    archives = sorted(p for p in (RAW / dataset).iterdir() if p.suffix.lower() in (".zip", ".rar"))
    print(f"{dataset}: {len(archives)} archives")

    for archive in archives:
        target = out / f"{archive.stem}.parquet"
        if target.exists():
            continue

        with tempfile.TemporaryDirectory(dir=out, ignore_cleanup_errors=True) as tmp:
            tmp = Path(tmp)
            unpack(archive, tmp)
            files = [p for p in tmp.rglob("*") if p.is_file()]

            # The data is the biggest CSV that is not documentation
            csvs = [p for p in files if p.suffix.lower() == ".csv" and not any(w in normalize(p.name) for w in NOT_DATA)]
            if not csvs:
                print(f"  {archive.name}: no data CSV inside, skipped")
                continue
            data = max(csvs, key=lambda p: p.stat().st_size)
            if len(csvs) > 1:
                print(f"  {archive.name}: {len(csvs)} CSVs inside, using the biggest ({data.name})")

            # Keep everything else as documentation
            for f in files:
                if f != data:
                    copy = DOCS / dataset / archive.stem / f.relative_to(tmp)
                    copy.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, copy)

            # CSV -> parquet
            data = to_utf8(data)
            with open(data, encoding="utf-8") as f:
                header = f.readline()
            separator = ";" if header.count(";") >= header.count(",") else ","
            partial = target.with_name(target.name + ".part")
            (
                pl.scan_csv(data, separator=separator, infer_schema=False)
                .rename(lambda c: c.lstrip("\ufeff").strip())  # drop the invisible BOM some headers carry
                .sink_parquet(partial, compression="zstd")
            )
            partial.replace(target)
            print(f"  {archive.name} -> {target.name}")

# [End-of-file] ----
