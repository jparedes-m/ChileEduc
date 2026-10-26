# [1] Scraping ----
# Downloads every .zip/.rar listed on each dataset page to data/raw/<dataset>/,
# renamed to performance_2015.zip, assistance_2015_03.rar, etc.
# Files already downloaded are skipped.


def download(url, target):
    partial = target.with_name(target.name + ".part")  # so a cut download never looks finished
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with open(partial, "wb") as f:
            for chunk in response.iter_content(1 << 20):
                f.write(chunk)
    partial.replace(target)
    print(f"  downloaded {target.name}")


for dataset, info in DATASETS.items():
    folder = RAW / dataset
    folder.mkdir(parents=True, exist_ok=True)

    # Read the page and keep the links to .zip/.rar files
    page = requests.get(info["url"], timeout=60)
    page.raise_for_status()
    links = [urljoin(info["url"], a["href"]) for a in BeautifulSoup(page.text, "html.parser").find_all("a", href=True)]
    links = [link for link in dict.fromkeys(links) if urlparse(link).path.lower().endswith((".zip", ".rar"))]
    if not links:
        print(f"{dataset}: no .zip/.rar links found on {info['url']} (did the page change?)")

    # Decide the name of each file and keep only the ones we don't have yet
    to_download = {}
    for link in links:
        original = unquote(Path(urlparse(link).path).name)
        new_name = file_name_for(dataset, original, info["monthly"])
        if new_name is None:
            print(f"  {original}: no year in the name, not downloaded ({link})")
            continue
        target = folder / (new_name + Path(original).suffix.lower())
        if not target.exists():
            to_download[link] = target

    print(f"{dataset}: {len(links)} files on the page, {len(to_download)} new")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(download, to_download.keys(), to_download.values()))

# [End-of-file] ----
