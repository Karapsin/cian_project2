# async_full_transfer_fast.py
import os
import shutil
import asyncio
import yadisk
import nest_asyncio

from py.constants.constants import YADISK_TOKEN

nest_asyncio.apply()

# global sync client for uploads (robust & thread-friendly)
_ya_sync = yadisk.YaDisk(token=YADISK_TOKEN)


def _remote_join(*parts):
    cleaned = [p.strip("/").replace("\\", "/") for p in parts if p is not None]
    if not cleaned:
        return "/"
    return "/" + "/".join([p for p in cleaned if p])


async def _ensure_remote_dir(y, path, created_cache):
    if path in created_cache:
        return
    try:
        if not await y.exists(path):
            try:
                await y.mkdir(path)
            except yadisk.exceptions.DirectoryExistsError:
                pass
    finally:
        created_cache.add(path)


async def _list_remote_files_in_dir(y, remote_dir):
    """
    Return a set of file names present in remote_dir.
    If dir doesn't exist, return empty set.
    """
    try:
        if not await y.exists(remote_dir):
            return set()
    except Exception:
        # if exists() hiccups, try listing; if it fails we'll treat as empty
        pass

    names = set()
    try:
        async for item in y.listdir(remote_dir, fields=["type", "name"]):
            # some versions expose dict-like items, others attrs; handle both
            itype = item.get("type") if isinstance(item, dict) else getattr(item, "type", None)
            iname = item.get("name") if isinstance(item, dict) else getattr(item, "name", None)
            if itype == "file" and iname:
                names.add(iname)
    except yadisk.exceptions.PathNotFoundError:
        # directory really doesn't exist
        return set()
    return names


async def _upload_file_threaded(local_file_path, remote_file_path, sem):
    async with sem:
        try:
            await asyncio.to_thread(_ya_sync.upload, local_file_path, remote_file_path)
        except yadisk.exceptions.PathExistsError:
            # Someone else uploaded meanwhile — treat as success
            return False
    return True


async def _transfer_one_top_dir(
    y,
    local_base_dir,
    remote_base_dir,
    top_dir_name,
    file_concurrency=20,
    remove_local=True,
):
    """
    Faster strategy:
      - Pre-create top remote dir (race-safe).
      - For each local subdir (root), list remote files once.
      - Upload only missing filenames using thread-pooled sync uploader.
    """
    local_dir = os.path.join(local_base_dir, top_dir_name)
    if not os.path.isdir(local_dir):
        return

    remote_dir_top = _remote_join(remote_base_dir, top_dir_name)
    dir_cache = set()
    await _ensure_remote_dir(y, remote_dir_top, dir_cache)

    file_sem = asyncio.Semaphore(file_concurrency)
    tasks = []

    for root, _dirs, files in os.walk(local_dir):
        # remote path for this exact root
        rel_root = os.path.relpath(root, start=local_dir)
        remote_dir = remote_dir_top if rel_root == "." else _remote_join(remote_dir_top, rel_root)

        # ensure the remote subdir exists (once per distinct dir)
        await _ensure_remote_dir(y, remote_dir, dir_cache)

        # one list call for this remote subdir
        remote_names = await _list_remote_files_in_dir(y, remote_dir)

        # schedule uploads only for missing files
        for fname in files:
            if fname in remote_names:
                continue
            local_file_path = os.path.join(root, fname)
            if not os.path.isfile(local_file_path):
                continue  # skipped/moved
            remote_file_path = _remote_join(remote_dir, fname)
            tasks.append(asyncio.create_task(_upload_file_threaded(local_file_path, remote_file_path, file_sem)))

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # fail fast if anything truly error'd (not PathExists)
        for r in results:
            if isinstance(r, Exception):
                raise r

    if remove_local:
        print(f"[OK] Removing local dir: {local_dir}")
        shutil.rmtree(local_dir, ignore_errors=False)


def transfer_missing_directories_and_files(
    local_base_dir,
    remote_base_dir,
    file_concurrency=20,   # simultaneous file uploads
    dir_concurrency=40,     # simultaneous top-level dirs
    remove_local=True,
):
    """
    Transfer ALL folders under local_base_dir to remote_base_dir quickly:
      - Lists remote once per subdir (no per-file exists()).
      - Thread-pooled sync uploads (stable, fast).
      - Parallel top-level directories.
    """
    async def _runner():
        async with yadisk.AsyncClient(token=YADISK_TOKEN) as y:
            try:
                entries = sorted(os.listdir(local_base_dir))
            except FileNotFoundError:
                print(f"[WARN] Local base dir not found: {local_base_dir}")
                return

            # keep only top-level directories
            top_dirs = [name for name in entries if os.path.isdir(os.path.join(local_base_dir, name))]
            if not top_dirs:
                print("Nothing to transfer.")
                return

            dir_sem = asyncio.Semaphore(dir_concurrency)

            async def _one_dir(name):
                async with dir_sem:
                    print(f"[>>] Transferring folder: {os.path.join(local_base_dir, name)} -> {_remote_join(remote_base_dir, name)}")
                    await _transfer_one_top_dir(
                        y=y,
                        local_base_dir=local_base_dir,
                        remote_base_dir=remote_base_dir,
                        top_dir_name=name,
                        file_concurrency=file_concurrency,
                        remove_local=remove_local,
                    )
                    print(f"[OK] Done: {name}")

            await asyncio.gather(*[asyncio.create_task(_one_dir(n)) for n in top_dirs])

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop = asyncio.get_event_loop()

    loop.run_until_complete(_runner())

        

def force_create_empty_cloud_dir(cloud_path):
    if _ya_sync.exists(cloud_path):
        _ya_sync.remove(cloud_path, permanently=True)
        
    _ya_sync.mkdir(cloud_path)
