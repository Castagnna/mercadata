import subprocess
import io
import pandas as pd
import os
import logging


def get_project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_total_memory() -> int:
    membytes = os.sysconf("SC_PAGESIZE") * os.sysconf("SC_PHYS_PAGES")
    return membytes >> 30  # dividing by 1024^3 (1 GB)


def get_num_cores() -> int:
    return os.sysconf("SC_NPROCESSORS_ONLN")


def check_write_permission(mount_point: str) -> bool:
    path = os.path.join(mount_point, "birdy")
    try:
        with open(path, "w") as f:
            f.write("content")
        os.remove(path)
        return True
    except OSError as e:
        if e.strerror == "Read-only file system":
            logging.info(f"{mount_point} is Read-only.")
            pass
    return False


def choose_storage_device(return_all_possible: bool = False):
    process = subprocess.run(["df"], stdout=subprocess.PIPE)

    filesystems = (
        pd.read_fwf(io.BytesIO(process.stdout))
        .assign(**{"Mounted on": lambda df: df["Mounted on"].replace("/", "/tmp")})
        .loc[lambda df: df["1K-blocks"] > (5 << 20)]  # must be larger than 5GB
        .loc[lambda df: df["Mounted on"].map(check_write_permission)]
        .drop_duplicates(subset=["Filesystem"])
        .drop_duplicates(subset=["1K-blocks"])
        .assign(mount_len=lambda df: df["Mounted on"].map(len))
        .sort_values(["1K-blocks", "mount_len"], ascending=[False, True])
    )

    if return_all_possible:
        return list(filesystems["Mounted on"])

    if filesystems.empty:
        raise LookupError("No writable mount point found")

    return filesystems["Mounted on"].iloc[0]
