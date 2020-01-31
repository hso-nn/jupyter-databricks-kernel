import argparse
import json
import os
import shutil
import sys
from pathlib import Path

from IPython.utils.tempdir import TemporaryDirectory
from jupyter_client.kernelspec import KernelSpecManager

kernel_json = {
    "python": {
        "argv": [
            sys.executable,
            "-m",
            "databricks_kernel.scalakernel",
            "-f",
            "{connection_file}",
        ],
        "display_name": "Databricks (Scala)",
        "name": "databricks_scala",
        "language": "scala",
    },
    "scala": {
        "argv": [
            sys.executable,
            "-m",
            "databricks_kernel.pykernel",
            "-f",
            "{connection_file}",
        ],
        "display_name": "Databricks (Python)",
        "name": "databricks_python",
        "language": "python",
    },
}


def install_my_kernel_spec(user=True, prefix=None):
    with TemporaryDirectory() as td:
        os.chmod(td, 0o755)  # Starts off as 700, not user readable
        for l in ["python", "scala"]:
            with open(os.path.join(td, "kernel.json"), "w") as f:
                json.dump(kernel_json[l], f, sort_keys=True)

            print(f"Installing Jupyter kernel spec ({l})")
            dest = KernelSpecManager().install_kernel_spec(
                td, f"databricks_{l}", user=user, prefix=prefix
            )
            
            shutil.copy(Path(__file__).parent / "resources" / f"databricks_{l}.png", Path(dest) / "logo-64x64.png")

def _is_root():
    try:
        return os.geteuid() == 0
    except AttributeError:
        return False  # assume not an admin on non-Unix platforms


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--user",
        action="store_true",
        help="Install to the per-user kernels registry. Default if not root.",
    )
    ap.add_argument(
        "--sys-prefix",
        action="store_true",
        help="Install to sys.prefix (e.g. a virtualenv or conda env)",
    )
    ap.add_argument(
        "--prefix",
        help="Install to the given prefix. "
        "Kernelspec will be installed in {PREFIX}/share/jupyter/kernels/",
    )
    args = ap.parse_args(argv)

    if args.sys_prefix:
        args.prefix = sys.prefix
    if not args.prefix and not _is_root():
        args.user = True

    install_my_kernel_spec(user=args.user, prefix=args.prefix)


if __name__ == "__main__":
    main()
