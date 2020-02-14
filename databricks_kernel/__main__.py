import argparse
import json

from .pykernel import DatabricksPythonKernel
from .scalakernel import DatabricksScalaKernel
from .utils import objectview

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Configuration file")
    parser.add_argument("--language", help="Language (Python or Scala)")
    args = parser.parse_args()
    with open(args.config) as f:
        config = json.load(f)

    if args.language == "python":
        DatabricksPythonKernel(objectview(config)).start()
        # from . import simple_kernel
    elif args.language == "scala":
        DatabricksScalaKernel(objectview(config)).start()
    else:
        raise NotImplementedError(
            f"Language {args.language} is not supported by databricks."
        )
