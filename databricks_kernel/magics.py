import json
from pathlib import Path

import os


async def run(kernel, filename, *args):
    filename = Path(filename).with_suffix(".ipynb")
    print(os.getcwd())
    print(filename.with_suffix(".pynb"))
    if not filename.exists():
        raise FileNotFoundError(filename)

    with filename.open() as f:
        data = json.load(f)

    results = []
    for cell in data["cells"]:
        code = "\n".join(cell["source"])
        response = await kernel.execute_code(code)
        # only text responses are supported
        if response:
            results.append(response.get("text"))

    return "\n".join(results)
