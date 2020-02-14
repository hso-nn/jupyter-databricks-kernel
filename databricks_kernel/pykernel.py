from .databricks_mixin import DatabricksMixin
from .kernel_base import KernelBase


class DatabricksPythonKernel(DatabricksMixin, KernelBase):
    language = "python"
    language_version = "0.1"
    language_info = {
        "name": "python",
        "mimetype": "text/python",
        "file_extension": ".py",
    }
