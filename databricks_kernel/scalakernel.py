from .databricks_mixin import DatabricksMixin
from .kernel_base import KernelBase


class DatabricksScalaKernel(DatabricksMixin, KernelBase):
    language = "scala"
    language_version = "0.1"
    language_info = {
        "name": "scala",
        "mimetype": "text/scala",
        "file_extension": ".scala",
    }
