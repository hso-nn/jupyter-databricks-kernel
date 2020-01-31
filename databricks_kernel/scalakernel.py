from ipykernel.kernelapp import IPKernelApp

from .kernel import DatabricksBaseKernel


class DatabricksScalaKernel(DatabricksBaseKernel):
    language = "scala"
    language_version = "0.1"
    language_info = {
        "name": "scala",
        "mimetype": "text/scala",
        "file_extension": ".scala",
    }


if __name__ == "__main__":
    IPKernelApp.launch_instance(kernel_class=DatabricksScalaKernel)
