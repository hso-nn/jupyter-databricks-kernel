from ipykernel.kernelapp import IPKernelApp

from .kernel import DatabricksBaseKernel


class DatabricksPythonKernel(DatabricksBaseKernel):
    language = "python"
    language_version = "0.1"
    language_info = {
        "name": "python",
        "mimetype": "text/python",
        "file_extension": ".py",
    }


if __name__ == "__main__":
    IPKernelApp.launch_instance(kernel_class=DatabricksPythonKernel)
