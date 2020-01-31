# Copyright (c) Metakernel Development Team.
# Distributed under the terms of the Modified BSD License.

from metakernel import Magic, option
import os

class RunMagic(Magic):

    @option(
        '-l', '--language', action='store', default=None,
        help='use the provided language name as kernel'
    )
    def line_run(self, filename, language=None):
        """
        %run FILENAME|NOTEBOOK_NAME - run code in filename with current kernel
        """
        self.kernel.do_execute_file(filename)


def register_magics(kernel):
    kernel.register_magics(RunMagic)
