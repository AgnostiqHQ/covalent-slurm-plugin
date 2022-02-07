# Copyright 2021 Agnostiq Inc.
#
# This file is a part of Covalent Cloud.

"""Define a custom executor for the Covalent dispatcher."""

from typing import Any

from covalent._shared_files.util_classes import DispatchInfo
from covalent._workflow.transport import TransportableObject
from covalent.executor import BaseExecutor

# Specify the plugin name as well as the short name given by this file's name
executor_plugin_name = "CustomExecutor"

class CustomExecutor(BaseExecutor):
    def execute(
        self,
        function: TransportableObject,
        kwargs: Any,
        execution_args: dict,
        dispatch_id: str,
        node_id: int = -1,
    ) -> Any:

        dispatch_info = DispatchInfo(dispatch_id)

        with self.get_dispatch_context(dispatch_info):
            print(f"Execution args: {execution_args}")

            # Execute the function here

        return None
