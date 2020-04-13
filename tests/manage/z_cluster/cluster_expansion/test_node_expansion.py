import logging

from tests import helpers
from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest, aws_platform_required
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.framework import config
from ocs_ci.ocs.node import add_new_node_and_label_it, add_new_node_and_label_upi

logger = logging.getLogger(__name__)


@ignore_leftovers
@tier1
class TestAddNode(ManageTest):
    """
    Automates adding worker nodes to the cluster while IOs
    """
    @aws_platform_required
    def test_add_node_aws(self):
        """
        Test for adding worker nodes to the cluster while IOs
        """
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            machines = machine_utils.get_machinesets()
            logger.info(f'The worker nodes number before {len(helpers.get_worker_nodes())}')
            for machine in machines:
                add_new_node_and_label_it(machine)
            logger.info(f'The worker nodes number after {len(helpers.get_worker_nodes())}')

        else:
            logger.info(f'The worker nodes number before {len(helpers.get_worker_nodes())}')
            new_nodes = 3
            assert add_new_node_and_label_upi(config, new_nodes), "Add node failed"
            logger.info(f'The worker nodes number after {len(helpers.get_worker_nodes())}')
