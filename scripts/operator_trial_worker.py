"""Credential-free installed-package worker for the operator release trial."""
import argparse
from pathlib import Path

from agent_bus import BusClient, WorkerRuntime, Completed, Blocked, PermanentFailure


class TrialExecutor:
    def execute(self, assignment):
        mode = assignment.context.get('mode')
        if mode == 'block' and not assignment.decisions:
            return Blocked('Choose the trial environment')
        if mode == 'fail' and assignment.attempt == 1:
            return PermanentFailure('trial_failure', 'Intentional first-attempt failure')
        return Completed('Operator trial completed', {
            'decision': assignment.decisions[-1]['decision'] if assignment.decisions else None})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--offset-directory', type=Path, required=True)
    args = parser.parse_args()
    WorkerRuntime(BusClient(args.url, 'operator-trial', offset_dir=args.offset_directory),
                  name='operator-trial', executor=TrialExecutor(), capabilities=['operator-trial'],
                  capacity=1, heartbeat_seconds=0.5).run()
