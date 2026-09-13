"""CLI intent planning and ordered acceptance checks over existing events."""
from __future__ import annotations

import copy
import json

from operations import ProjectionLookupError
from projection import PMState, PROJECTION_TOPICS, apply_event


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def read_snapshot(observer, *, key=None, anchor=None):
    head = observer.health().get('last_event_id')
    if type(head) is not int or head < 0:
        raise ValueError('bus does not expose a bounded history cursor; upgrade the bus with the CLI')
    state, prior, anchored, accepted = PMState(), None, None, None
    for event in observer.iter_events(through_id=head, topics=list(PROJECTION_TOPICS)):
        applied = apply_event(state, event)
        if event['id'] == anchor:
            anchored = event
        if key and event.get('actor') == 'human' and event.get('idempotency_key') == key:
            prior, accepted = event, applied
    return state, head, prior, anchored, accepted


def intervene(observer, publish, *, command, task_id, reason=None, anchor=None,
              additional_retries=None, decision=None, idempotency_key=None, now):
    """Return whether replay accepted this intent, not a completion promise."""
    if reason is not None:
        reason = reason.strip()
        if not reason:
            raise ValueError('--reason must not be empty')
    if command not in {'cancel', 'retry', 'decide'}:
        raise ValueError('unknown intervention')
    if command in {'retry', 'decide'} and (type(anchor) is not int or anchor <= 0):
        raise ValueError('an explicit failure/decision event ID is required')
    key = idempotency_key or f'operator:{command}:task:{task_id}' + (f':event:{anchor}' if anchor else '')
    if not key.strip():
        raise ValueError('idempotency key must not be empty')
    state, head, prior, anchored, accepted = read_snapshot(observer, key=key, anchor=anchor)
    task = state.tasks.get(task_id)
    if task is None:
        raise ProjectionLookupError(f'task {task_id} was not found')
    if command == 'cancel':
        topic = 'task.cancel_requested'
        payload = {'task_id': task_id, 'reason': reason}
        cause = task.created_event_id
    else:
        expected = 'task.failed' if command == 'retry' else 'decision.needed'
        if not anchored or anchored['topic'] != expected or anchored['payload'].get('task_id') != task_id:
            raise ValueError(f'event #{anchor} is not {expected} for task {task_id}')
        cause = anchor
        if command == 'retry':
            if type(additional_retries) is not int or additional_retries <= 0:
                raise ValueError('--additional-retries must be positive')
            topic = 'task.retry_requested'
            payload = {'task_id': task_id, 'reason': reason, 'additional_retries': additional_retries}
        else:
            topic = 'decision.made'
            payload = {'task_id': task_id, 'assignment_id': anchored['payload']['assignment_id'],
                       'decision_id': anchored['payload']['decision_id'], 'decision': decision}
    intent = {'topic': topic, 'actor': 'human', 'payload': payload, 'caused_by': cause,
              'correlation_id': task.correlation_id, 'schema_version': 2, 'idempotency_key': key}
    canonical(intent)  # Reject NaN and non-JSON decision values before publishing.
    if prior is not None:
        if any(canonical(prior.get(k)) != canonical(v) for k, v in intent.items()):
            raise ValueError('idempotency key already belongs to different intent; do not change a retry command')
        event = prior
        duplicate = True
    else:
        proposed = {**intent, 'id': head + 1, 'ts': now}
        if not apply_event(copy.deepcopy(state), proposed):
            raise ValueError(f'task {task_id} no longer accepts this {command} intent; '
                             f'current state is {task.status}. Inspect the task and choose its current event ID.')
        event = publish(intent)
        if not isinstance(event, dict) or type(event.get('id')) is not int or event['id'] <= 0:
            raise ValueError(f'publication returned no valid event ID; retry the same command/key {key!r}')
        # Another identical command may have won before our initial snapshot;
        # or intervening completion/cancellation may have made this claim stale.
        # Replaying through exactly the returned event handles both cases.
        state, accepted, found = PMState(), None, False
        for recorded in observer.iter_events(through_id=event['id'], topics=list(PROJECTION_TOPICS)):
            applied = apply_event(state, recorded)
            if recorded['id'] == event['id']:
                if any(canonical(recorded.get(k)) != canonical(v) for k, v in intent.items()):
                    raise ValueError('recorded event does not match submitted intent')
                event, accepted, found = recorded, applied, True
        if not found:
            raise ValueError(f'event #{event["id"]} was recorded but could not be verified; '
                             'retry the same command/key, do not assume success')
        head = event['id']
        task = state.tasks[task_id]
        duplicate = False
    return {'recorded': True, 'accepted': bool(accepted), 'duplicate': duplicate,
            'event': event, 'observed_through_id': head, 'task_status': task.status,
            'next_action': ['agent-bus', 'task', str(task_id)],
            'message': ('Intent accepted by ordered replay; inspect the task for subsequent PM progress.'
                        if accepted else 'Intent remains in history but replay rejected it as stale or inapplicable.')}
